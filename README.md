# HiveReclaim

**Mempool watcher and marked capital recovery microservice for A2A agent networks.**

HiveReclaim monitors known bad-actor addresses on Base L2 for outbound USDC transactions containing cryptographically marked (drip_id-tagged) capital. When detected, it logs recovery events, broadcasts to HiveVault and HiveForge blacklists, and lays the groundwork for front-run gas-auction recovery.

---

## Background

Steve's agent wallet was drained by address `0x2dCDEA8a708f1FDECA5e2E59d4cb70Bd2E9BdEC8` (Manus's aggregator). HiveVault drips embed a unique `drip_id` in transaction calldata — this marks the capital as recoverable. HiveReclaim parks a watcher on known bad-actor addresses and when they broadcast outbound USDC transactions, attempts recovery.

This is legally equivalent to MEV (Maximal Extractable Value) bots that front-run transactions on Ethereum — a known, legal, on-chain practice. The agent watches public mempool data and competes in a gas auction.

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/reclaim/watchlist` | List all watched addresses |
| POST | `/reclaim/watch` | Add an address to the watchlist |
| DELETE | `/reclaim/watch/{address}` | Remove an address from the watchlist |
| GET | `/reclaim/events` | Recent mempool events from watched addresses |
| GET | `/reclaim/recoveries` | Successful and attempted recovery events |
| POST | `/reclaim/scan` | Manually trigger a scan of all watched addresses |
| GET | `/reclaim/marked/{drip_id}` | Check if a drip_id has been seen on-chain |
| GET | `/reclaim/frontrun/explain` | Educational explanation of front-run recovery |

---

## Configuration

Set via environment variables:

| Variable | Description |
|----------|-------------|
| `HIVE_KEY` | HiveVault internal auth key |
| `PORT` | HTTP port (default: 8000) |
| `BASESCAN_API_KEY` | Basescan API key (optional, improves rate limits) |
| `GAS_WALLET_ADDRESS` | Funded wallet address for live recovery (future) |
| `GAS_WALLET_KEY` | Private key for live recovery gas wallet (future) |

---

## How Front-Run Recovery Works

1. **Mempool Monitoring** — Watch pending txs from known bad-actor addresses  
2. **Marked Capital ID** — Match drip_id embedded in calldata  
3. **Higher Gas Bid** — Submit recovery tx with higher priority fee  
4. **Transaction Ordering** — Block builders prioritise by fee, recovery tx goes first  
5. **Recovery** — USDC redirected to rightful owner; event logged; blacklists broadcast  

See `GET /reclaim/frontrun/explain` for full JSON explanation.

---

## Watchlist Persistence

- In-memory: `watchlist` dict in process  
- Disk: `/tmp/reclaim_watchlist.json`  
- Events: `/tmp/reclaim_events.json` (capped at 500 events)

Pre-loaded on startup:
```
0x2dCDEA8a708f1FDECA5e2E59d4cb70Bd2E9BdEC8
Reason: swept $25 USDC marked capital from Hive2 agent wallet on 2026-04-23
```

---

## Running Locally

```bash
pip install -r requirements.txt
python hive_reclaim.py
# Service available at http://localhost:8000
```

---

## Deployment

Deploy to Render using the included `render.yaml`:

```bash
render deploy
```

Or manually via Render dashboard — connect the GitHub repo and Render will auto-detect `render.yaml`.

---

## DID

`did:hive:reclaim`  
Role: `reclaim_agent`  
Tier: `VOID`
