# Key-Value Baseline Benchmark Utilities

These are research utilities for the graduation project baseline. They are not production ClickHouse code and are intended only to collect numerical motivation before implementing a Redis/Memcached-compatible endpoint.

The native ClickHouse TCP benchmark is intentionally not included yet. It can be added later if the client dependency is easy to set up.

## Workflow

1. Generate dataset.
2. Create ClickHouse `EmbeddedRocksDB` table.
3. Load ClickHouse.
4. Start Redis.
5. Load Redis.
6. Run HTTP SQL benchmark.
7. Run Redis `GET`/`MGET` benchmark.

## Example Commands

Generate a small smoke dataset:

```bash
python3 benchmark/kv_baseline/generate_dataset.py \
  --rows 100000 \
  --value-size 64 \
  --output benchmark/kv_baseline/data_100k_64.tsv \
  --keys-output benchmark/kv_baseline/keys_100k.txt
```

Create the ClickHouse table:

```bash
clickhouse-client < benchmark/kv_baseline/load_clickhouse.sql
```

Load ClickHouse:

```bash
clickhouse-client --query "INSERT INTO kv_baseline FORMAT TSV" \
  < benchmark/kv_baseline/data_100k_64.tsv
```

Start Redis locally, for example:

```bash
redis-server --save "" --appendonly no
```

Load Redis. This script requires `redis-py`:

```bash
python3 -m pip install redis
```

```bash
python3 benchmark/kv_baseline/load_redis.py \
  --input benchmark/kv_baseline/data_100k_64.tsv \
  --flushdb
```

Run ClickHouse HTTP SQL benchmark:

```bash
python3 benchmark/kv_baseline/bench_clickhouse_http.py \
  --keys-file benchmark/kv_baseline/keys_100k.txt \
  --duration 30 \
  --concurrency 8 \
  --batch-size 10 \
  --dataset-size 100000 \
  --value-size 64 \
  --output benchmark/kv_baseline/results/baseline.csv
```

Run ClickHouse Redis-compatible endpoint benchmark. This script uses raw sockets and the Python standard library only:

```bash
python3 benchmark/kv_baseline/bench_clickhouse_redis.py \
  --host 127.0.0.1 \
  --port 9006 \
  --db 0 \
  --keys-file benchmark/kv_baseline/keys_100k.txt \
  --duration 30 \
  --concurrency 8 \
  --batch-size 10 \
  --dataset-size 100000 \
  --value-size 64 \
  --output benchmark/kv_baseline/results/baseline.csv
```

Each worker opens one TCP connection, sends `SELECT 0` once, then keeps using the same connection for `GET` or `MGET` commands.

Run Redis raw-socket benchmark. This script uses the same raw-socket RESP style as `bench_clickhouse_redis.py` and is preferred for the final apples-to-apples comparison:

```bash
python3 benchmark/kv_baseline/bench_redis_raw.py \
  --host 127.0.0.1 \
  --port 6379 \
  --db 0 \
  --keys-file benchmark/kv_baseline/keys_100k.txt \
  --duration 30 \
  --concurrency 8 \
  --batch-size 10 \
  --dataset-size 100000 \
  --value-size 64 \
  --output benchmark/kv_baseline/results/baseline.csv
```

Run Redis `redis-py` benchmark. This can remain as an additional reference row, but it should not be the main final comparison against the raw-socket ClickHouse Redis-compatible endpoint benchmark:

```bash
python3 benchmark/kv_baseline/bench_redis.py \
  --keys-file benchmark/kv_baseline/keys_100k.txt \
  --duration 30 \
  --concurrency 8 \
  --batch-size 10 \
  --dataset-size 100000 \
  --value-size 64 \
  --output benchmark/kv_baseline/results/baseline.csv
```

The CSV schema is:

```csv
system,interface,dataset_size,value_size,batch_size,concurrency,qps,p50_ms,p95_ms,p99_ms
```

Generated datasets and large result files should not be committed.
