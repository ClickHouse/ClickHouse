## Paimon Incremental Read Integration Test

This test validates Paimon incremental read with:

- A dedicated writer jar (`paimon-incremental-writer`) built inside the
  `clickhouse/integration-test-with-hms` Docker image
- A ClickHouse table created by `ENGINE = PaimonLocal(...)`
- Incremental reads with Keeper state

### Test entry

- `test.py`

### Writer project

- Source: `ci/docker/integration/clickhouse_with_hms_catalog/paimon-incremental-data/`
- Main class: `org.apache.paimon.rest.PaimonIncrementalWriter`
- Jar path inside the Docker image: `/opt/paimon/paimon-incremental-writer.jar`

### How it works

The writer jar is built from source during the Docker image build
(`clickhouse/integration-test-with-hms`) via a multi-stage Dockerfile.
At test time, a sidecar container running the same image shares a Docker
named volume with the ClickHouse node. The test invokes the writer via
`docker exec` into the sidecar container.

### Assertions covered

0. Warm-up snapshot (1 row) is written and consumed first, to ensure schema can be inferred when creating `PaimonLocal` table
1. First business incremental read after snapshot-2 returns `10`
2. Next read without new snapshot returns `0`
3. First business incremental read after snapshot-3 returns `10`
4. Next read without new snapshot returns `0`
5. `paimon_target_snapshot_id = 2` is deterministic and returns `10` repeatedly
6. With `max_consume_snapshots = 2`, after warm-up consumption and then 3 commits (10 rows per snapshot), reads are capped per query: `20`, then `10`, then `0`
7. Refreshable MV pipeline: Paimon (incremental read) -> MV (APPEND) -> MergeTree accumulates rows correctly
