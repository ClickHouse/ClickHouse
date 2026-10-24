# Paimon object-storage test architecture

Two complementary tiers, following the Iceberg/Delta pattern:

| Tier | Suite | Storage | CI job | Purpose |
|---|---|---|---|---|
| Integration | `test_storage_paimon_with_spark/` (this suite) | MinIO + Azurite emulators; HDFS via the Hive image's namenode | Integration tests, every PR | Full behavioral matrix |
| E2E | `test_e2e_paimon/` | Real S3 + real Azure Blob | "Integration tests E2E" (gated) + nightly | Real auth/endpoint/TLS paths |

Test data is written by Spark with the Paimon catalog (the `paimon-spark-3.5`
bundle is baked into the integration runner image by
`ci/docker/integration/runner/Dockerfile`, guarded by the public
`test_paimon_spark_smoke` suite) into a local warehouse
and uploaded **in commit order** (data → manifests → snapshots → hint files),
the ordering real Paimon writers produce — Paimon table directories are
relocatable (metadata stores relative paths). A live Flink writer and a live
HDFS namenode run as sidecars for their modules. Shared helpers:
`helpers/paimon_utils.py`; the e2e cloud managers live in
`helpers/catalog_manager_paimon.py`.

## Module map {#module-map}

- `test_table_functions.py` — `paimonS3`/`paimonAzure` (+ `paimon`,
  `paimonCluster` aliases, cluster variants), virtual columns, the `structure`
  argument (pinned: Paimon metadata schema currently wins, ClickHouse#113361).
- `test_engines.py` — `PaimonS3`/`PaimonAzure`, the experimental gate, reload
  after restart, explicit `DETACH`/`ATTACH`, reads under parallel replicas,
  `Paimon` disk autodetection.
- `test_types.py` — full supported type matrix (composites are `NOT NULL`
  until ClickHouse#113337 is fixed).
- `test_partition_pruning.py` — INT/DATE/STRING/DECIMAL partitions, pruned
  file counts via `EngineFileLikeReadFiles`, path-escaped partition values.
- `test_schema_evolution.py` — ADD/RENAME/DROP column across snapshots (pins
  name-based resolution: pre-rename files read `NULL`).
- `test_snapshot_visibility.py` — new snapshots visible to table functions
  (per query) and engines (metadata refresh).
- `test_metadata_cache.py` — hit/miss accounting, `SYSTEM DROP PAIMON
  METADATA CACHE`, CREATE-time latch for engines, fewer object-storage GETs
  on warm cache, table-recreation key separation, size-bound enforcement,
  `SYSTEM RELOAD CONFIG` resizing, `max_entries`/`WeightLost` eviction
  accounting, and LRU/SLRU policy knob wiring (node2 runs an 8 KiB cache).
- `test_incremental_read.py` — Keeper-backed consumption (delivered id sets,
  not counts), `paimon_target_snapshot_id`, `max_consume_snapshots`, Keeper
  znode assertions, `{database}`/`{table}` keeper-path macros, disabled
  background refresh, HA competing consumers over a shared watermark,
  required-settings error, refreshable-MV pipeline.
- `test_compaction.py` — `DELETE` manifest entries honored by full scans;
  incremental reads skip `COMPACT` snapshots.
- `test_spark_mutations.py` — Spark `UPDATE`/`DELETE` copy-on-write served
  exactly post-mutation; the CDC primary-key journey as a **strict xfail**
  asserting correct merge semantics (flips when ClickHouse#113360 is fixed).
- `test_concurrent_reads.py` — readers never error nor observe a torn commit
  while the writer commits continuously.
- `test_fault_tolerance.py` — processing-lock contention, Keeper outage +
  recovery, missing data files + recovery, watermark durability across server
  restart, and the at-most-once loss window (activates when the failpoint
  from ClickHouse#113443 reaches binaries; skips with a reason until then).
- `test_scale.py` — 30 snapshots / ~120 files: full-scan correctness, pruning,
  bounded incremental draining with the delivered id set proven exactly once.
- `test_reader_compat.py` — checked-in foreign-writer fixtures: Spark with
  Paimon 1.3.2 and Flink with paimon-flink 1.1.1 (see `fixtures/README.md`).
- `test_flink_writer.py` — a live Flink 1.19 SQL client (sidecar) committing
  snapshot-by-snapshot into a warehouse consumed by Keeper incremental reads.
- `test_hdfs.py` — `paimonHDFS` + `PaimonHDFS` against the HDFS namenode
  embedded in the Hive test image (amd64-only image, ARM runners skip).
- `test_errors.py` — unsupported `ROW` type, unsupported and supported
  `scan.mode` values, unsupported schema version (hand-corrupted fixture),
  schema-without-snapshots, nonexistent path.

## Customer journey coverage {#customer-journey-coverage}

| Journey | Status |
|---|---|
| Batch analytics over Spark-written append tables (S3/Azure) | Covered (both tiers) |
| Dashboards / MV pipelines into MergeTree | Covered (`test_incremental_read.py` MV test) |
| Streaming ingestion via Keeper incremental reads | Covered incl. controls, faults, restart durability, exactly-once id accounting |
| HA ingestion: replicas as competing consumers on one keeper path | Covered (`test_shared_watermark_competing_consumers`) |
| GDPR deletes / corrections (Spark UPDATE/DELETE, copy-on-write) | Covered (`test_spark_mutations.py`) |
| Table maintenance (compaction) | Covered (`test_compaction.py`) |
| Schema changes on a live pipeline | Covered (`test_schema_evolution.py`) |
| CDC/upserts into primary-key tables | **Strict xfail** asserting correct merge semantics — flips when ClickHouse#113360 is fixed |
| Readers serving while the writer commits | Covered (`test_concurrent_reads.py`; uploads happen in commit order, as real writers commit) |
| Tables written by newer Paimon versions | Covered for 1.3.2 (`test_reader_compat.py`) |
| Flink-written tables (the dominant Paimon writer) | Covered: fixture + live Flink commits feeding incremental reads (`test_flink_writer.py`); the CDC-into-primary-key half is blocked on ClickHouse#113360 |
| Crash inside the at-most-once window | Test in place (`test_at_most_once_loss_window`); activates when the failpoint (ClickHouse#113443) reaches binaries |
| Role-based S3 auth (`extra_credentials(role_arn)`) | Covered at the shared S3 layer by `test_e2e_s3_role_arn` (identical parsing path; no per-format role tests, same as Iceberg/Delta) |
| Time travel ("AS OF") | Product gap: only `paimon_target_snapshot_id` in incremental mode |
| On-prem HDFS | Covered on amd64 (`test_hdfs.py`); ARM runners skip (amd64-only image) |

## Deliberately untested (and why) {#deliberately-untested}

- **Primary-key tables** — the reader has no merge-on-read and returns a raw
  union of data files (ClickHouse#113360); the correct semantics are asserted
  by a strict xfail rather than pinning the wrong output.
- **Deletion vectors** — not implemented by the reader.
- **Snapshot expiration** — the write-locally-then-upload strategy never
  deletes remote files, so expiry cannot be faithfully reproduced.
- **Deterministic torn commits** — uploads happen in commit order (like real
  writers), so readers-during-commits is covered stochastically by
  `test_concurrent_reads.py`; *forcing* a torn commit deterministically would
  require an out-of-order writer (see `test_missing_data_files` for the
  coarse version).
- **TIMESTAMP partitions** — broken upstream (ClickHouse#112768).
- **Kerberized HDFS** — deliberately not planned: no known customer, no
  KDC/kerberized image in the framework (the upstream kerberized-HDFS suites
  are skip-listed in this repo), and HDFS support itself is semi-deprecated.
  Revisit only with a customer attached.
- **`paimonHDFSCluster`** — the single-node Hive image cluster covers the
  non-cluster function and engine; fan-out is exercised by the S3/Azure
  cluster-function tests (same distribution code path).

## Running locally {#running-locally}

```bash
python -m ci.praktika run "integration" --test test_storage_paimon_with_spark
```

The e2e suite needs the data-lake e2e credentials in
`tests/integration/.env` (see `tests/integration/.env.example`):

```bash
cd tests/integration && pytest -m e2e test_e2e_paimon/test.py -v
```
