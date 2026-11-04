# Foreign writer fuzzing for data lake tables

A seeded, reproducible differential fuzzer for data lake tables written by other tools and read by ClickHouse, and for tables written by ClickHouse and read by other tools. It is a standalone tool, not part of the integration test suite: `test_fuzz.py` and `conftest.py` in this directory drive it with pytest on top of the integration test harness (`tests/integration/helpers`), and `foreign_writer_fuzz/` is the library. It runs nightly in the `NightlySQLancer` workflow, next to SQLancer, as the `Data lake fuzzer` jobs (`ci/jobs/datalake_fuzzer_job.py`) on a release and an ASan/UBSan build, with fresh random seeds and about 20 minutes per writer.

It targets the class of bugs where ClickHouse silently returns different data than the tool that wrote the table, or crashes on a table shape that tool produces: delete files and deletion vectors, schema and partition evolution, merge-on-read, statistics-based pruning, trivial `count()` from metadata, time travel, and metadata that ClickHouse writes in a form other readers reject.

## How it works

1. `operations.generate_history` draws a random table (schema over the type matrix of the writer, partitioning, writer options such as format version, delete mode, deletion vectors, column mapping, primary key) and a random list of operations from the seed. A history is plain JSON.
2. `runner.run_history` creates the table with the writer adapter and executes the operations one by one. Rows are generated from per-operation seeds, so a replayed or shrunk history writes the same values.
3. After every executed operation the writer reads the table back with its own reader (Spark, PyIceberg, delta-rs). `oracle.Oracle` compares ClickHouse with that read on every ClickHouse source of the table:
   - `content`: the full row multiset,
   - `count`: `count()` with and without `optimize_trivial_count_query`,
   - `aggregates`: `count(c)`, `minOrNull(c)`, `maxOrNull(c)`,
   - `filter`: rows and `count()` under random predicates (`NOT`, `AND`, `OR`, `IN`, `BETWEEN`, `IS NULL`, comparisons), evaluated in Python with SQL three-valued logic,
   - `time_travel`: an older live snapshot through `iceberg_snapshot_id` or `delta_lake_snapshot_version`.

   Every query runs with a random variant of the read settings that select the optimizations (partition pruning, manifest statistics, Parquet filter push-down, the Delta engine predicate, the query condition cache, `max_threads`, `max_block_size`, ...). Settings unknown to the server are dropped, so the harness runs against older builds too.
4. The server log is scanned after every step for `<Fatal>`, `Logical error` and sanitizer reports, and the server must still answer.
5. On a failure the history is truncated at the failing step and shrunk by delta debugging (`runner.shrink`) while it fails with the same signature (check, source, error code). The report contains the seed, the failing check with the query, settings and a row diff, the same query re-run with all optimizations off and with default settings, the minimal operation list and a replay command. The minimal history is also saved as JSON next to the instance logs.

Writer-side errors (a tool refusing an operation, or a bug in the tool) are not ClickHouse bugs: the operation is recorded as rejected and the history continues. A ClickHouse-written table that a foreign reader cannot open is a failure.

## Writers

| Name | Writer | ClickHouse sources | Operations |
|------|--------|--------------------|------------|
| `spark_iceberg` | Spark, Iceberg Hadoop catalog on MinIO, format version 1, 2 and 3 (deletion vectors) | `icebergS3` | append, overwrite (static, dynamic), delete, update, merge (copy-on-write and merge-on-read), add/drop/rename/move column, type promotion, add/drop partition field (identity, bucket, truncate, year, month, day, hour), `rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots`, write mode switch |
| `pyiceberg_rest` | PyIceberg through the Iceberg REST catalog | `icebergS3`, `DataLakeCatalog` | append, overwrite, delete, update and merge (upsert), schema and partition evolution, snapshot expiry; `uuid` columns |
| `spark_delta` | Spark `delta-spark` on MinIO | `deltaLake` | append, overwrite, delete, update, merge, with and without deletion vectors and column mapping, add/drop/rename/move column, type widening, `OPTIMIZE`, `VACUUM`, automatic checkpoints |
| `deltars` | delta-rs (`deltalake`) on MinIO | `deltaLake` | append, overwrite, delete, update, merge, add column, compaction, vacuum, checkpoint |
| `spark_hudi` | Spark Hudi, Copy-on-Write | `hudi` | insert, insert overwrite, upsert, delete |
| `spark_paimon` | Spark Paimon, append-only and primary-key tables, deletion vectors | `paimonS3` | insert, insert overwrite, delete, update, merge, schema evolution, compaction, snapshot expiry |
| `clickhouse_iceberg` | ClickHouse `IcebergS3`, read by PyIceberg and Spark | the table itself | insert, `ALTER DELETE/UPDATE`, add/drop/rename/modify column, `OPTIMIZE` |
| `clickhouse_delta` | ClickHouse `DeltaLake` (table created by delta-rs), read by delta-rs and Spark | the table itself | insert |

Not covered yet: Iceberg equality deletes (neither Spark nor PyIceberg writes them; Flink would), pypaimon, Trino, Azure storage, `*Cluster` table functions.

## Running

The job script starts docker-in-docker and the cluster in the integration test runner image, so run it through praktika from the repository root. It takes the binary from `ci/tmp/clickhouse`, `build/programs/clickhouse` or `--path`.

```bash
# The nightly budget (random seeds, 20 minutes per writer, about 3 hours):
python -m ci.praktika run "Data lake fuzzer (arm_release)" --workflow NightlySQLancer

# A short run for two writers with fresh random seeds:
python -m ci.praktika run "Data lake fuzzer (arm_release)" --workflow NightlySQLancer \
    --param FOREIGN_WRITER_FUZZ_WRITERS=spark_iceberg,spark_delta,FOREIGN_WRITER_FUZZ_TIME_BUDGET=300

# The smoke budget of the defaults in `budget.py` (seed 0, 6 operations, 30 seconds per writer):
python -m ci.praktika run "Data lake fuzzer (arm_release)" --workflow NightlySQLancer \
    --param FOREIGN_WRITER_FUZZ_BASE_SEED=0,FOREIGN_WRITER_FUZZ_SEEDS=1,FOREIGN_WRITER_FUZZ_OPS=6,FOREIGN_WRITER_FUZZ_TIME_BUDGET=30

# Replay a failure from its saved history (it is in `datalake_fuzzer_logs.tar.zst` of the job;
# put it under the repository, e.g. `ci/tmp/`, the job runs in a container):
python -m ci.praktika run "Data lake fuzzer (arm_release)" --workflow NightlySQLancer \
    --test test_fuzz.py::test_replay --param FOREIGN_WRITER_FUZZ_REPLAY=ci/tmp/<history>.json

# Regenerate it from the seed instead:
python -m ci.praktika run "Data lake fuzzer (arm_release)" --workflow NightlySQLancer \
    --param FOREIGN_WRITER_FUZZ_WRITERS=<writer>,FOREIGN_WRITER_FUZZ_SEED_LIST=<seed>
```

`--param` takes comma-separated `KEY=VALUE` pairs; a comma not followed by `KEY=` is part of the value. All knobs are listed in `budget.py`. `FOREIGN_WRITER_FUZZ_KNOWN_ISSUES=1` opens the gates of `known_issues.py`, which otherwise steer around open ClickHouse bugs so that the nightly run reports only new ones; delete an entry when its issue is fixed.

## Bugs found by the fuzzer

These were found by this fuzzer and filed afterwards; their gates in `known_issues.py` link the issues. Each was found on master (26.11.1.1329) with the seed shown and shrunk to the history below.

- https://github.com/ClickHouse/ClickHouse/issues/128306 `paimon_date_before_epoch` (`spark_paimon`, seed 0): a Paimon `DATE` column is read as `Date`, so `1969-12-31` fails with `Input value -1 is out of allowed Date range` (`VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`). Iceberg and Delta map `date` to `Date32`. Repro: Spark `CREATE TABLE paimon.default.t (id BIGINT, d DATE)`, `INSERT INTO paimon.default.t VALUES (1, DATE'1969-12-31')`, then `SELECT * FROM paimonS3(...)`.
- https://github.com/ClickHouse/ClickHouse/issues/128308 `paimon_schema_change_without_snapshot` (`spark_paimon`, seed 0, 8 operations): `ALTER TABLE ... ADD COLUMNS` writes `schema-1` but no snapshot; ClickHouse keeps using the schema of the latest snapshot, so `SELECT <new column>` fails with `UNKNOWN_IDENTIFIER` until the next commit, while Spark returns the column with NULLs.
- https://github.com/ClickHouse/ClickHouse/issues/127545 (repro comment https://github.com/ClickHouse/ClickHouse/issues/127545#issuecomment-6534985538) Paimon rename (`spark_paimon`, seed 0, 12 operations; related to https://github.com/ClickHouse/ClickHouse/issues/127545): after `RENAME COLUMN c751 TO c906` and a `MERGE INTO` that rewrites rows, ClickHouse returns NULL in `c906` for the rewritten rows. Gated together with the by-name resolution issue as `paimon_columns_by_name`.
- https://github.com/ClickHouse/ClickHouse/issues/128307 `paimon_row_type` (`spark_paimon`, seed 1): any Paimon `ROW` column makes the whole table unreadable with `Unsupported Paimon type: ROW` (`BAD_ARGUMENTS`); the type parser in `Paimon/Types.h` handles `ARRAY` and `MAP` but not `ROW`. Repro: Spark `CREATE TABLE paimon.default.t (id BIGINT, s STRUCT<f0: STRING>)`, insert a row, `SELECT * FROM paimonS3(...)`.
- https://github.com/ClickHouse/ClickHouse/issues/128301 `clickhouse_iceberg_v1_metadata` (`clickhouse_iceberg`, seed 1): after an `INSERT` into an `IcebergS3` table created with `iceberg_format_version = 1`, the new metadata file lacks the `schema` field that format version 1 requires; PyIceberg fails with `TableMetadataWrapper ... schema Field required`.
- https://github.com/ClickHouse/ClickHouse/issues/128303 `clickhouse_iceberg_bool` (`clickhouse_iceberg`, seed 1): `CREATE TABLE ... (c Nullable(Bool)) ENGINE = IcebergS3(...)` fails with `Unsupported type for iceberg Bool` (`getIcebergType` has no case for `Bool`), although Iceberg has `boolean` and ClickHouse reads it.

Rediscovered: https://github.com/ClickHouse/ClickHouse/issues/123954 (Hudi rows replaced by `insert_overwrite_table` are still read) and https://github.com/ClickHouse/ClickHouse/issues/127547 (a blank string Paimon partition value is looked up under `c=` and fails with `S3_ERROR` 404, `spark_paimon` seed 1). The gates for https://github.com/ClickHouse/ClickHouse/issues/114933, https://github.com/ClickHouse/ClickHouse/issues/114994 and https://github.com/ClickHouse/ClickHouse/issues/114854 were added up front because those issues are still open.

## Extending

- A new operation: an `OpSpec` in `operations.py` (`generate`, `validate`, `prepare`, `apply_schema`), `register` it, implement it in `Writer.execute` of the adapters that support it, and give it a weight in their `Capabilities.op_weights`.
- A new writer: subclass `writers.base.Writer`, declare its `Capabilities` (type matrix, partition transforms, predicate kinds its delete syntax can express, operation weights, random table options), implement `create`, `execute`, `read`, `sources` and optionally the time-travel methods, and add it to `writers.WRITERS`. Set `clickhouse_writes = True` when ClickHouse writes and `read` is a foreign reader.
- A new type: extend `ltypes.LType` (random values, DDL for Spark, pyarrow and ClickHouse, `canon`) and add it to the capabilities of the writers that support it.
- A new check or read setting: `oracle.Oracle` and `READ_SETTINGS_COMMON` / `READ_SETTINGS_FAMILY`.
- An open ClickHouse bug that the fuzzer keeps hitting: a `KnownIssue` with the issue link and a `gates.active(...)` check at the place that would trigger it.
