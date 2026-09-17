# Join Order Benchmark (JOB)

The Join Order Benchmark (JOB) stresses the query optimizer with 113 analytical queries over a real-world, highly-correlated dataset (a snapshot of IMDB).

Source of the queries: [Join Order Benchmark](https://github.com/gregrahn/join-order-benchmark) repository.

## Loading the data

`init.sql` is the original, unmodified JOB schema, where columns are nullable unless declared `NOT NULL`. The tables must therefore be created with `data_type_default_nullable=1` (passed as `--data_type_default_nullable=1` below), otherwise the nullable columns are created as non-nullable and loading rows with NULLs fails.

Create the schema, for example:

```bash
clickhouse client --data_type_default_nullable=1 --queries-file init.sql
```
For IMDB Data Set check [Join Order Benchmark](https://github.com/gregrahn/join-order-benchmark) repository.

# List of known problems

* On ClickHouse Cloud, use `init_cloud.sql` instead of `init.sql`: it is the same schema translated to explicit ClickHouse types to work around a [bug-97287](https://github.com/ClickHouse/ClickHouse/issues/97287) in the cloud shared catalog.

* Original queries that return an empty result: 2c, 5a, 5b, 10b, 32a (5 of 113). This is expected, see [Data set mismatch: empty queries results and reproducibility issues](https://github.com/gregrahn/join-order-benchmark/issues/11)