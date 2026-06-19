# Join Order Benchmark (JOB)

The Join Order Benchmark (JOB) stresses the query optimizer with 113 analytical queries over a real-world, highly-correlated dataset (a snapshot of IMDB).

## Loading the data

The dataset is available as Parquet files. The bucket is public for reads. Create the schema, then load the tables, for example:

```bash
clickhouse client --data_type_default_nullable=1 --queries-file init.sql

for table in aka_name aka_title cast_info char_name comp_cast_type company_name \
             company_type complete_cast info_type keyword kind_type link_type \
             movie_companies movie_info movie_info_idx movie_keyword movie_link \
             name person_info role_type title; do
    clickhouse client --query \
        "INSERT INTO ${table} SELECT * FROM s3('https://s3.eu-west-3.amazonaws.com/public-pme/join_bench/job/${table}.parquet', NOSIGN, 'Parquet')"
done
```

`init.sql` is the original, unmodified JOB schema, where columns are nullable unless declared `NOT NULL`. The tables must therefore be created with `data_type_default_nullable=1` (passed as `--data_type_default_nullable=1` above), otherwise the nullable columns are created as non-nullable and loading rows with NULLs fails.

# List of known problems

On ClickHouse Cloud, use `init_cloud.sql` instead of `init.sql`: it is the same schema translated to explicit ClickHouse types to work around a [bug-97287](https://github.com/ClickHouse/ClickHouse/issues/97287) in the cloud shared catalog.

Original queries that return an empty result: 2c, 5a, 5b, 10b, 32a (5 of 113)
This is expected, see [Data set mismatch: empty queries results and reproducibility issues](https://github.com/gregrahn/join-order-benchmark/issues/11)