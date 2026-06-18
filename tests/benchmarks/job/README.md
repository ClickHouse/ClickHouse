# Join Order Benchmark (JOB)

The Join Order Benchmark (JOB) stresses the query optimizer with 113 analytical queries over a real-world, highly-correlated dataset (a snapshot of IMDB).

## Loading the data

The dataset is available as Parquet files. The bucket is public for reads. Create the schema, then load the tables, for example:

```bash
clickhouse client --queries-file init.sql

for table in aka_name aka_title cast_info char_name comp_cast_type company_name \
             company_type complete_cast info_type keyword kind_type link_type \
             movie_companies movie_info movie_info_idx movie_keyword movie_link \
             name person_info role_type title; do
    clickhouse client --query \
        "INSERT INTO ${table} SELECT * FROM s3('https://s3.eu-west-3.amazonaws.com/public-pme/join_bench/job/${table}.parquet', 'Parquet')"
done
```

# List of known problems

The following queries deviate slightly from the canonical JOB queries. The changes are noted in a comment at the top of the corresponding query file.

## Q2c, Q5b
The original queries return an empty result. The aggregate was switched from `MIN(...)` to `count(...)` so the query returns `0` instead of an empty row, making the result explicit.

## Q5a, Q10b, Q32a
The original queries return an empty result against this snapshot. The selection predicates were adjusted slightly to ensure a non-empty result.
