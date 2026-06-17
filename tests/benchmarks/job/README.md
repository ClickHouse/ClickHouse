# Join Order Benchmark (JOB)

The Join Order Benchmark (JOB) stresses the query optimizer with 113 analytical queries over a
real-world, highly-correlated dataset (a snapshot of IMDB). It was introduced in the paper
["How Good Are Query Optimizers, Really?"](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)
(Leis et al., VLDB 2015) and is the de facto benchmark for evaluating join ordering and
cardinality estimation.

The 113 queries are organized into 33 families (`1`–`33`); the queries within a family
(`a`, `b`, `c`, ...) share the same join graph but differ in their selection predicates.

## Loading the data

The dataset is available as Parquet files. Create the schema, then load the tables, for example:

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

The bucket is public for reads, so no credentials are required.

## Preparing the data from the original CSV files

The Parquet files above are derived from the original IMDB snapshot used by JOB, which is
distributed as one CSV file per table (`aka_name.csv`, `title.csv`, ...). These CSVs use
PostgreSQL `COPY` semantics with `ESCAPE '\'`: a backslash escapes the quote character only
inside a quoted field, while outside quotes a backslash is a literal character. ClickHouse expects
RFC 4180 CSV (doubled quotes, no backslash escaping), so the files must be re-encoded first.

`convert_csv.py` (included in this folder) performs that re-encoding. It reads the original CSV on
stdin and writes standard CSV on stdout, doubling embedded quotes and preserving empty unquoted
fields (which ClickHouse maps to `NULL` for `Nullable` columns).

To build the tables from the original CSVs:

```bash
clickhouse client --queries-file init.sql

for table in aka_name aka_title cast_info char_name comp_cast_type company_name \
             company_type complete_cast info_type keyword kind_type link_type \
             movie_companies movie_info movie_info_idx movie_keyword movie_link \
             name person_info role_type title; do
    python3 convert_csv.py < "${table}.csv" \
        | clickhouse client --query "INSERT INTO ${table} FORMAT CSV"
done
```

Once the tables are populated, they can be exported to Parquet for faster re-import later, e.g.
`clickhouse client --query "SELECT * FROM title ORDER BY id FORMAT Parquet" > title.parquet`.

# List of known problems

The following queries deviate slightly from the canonical JOB queries. The changes are noted in a
comment at the top of the corresponding query file.

## Q2c, Q5b
The original queries return an empty result. The aggregate was switched from `MIN(...)` to
`count(...)` so the query returns `0` instead of an empty row, making the result explicit.

## Q5a, Q10b, Q32a
The original queries return an empty result against this snapshot. The selection predicates were
adjusted slightly to ensure a non-empty result.
