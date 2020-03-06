# Generate {#table_engines-generate}

The Generate table engine produces random data for given table schema.

Usage examples:

- Use in test to populate reproducible large table.
- Generate random input for fuzzing tests.

## Usage in ClickHouse Server

```sql
Generate(max_array_length, max_string_length, random_seed)
```

The `max_array_length` and `max_string_length` parameters specify maximum length of all
array columns and strings correspondingly in generated data.

Generate table engine supports only `SELECT` queries.

It supports all [DataTypes](../../data_types/index.md) that can be stored in a table except `LowCardinality` and `AggregateFunction`.

**Example:**

**1.** Set up the `generate_engine_table` table:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE=Generate(3, 5, 1)
```

**2.** Query the data:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Details of Implementation
- Not supported:
    - `ALTER`
    - `SELECT ... SAMPLE`
    - `INSERT`
    - Indices
    - Replication

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
