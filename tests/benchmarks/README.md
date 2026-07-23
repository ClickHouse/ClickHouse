# Benchmarks

SQL benchmarks for ClickHouse. Each subdirectory contains a self-contained benchmark with the following structure:


| File / Directory | Description                                                                                                |
| ---------------- | ---------------------------------------------------------------------------------------------------------- |
| `init.sql`       | Schema definitions (CREATE TABLE statements)                                                               |
| `settings.json`  | ClickHouse settings to use when running the queries (to make it SQL-standard compliant) |
| `queries/`       | The query files                                                                                  |
