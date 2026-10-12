---
name: create-test-datasets
description: Create test datasets (hits, visits, tpcds, tpch) from standard scripts. Ensures the server is running first.
argument-hint: [hits] [visits] [tpcds] [tpch]
disable-model-invocation: false
allowed-tools: Bash(clickhouse-client:*), Bash(clickhouse:*), Bash(pgrep:*), Bash(ls:*), Bash(cat:*), Bash(bash:*), Read, Glob, Grep, AskUserQuestion
---

# Create Test Datasets Skill

Set up test datasets by executing standard creation scripts. Supports `hits`, `visits`, `tpcds`, and `tpch`.

## Arguments

- `$ARGS` (optional): Space-separated list of dataset names from `{hits, visits, tpcds, tpch}`. If no arguments are provided, defaults to `hits visits`.

Examples:
- `/create-test-datasets` — sets up `hits` and `visits` (default)
- `/create-test-datasets hits` — sets up only `hits`
- `/create-test-datasets tpch tpcds` — sets up TPC-H and TPC-DS
- `/create-test-datasets hits visits tpch` — sets up all three

## Process

### 1. Parse arguments

Parse the argument string into a set of requested datasets. Valid values are `hits`, `visits`, `tpcds`, `tpch`. If no arguments are provided, use `{hits, visits}` as the default. If any argument is not in the valid set, report an error and stop.

### 2. Verify the server is running

```bash
clickhouse-client -q "SELECT 1" 2>&1
```

If the server is not reachable, report an error and stop. Do **not** attempt to start the server — ask the user to start it first.

### 3. Check for conflicting tables

Before making any changes, check whether any target tables already exist. Query `system.tables` once:

```bash
clickhouse-client -q "SELECT database || '.' || name FROM system.tables WHERE
    (database = 'test' AND name IN ('hits', 'visits'))
    OR (database = 'tpcds')
    OR (database = 'tpch')"
```

Build a list of conflicting tables based on what was requested:
- If `hits` is requested: `test.hits` must not exist.
- If `visits` is requested: `test.visits` must not exist.
- If `tpcds` is requested: the `tpcds` database must have no tables.
- If `tpch` is requested: the `tpch` database must have no tables.

If any conflicts are found, report all of them to the user and stop. Do **not** drop or overwrite anything. Suggest the user drop the conflicting tables manually and rerun the skill.

### 4. Set up hits and/or visits

Only execute this step if `hits` or `visits` (or both) are in the requested set.

The file `tests/docker_scripts/create.sql` contains two `CREATE TABLE` statements: one for `datasets.hits_v1` and one for `datasets.visits_v1`. For each requested dataset, extract only the corresponding statement, replace the table name to create it directly as `test.hits` or `test.visits`, and execute it. This avoids creating unnecessary intermediate tables.

```bash
clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test"
```

If `hits` is requested — extract the `datasets.hits_v1` statement, replace the name, and execute:
```bash
clickhouse-client --multiquery <<< "$(sed -n '/^CREATE TABLE datasets\.hits_v1/,/);/p' tests/docker_scripts/create.sql | sed 's/datasets\.hits_v1/test.hits/')"
```

If `visits` is requested — extract the `datasets.visits_v1` statement, replace the name, and execute:
```bash
clickhouse-client --multiquery <<< "$(sed -n '/^CREATE TABLE datasets\.visits_v1/,/);/p' tests/docker_scripts/create.sql | sed 's/datasets\.visits_v1/test.visits/')"
```

### 5. Set up TPC-DS

Only execute this step if `tpcds` is in the requested set.

```bash
bash tests/docker_scripts/create_tpcds.sh
```

### 6. Set up TPC-H

Only execute this step if `tpch` is in the requested set.

```bash
bash tests/docker_scripts/create_tpch.sh
```

### 7. Verify

For each dataset that was set up, confirm it exists and report row counts:

- If `hits`: `SELECT 'test.hits', count() FROM test.hits`
- If `visits`: `SELECT 'test.visits', count() FROM test.visits`
- If `tpcds`: `SELECT name, total_rows FROM system.tables WHERE database = 'tpcds' ORDER BY name`
- If `tpch`: `SELECT name, total_rows FROM system.tables WHERE database = 'tpch' ORDER BY name`

Report all results to the user.
