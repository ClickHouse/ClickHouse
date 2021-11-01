---
toc_priority: 556
toc_title: executable
---

# executable {#executable}

Creates a table of the defined structure and fills it with data from a script or a query. ClickHouse looks for your custom scripts in the `user_scripts` folder (on Linux it is typically the `/var/lib/clickhouse/user_scripts` folder). 

**Syntax**

```sql
executable(script_line, format, structure[, query])
```

**Arguments**

- `script_line` — An execution line for a script. Can include not only script name, but also arguments, passed to the script. [String](../../sql-reference/data-types/string.md).
- `format` — [Format](../../interfaces/formats.md) of data, passed to the executed script. [String](../../sql-reference/data-types/string.md).
- `structure` — Defines columns and their types, as in [CREATE TABLE](../../sql-reference/statements/create/table.md) query. [String](../../sql-reference/data-types/string.md).
- `query` — A query, which results are passed to the executed script. Optional parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   A table of the defined structure, filled with data from the script or the query.

**Examples**

**1. Data passed in a script argument**

Consider the script `input_arg.sh`, that adds the word `Key` to the input argument:

```bash
#!/bin/bash
echo "Key $1"
```

The data is selected from a table, built by passing an argument to the `input_arg.sh` script:

```sql
SELECT * FROM executable('input_arg.sh 1', 'TabSeparated', 'value String');
```

Result:

```text
Key 1
```

**2. Data passed in a query**

Consider the script `input_script.sh`, that adds the word `Key` to the input strings:

```bash
#!/bin/bash
while read read_data; do printf "Key $read_data\n"; done
```

The data is selected from a table, built by streaming the data from the `SELECT 1` query to the `input_script.sh` script:

```sql
SELECT * FROM executable('input_script.sh', 'TabSeparated', 'value String', (SELECT 1));
```

Result:

```text
Key 1
```

**See Also**

-   [Executable](../../engines/table-engines/special/executable.md) table engine
-   [Executable](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-executable) dictionary