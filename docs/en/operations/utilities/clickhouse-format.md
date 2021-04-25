---
toc_priority: 65
toc_title: clickhouse-format
---

# clickhouse-format {#clickhouse-format}

The `clickhouse-format` program enables formatting input queries.

Keys:

- `--help` or`-h` — Produce help message
- `--hilite` — Add syntax highlight with ANSI terminal escape sequences
- `--oneline` — Format in single line
- `--quiet` or `-q` — Just check syntax, no output on success
- `--multiquery` or `-n` — Allow multiple queries in the same file
- `--obfuscate` — Obfuscate instead of formatting
- `--seed <string>` — Seed arbitrary string that determines the result of obfuscation
- `--backslash` — Add a backslash at the end of each line of the formatted query. Can be useful when you copy a query from web or somewhere else with multiple lines, and want to execute it in command line.

## Examples {#examples} 

1. Example with highlighting and single line:

```bash
$clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

Result:

```sql
SELECT sum(number) FROM numbers(5)
```

2. Example with multiqueries: 

```bash
$clickhouse-format -n <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```
    
Result:

```text
SELECT 1
;

SELECT 1
UNION ALL
(
    SELECT 1
    UNION DISTINCT
    SELECT 3
)
;
```

3. Example with obfuscating:

```bash
$clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```
    
Result:

```text
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```
   
Another seed string:

```bash
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

Result:

```text
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

4. Example with backslash:

```bash
$clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

Result:

```text
SELECT * \
FROM  \
( \
    SELECT 1 AS x \
    UNION ALL \
    SELECT 1 \
    UNION DISTINCT \
    SELECT 3 \
)
``` 