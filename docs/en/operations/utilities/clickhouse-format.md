---
slug: /en/operations/utilities/clickhouse-format
title: clickhouse-format
---

Allows formatting input queries.

Keys:

- `--help` or`-h` — Produce help message.
- `--query` — Format queries of any length and complexity.
- `--hilite` — Add syntax highlight with ANSI terminal escape sequences.
- `--oneline` — Format in single line.
- `--max_line_length` — Format in single line queries with length less than specified.
- `--comments` — Keep comments in the output.
- `--quiet` or `-q` — Just check syntax, no output on success.
- `--multiquery` or `-n` — Allow multiple queries in the same file.
- `--obfuscate` — Obfuscate instead of formatting.
- `--seed <string>` — Seed arbitrary string that determines the result of obfuscation.
- `--backslash` — Add a backslash at the end of each line of the formatted query. Can be useful when you copy a query from web or somewhere else with multiple lines, and want to execute it in command line.

## Examples {#examples}

1. Formatting a query:

```bash
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

Result:

```bash
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. Highlighting and single line:

```bash
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

Result:

```sql
SELECT sum(number) FROM numbers(5)
```

3. Multiqueries:

```bash
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

Result:

```
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. Obfuscating:

```bash
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

Result:

```
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

Same query and another seed string:

```bash
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

Result:

```
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. Adding backslash:

```bash
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

Result:

```
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
