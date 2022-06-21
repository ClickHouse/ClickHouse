---
---

# GROUPING

## GROUPING SETS

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number % 2, number) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) = 1 AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, grouping(number, number % 2) = 1;
```

Response
```response
```

```sql
SELECT
    number,
    count(),
    grouping(number, number % 2) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number, number % 2),
        ()
    )
ORDER BY (gr, number);
```

Response
```response
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2) = 2
ORDER BY number
```

Response
```response
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2) = 1
ORDER BY number
```

Response
```response
```

```sql
SELECT
    number,
    GROUPING(number, number % 2) = 1 as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    GROUPING SETS (
    (number),
    (number % 2))
ORDER BY number, gr;
```

Response
```response
```


```sql
SELECT
    number,
    grouping(number, number % 2) = 3
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number),
    GROUPING(number % 2)
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH ROLLUP
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2)
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH CUBE
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) + 3 as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
HAVING grouping(number) != 0
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
ORDER BY
    number, gr;
```

Response
```response
```

```sql
SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
ORDER BY
    number, gr;
```

Response
```response
```
