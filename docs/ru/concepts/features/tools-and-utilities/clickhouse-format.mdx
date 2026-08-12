---
description: 'Руководство по использованию утилиты format для работы с форматами данных ClickHouse'
slug: /operations/utilities/clickhouse-format
title: 'clickhouse-format'
doc_type: 'reference'
---

Позволяет форматировать входные запросы.

Ключи:

* `--help` или `-h` — Вывести справочное сообщение.
* `--query` — Форматировать запросы любой длины и сложности.
* `--hilite` или `--highlight` — Добавить подсветку синтаксиса с помощью ANSI escape-последовательностей терминала.
* `--oneline` — Форматировать в одну строку.
* `--max_line_length` — Форматировать в одну строку запросы, длина которых меньше указанной.
* `--comments` — Сохранять комментарии в выводе.
* `--quiet` или `-q` — Только проверить синтаксис, без вывода при успешном выполнении.
* `--multiquery` или `-n` — Разрешить несколько запросов в одном файле.
* `--obfuscate` — Выполнять обфускацию вместо форматирования.
* `--seed <string>` — Произвольная строка seed, определяющая результат обфускации.
* `--backslash` — Добавить обратную косую черту в конец каждой строки форматированного запроса. Это может быть полезно, если вы копируете многострочный запрос из веб-интерфейса или откуда-то ещё и хотите выполнить его в командной строке.
* `--semicolons_inline` — В режиме multiquery ставить точки с запятой в последней строке запроса, а не на новой строке.

<div id="examples">
  ## Примеры
</div>

1. Форматирование запроса:

```bash title="Query"
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

```bash title="Response"
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. Подсветка и вывод в одну строку:

```bash title="Query"
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

```sql title="Response"
SELECT sum(number) FROM numbers(5)
```

3. Мультизапросы:

```bash title="Query"
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

```sql title="Response"
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. Обфускация:

```bash title="Query"
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

Тот же запрос, но с другой строкой seed:

```bash title="Query"
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. Добавление обратной косой черты:

```bash title="Query"
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

```sql title="Response"
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