---
toc_priority: 38
toc_title: merge
---

# merge {#merge}

Cоздаёт временную таблицу типа [Merge](../../engines/table-engines/special/merge.md). Структура таблицы берётся из первой попавшейся таблицы, подходящей под регулярное выражение.

**Синтаксис**

```sql
merge('db_name', 'tables_regexp')
```
**Аргументы**

- `db_name` — Возможные варианты:
    - имя БД, 
    - выражение, возвращающее строку с именем БД, например, `currentDatabase()`,
    - `REGEXP(expression)`, где `expression` — регулярное выражение для отбора БД.

- `tables_regexp` — регулярное выражение для имен таблиц в указанной БД или нескольких БД.

**См. также**

-   Табличный движок [Merge](../../engines/table-engines/special/merge.md)
