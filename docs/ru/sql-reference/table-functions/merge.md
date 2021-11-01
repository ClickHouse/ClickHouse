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

- `db_name` — имя БД или регулярное выражение для отбора БД. Можно использовать выражение, возвращающее строку с именем БД, например, `currentDatabase()`. 

- `tables_regexp` — ы

**См. также**

-   Табличный движок [Merge](../../engines/table-engines/special/merge.md)
