---
toc_priority: 51
toc_title: TRUNCATE
---

# TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет все данные из таблицы. Если условие `IF EXISTS` не указано, запрос вернет ошибку, если таблицы не существует.

Запрос `TRUNCATE` не поддерживается для следующих движков: [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) и [Null](../../engines/table-engines/special/null.md).


[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/truncate/) <!--hide-->