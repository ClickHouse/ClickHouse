---
toc_priority: 44
toc_title: TTL
---

#  Манипуляции с TTL таблицы {#manipuliatsii-s-ttl-tablitsy}

## MODIFY TTL {#modify-ttl}

Вы можете изменить [TTL для таблицы](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl) запросом следующего вида:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

## REMOVE TTL {#remove-ttl}

Убирает свойство TTL из выбранного вами столбца.

Синтаксис:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE TTL 
```

**Пример**

Запросы и результаты:

Создадим таблицу:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH;
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Чтобы провести фоновую очистку с помощью TTL, выполните:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
В результате видно, что вторая строка удалена.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

А вот теперь ничего не удалено.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

### Смотрите также

- Подробнее о [свойстве TTL](../../../engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/ttl/) <!--hide-->