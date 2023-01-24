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

Удалить табличный TTL можно запросом следующего вида:

```sql
ALTER TABLE table_name REMOVE TTL
```

**Пример**

Создадим таблицу с табличным `TTL` и заполним её данными:

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

Выполним `OPTIMIZE` для принудительной очистки по `TTL`:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl;
```
В результате видно, что вторая строка удалена.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Удаляем табличный `TTL`:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Заново вставляем удаленную строку и снова принудительно запускаем очистку по `TTL` с помощью `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl;
```

`TTL` больше нет, поэтому данные не удаляются:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

### Смотрите также

- Подробнее о [свойстве TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl).
- Изменить столбец [с TTL](../../../sql-reference/statements/alter/column.md#alter_modify-column).