---
toc_priority: 50
toc_title: VIEW
---

# Выражение ALTER TABLE … MODIFY QUERY {#alter-modify-query}

Вы можеие изменить запрос `SELECT`, который был задан при создании [материализованного представления](../create/view.md#materialized), с помощью запроса 'ALTER TABLE … MODIFY QUERY'. Используйте его если при создании материализованного представления не использовалась секция `TO [db.]name`. Настройка `allow_experimental_alter_materialized_view_structure` должна быть включена. 

Если при создании материализованного представления использовалась конструкция `TO [db.]name`, то для изменения отсоедините представление с помощью [DETACH](../detach.md), измените таблицу с помощью [ALTER TABLE](index.md), а затем снова присоедините запрос с помощью [ATTACH](../attach.md).

**Пример**

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table; 
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```
```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

## Выражение ALTER LIVE VIEW {#alter-live-view}

Выражение `ALTER LIVE VIEW ... REFRESH` обновляет [Live-представление](../create/view.md#live-view). См. раздел [Force Live View Refresh](../create/view.md#live-view-alter-refresh).
