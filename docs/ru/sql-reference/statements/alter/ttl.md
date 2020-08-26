---
toc_priority: 44
toc_title: TTL
---

#  Манипуляции с TTL таблицы {#manipuliatsii-s-ttl-tablitsy}

Вы можете изменить [TTL для таблицы](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl) запросом следующего вида:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/ttl/) <!--hide-->