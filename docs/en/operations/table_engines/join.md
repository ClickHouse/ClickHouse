
# Join

A prepared data structure for JOIN that is always located in RAM.

```
Join(ANY|ALL, LEFT|INNER, k1[, k2, ...])
```

Engine parameters: `ANY|ALL` – strictness; `LEFT|INNER` – type. For more information, see the [JOIN Clause](../../query_language/select.md#select-join) section.
These parameters are set without quotes and must match the JOIN that the table will be used for. k1, k2, ... are the key columns from the USING clause that the join will be made on.

The table can't be used for GLOBAL JOINs.

You can use INSERT to add data to the table, similar to the Set engine. For ANY, data for duplicated keys will be ignored. For ALL, it will be counted. You can't perform SELECT directly from the table. The only way to retrieve data is to use it as the "right-hand" table for JOIN.

Storing data on the disk is the same as for the Set engine.


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/join/) <!--hide-->
