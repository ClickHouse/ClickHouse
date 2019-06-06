
# Join

A prepared data structure for JOIN that is always located in RAM.

```
Join(ANY|ALL, LEFT|INNER, k1[, k2, ...])
```

Engine parameters: `ANY|ALL` – strictness; `LEFT|INNER` – type. For more information, see the [JOIN Clause](../../query_language/select.md#select-join) section.
These parameters are set without quotes and must match the JOIN that the table will be used for. k1, k2, ... are the key columns from the USING clause that the join will be made on.

The table can't be used for GLOBAL JOINs.

You can use INSERT to add data to the table, similar to the Set engine. For ANY, data for duplicated keys will be ignored. For ALL, it will be counted.

You can't perform SELECT directly from the table. There are two ways to retrieve data from table with Join engine
* use it as the "right-hand" table for JOIN
* use `joinGet` function, which allows to extract data from Join table with dictionary-like syntax.

Storing data on the disk is the same as for the Set engine.

You can customize several settings when creating JOIN table with the following syntax:
```
CREATE TABLE join_any_left_null ( ... ) ENGINE = Join(ANY, LEFT, ...) SETTINGS join_use_nulls = 1;
```

The following setting are supported by JOIN engine:
* `join_use_nulls`
* `max_rows_in_join`
* `max_bytes_in_join`
* `join_overflow_mode`
* `join_any_take_last_row` 


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/join/) <!--hide-->
