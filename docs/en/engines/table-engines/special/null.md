---
sidebar_position: 50
sidebar_label:  'Null'
---

# Null Table Engine

When writing to a `Null` table, data is ignored. When reading from a `Null` table, the response is empty.

:::note
If you are wondering why this is useful, note that you can create a materialized view on a `Null` table. So the data written to the table will end up affecting the view, but original raw data will still be discarded.
:::


[Original article](https://clickhouse.com/docs/en/operations/table_engines/special/null/) <!--hide-->
