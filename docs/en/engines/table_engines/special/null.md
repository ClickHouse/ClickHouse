---
toc_priority: 38
toc_title: 'Null'
---

# Null {#null}

When writing to a Null table, data is ignored. When reading from a Null table, the response is empty.

However, you can create a materialized view on a Null table. So the data written to the table will end up in the view.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/null/) <!--hide-->
