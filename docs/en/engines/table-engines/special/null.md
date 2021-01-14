---
toc_priority: 38
toc_title: 'Null'
---

# Null Table Engine {#null}

When writing to a `Null` table, data is ignored. When reading from a `Null` table, the response is empty.

!!! info "Hint"
    However, you can create a materialized view on a `Null` table. So the data written to the table will end up affecting the view, but original raw data will still be discarded.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/null/) <!--hide-->
