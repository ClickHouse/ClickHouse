---
toc_priority: 42
toc_title: View
---

# View Table Engine {#table_engines-view}

Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query).

[Original article](https://clickhouse.com/docs/en/operations/table_engines/view/) <!--hide-->
