<a name="table_engines-view"></a>

# View

Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query).


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/view/) <!--hide-->
