# system.processes {#system_tables-processes}

This system table is used for implementing the `SHOW PROCESSLIST` query.

Columns:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` user. The field contains the username for a specific query, not for a query that this query initiated.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` on the query requestor server.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage) setting.
-   `query` (String) – The query text. For `INSERT`, it doesn’t include the data to insert.
-   `query_id` (String) – Query ID, if defined.
