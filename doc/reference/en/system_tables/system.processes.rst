system.processes
----------------

This system table is used for implementing the ``SHOW PROCESSLIST`` query.
Columns:

.. code-block:: text

  user String              - Name of the user who made the request. For distributed query processing, this is the user who helped the requestor server send the query to this server, not the user who made the distributed request on the requestor server.

  address String           - The IP address the request was made from. The same for distributed processing.

  elapsed Float64          - The time in seconds since request execution started.

  rows_read UInt64         - The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.

  bytes_read UInt64        - The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.

  total_rows_approx UInt64 - The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.

  memory_usage UInt64      - How much memory the request uses. It might not include some types of dedicated memory.

  query String             - The query text. For INSERT, it doesn't include the data to insert.

  query_id String          - Query ID, if defined.
