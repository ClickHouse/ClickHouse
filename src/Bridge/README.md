Contains functionality to implement "bridges". A bridge is a separate binary which provides special functionality that is deliberately not
part of the ClickHouse server. Examples:
- the odbc-bridge - for connecting to ClickHouse via ODBC
- the library-bridge - for loading libraries at runtime (e.g. libcatboost.ai)

The clickhouse server and the bridge binary communicate via HTTP.

Separate bridge binaries exist but the functionality that they provide is usually not very useful and are often buggy, can lead to sporadic crashes, etc.
We don't want problems to spill into the address space of the clickhouse server.
