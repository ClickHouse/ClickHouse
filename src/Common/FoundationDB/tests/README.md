# FDB-related Unit Tests

All tests in this directory rely on a actual fdb cluster.
The following environment variables control these tests:

* `CLICKHOUSE_UT_FDB_CLUSTER_FILE`: Path to fdb.cluster. It can be found in
  /etc/foundationdb/fdb.cluster usually.
* `CLICKHOUSE_UT_FDB_LIBRARY_PATH`: (Optional) Path to save and keep libfdb_c.so.
   By default, libfdb_c.so is placed in the /tmp directory and cleaned up automatically.
   You can specify a path to keep it for debug or sanitizer.
   You are also responsible for cleaning up it manually.