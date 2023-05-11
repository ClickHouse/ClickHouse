# Embed fdb c binding library

Dynamic linking libfdb_c.so cause a lot of problems for deployment and testing.
This library try to embed libfdb_c.so in clickhouse binary by following steps:

1. Compile time
   1. Build libfdb_c.so by ExternalProject
   1. Embed libfdb_c.so by clickhouse_embed_binaries
1. Runtime
   1. Extract libfdb_c.so to /tmp/ by getResouce
   1. dlopen and load symbols

It provides a fdb_c wrapper to hide these steps, so there is almost no code modification required.
