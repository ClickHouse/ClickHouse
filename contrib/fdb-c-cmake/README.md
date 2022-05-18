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

## How to upgrade fdb?

Upgrading fdb requires some manual steps.

1. Update FDB_VERSION, FDB_API_VERSION and patchs in CMakeLists.txt
1. Build the fdb ep: `cmake --build . --target foundationdb_ep`
1. Copy `${CMAKE_BINARY_DIR}/contrib/fdb-c-cmake/foundationdb_ep-prefix/include/foundationdb` to `include/foundationdb/internal`
1. Patch `fdb_c.h` to remove `extern "C"` and add namespace. Here is an example patch file.

   ``` patch
   diff --git a/contrib/fdb-c-cmake/include/foundationdb/internal/fdb_c.h b/contrib/fdb-c-cmake/include/foundationdb/internal/fdb_c.h
   index 273d014827d..70c6a02419c 100644
   --- a/contrib/fdb-c-cmake/include/foundationdb/internal/fdb_c.h
   +++ b/contrib/fdb-c-cmake/include/foundationdb/internal/fdb_c.h
   @@ -60,9 +60,8 @@
   #include "fdb_c_options.g.h"
   #include "fdb_c_types.h"
   
   -#ifdef __cplusplus
   -extern "C" {
   -#endif
   +namespace foundationdb::api
   +{
   
   DLLEXPORT const char* fdb_get_error(fdb_error_t code);
   
   @@ -571,8 +570,5 @@ DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range_selector(FDBTr
   #endif
   
   DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_delay(double seconds);
   -
   -#ifdef __cplusplus
   }
   #endif
   -#endif
   ```

1. Update api lists in fdb_c wrapper `include/foundationdb/fdb_c.h`
1. Re-run cmake to build anything

