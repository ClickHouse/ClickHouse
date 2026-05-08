## Build Target Name

- Symptom: `ninja -C build clickhouse-server` failed with `unknown target 'clickhouse-server'`.
- Root cause: This build directory exposes the server binary through the aggregate `clickhouse` target and the handler object through `src/CMakeFiles/dbms.dir/Server/RedisHandler.cpp.o`; there is no `clickhouse-server` target.
- Fix: Used `ninja -C build -t targets` to find the available target and switched verification to the touched translation unit target.
- Verification: `ninja -C build src/CMakeFiles/dbms.dir/Server/RedisHandler.cpp.o > build/redis_get_build.log 2>&1` completed successfully.

## Sandbox `ccache` Write Failure

- Symptom: `ninja -C build clickhouse` failed immediately with repeated `ccache: error: Read-only file system`.
- Root cause: The default sandbox prevented `ccache` from writing to its cache while compiling.
- Fix: Re-ran the build command with escalated sandbox permissions.
- Verification: The escalated targeted compile of `src/CMakeFiles/dbms.dir/Server/RedisHandler.cpp.o` completed successfully.

## `build-new` Sandbox `ccache` Write Failure

- Symptom: `ninja -C build-new clickhouse-server-lib` failed while compiling `src/CMakeFiles/dbms.dir/Server/RedisHandler.cpp.o` and `src/CMakeFiles/dbms.dir/Server/RedisHandlerFactory.cpp.o`, with `ccache: error: Read-only file system`.
- Root cause: The default sandbox prevented `ccache` from writing while using the correct `build-new` directory.
- Fix: Re-ran the same `build-new` command with escalated sandbox permissions.
- Verification: `ninja -C build-new clickhouse-server-lib > build-new/build_redis_get.log 2>&1` completed successfully with escalated sandbox permissions.
