# Redis `UInt64` Key Implementation Issues

## Build Target Name

- Symptom: `ninja -C build-new clickhouse-server` failed with `unknown target 'clickhouse-server'`.
- Root cause: this build tree exposes `clickhouse`, `clickhouse-server-lib`, and `libclickhouse-server-lib.a`, but not a `clickhouse-server` target.
- Fix: use `ninja -C build-new clickhouse-server-lib` for a focused compile check of the server library.
- Verification: `ninja -C build-new clickhouse-server-lib` reached `src/Server/RedisHandler.cpp` compilation.

## Sandbox `ccache` Write Failure

- Symptom: the first `ninja -C build-new clickhouse-server-lib` attempt failed while compiling `src/Server/RedisHandler.cpp` with `ccache: error: Read-only file system`.
- Root cause: the sandboxed command could not write the compiler cache/output path required by this build.
- Fix: rerun the same command with escalated local filesystem permissions and redirect output to `build-new/redis_uint64_compile.log`.
- Verification: the rerun completed successfully; the log ends with `Building CXX object src/CMakeFiles/dbms.dir/Server/RedisHandler.cpp.o` followed by `Linking CXX static library src/libdbms.a`.

## Code Compile Result

- No implementation-related compile error was observed.
- No runtime issue was observed because runtime/manual Redis endpoint checks were not run in this step.
