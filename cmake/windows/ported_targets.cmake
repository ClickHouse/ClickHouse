# The Windows port is not finished: `clickhouse-bundle` does not link yet, because Poco's
# Windows platform layer is missing from our fork and `src/Common` still calls a number of
# Linux-only interfaces. See docs/en/development/build-cross-windows.md for the state of it.
#
# Until the binary links, this aggregate target defines what *is* expected to build, so that
# the cross-build is verified on every pull request and cannot silently rot while the rest of
# the porting lands. CI builds it; see `ci/jobs/build_clickhouse.py`.
#
# The contents are derived from the contrib list rather than hand-copied from it, so a newly
# added third-party library is covered automatically. If yours does not build for Windows and
# nothing in `clickhouse-client`/`clickhouse-local` needs it, add it to
# `WINDOWS_UNPORTED_TARGETS` below with a note - do not silently drop the coverage.

set (WINDOWS_UNPORTED_TARGETS
    # Needs its `src/win/` sources selected instead of `src/unix/`. Only reached through
    # nats-io, cassandra and amqp-cpp, none of which is part of the client or local.
    _uv
    # Wants `iconv.h`, and its `off_t` use assumes LP64. Only reached through libhdfs3 and
    # the Azure SDK.
    _libxml2
    # Needs `BENCHMARK_STATIC_DEFINE`, otherwise its declarations stay `dllimport`. Only
    # built for `ENABLE_BENCHMARKS`.
    google_benchmark
    google_benchmark_main
    # libFuzzer's Windows support needs its own platform sources; only for `ENABLE_FUZZING`.
    _fuzzer
    _fuzzer_no_main
    # Supplies the math and memory functions only `cmake/linux/default_libs.cmake` links.
    libllvmlibc
    # Links `clickhouse_common_io`, so it cannot build before `src/Common` does.
    _roaring
)

# Collect the library targets defined under a directory and all of its subdirectories.
function (windows_collect_library_targets out_var dir)
    set (collected "")

    get_property (targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach (target ${targets})
        get_target_property (type ${target} TYPE)
        # `INTERFACE_LIBRARY` targets have nothing to compile, and `UTILITY` ones are custom
        # targets whose commands may well be host-only.
        if (type STREQUAL "STATIC_LIBRARY" OR type STREQUAL "OBJECT_LIBRARY")
            list (APPEND collected ${target})
        endif ()
    endforeach ()

    get_property (subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
    foreach (subdir ${subdirs})
        windows_collect_library_targets (sub_collected "${subdir}")
        list (APPEND collected ${sub_collected})
    endforeach ()

    set (${out_var} "${collected}" PARENT_SCOPE)
endfunction ()

windows_collect_library_targets (WINDOWS_PORTED_TARGETS "${ClickHouse_SOURCE_DIR}/contrib")
list (REMOVE_ITEM WINDOWS_PORTED_TARGETS ${WINDOWS_UNPORTED_TARGETS})

# The C++ runtime. Built from `contrib/llvm-project`, but wired up by `cmake/cxx.cmake` and
# `cmake/unwind.cmake` outside of `contrib/`, so they are not in the list collected above.
list (APPEND WINDOWS_PORTED_TARGETS cxx cxxabi unwind clang_rt_builtins)

list (LENGTH WINDOWS_PORTED_TARGETS _count)
message (STATUS "Windows port: ${_count} targets are expected to build (target 'clickhouse-windows-ported')")

add_custom_target (clickhouse-windows-ported)
add_dependencies (clickhouse-windows-ported ${WINDOWS_PORTED_TARGETS})
