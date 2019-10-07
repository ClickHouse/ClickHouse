option (SANITIZE "Enable sanitizer: address, memory, thread, undefined" "")

set (SAN_FLAGS "${SAN_FLAGS} -g -fno-omit-frame-pointer -DSANITIZER")

if (SANITIZE)
    if (SANITIZE STREQUAL "address")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libasan")
        endif ()

    elseif (SANITIZE STREQUAL "memory")
        # MemorySanitizer flags are set according to the official documentation:
        # https://clang.llvm.org/docs/MemorySanitizer.html#usage
        #
        # For now, it compiles with `cmake -DSANITIZE=memory -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS_ADD="-O1" -DCMAKE_C_FLAGS_ADD="-O1"`
        # Compiling with -DCMAKE_BUILD_TYPE=Debug leads to ld.lld failures because
        # of large files (was not tested with ld.gold). This is why we compile with
        # RelWithDebInfo, and downgrade optimizations to -O1 but not to -Og, to
        # keep the binary size down.
        # TODO: try compiling with -Og and with ld.gold.
        set (MSAN_FLAGS "-fsanitize=memory -fsanitize-memory-track-origins -fno-optimize-sibling-calls")

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")

        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libmsan")
        endif ()

        # Temporarily disable many external libraries that don't work under
        # MemorySanitizer yet.
        set (ENABLE_HDFS 0 CACHE BOOL "")
        set (ENABLE_CAPNP 0 CACHE BOOL "")
        set (ENABLE_RDKAFKA 0 CACHE BOOL "")
        set (ENABLE_ICU 0 CACHE BOOL "")
        set (ENABLE_POCO_MONGODB 0 CACHE BOOL "")
        set (ENABLE_POCO_NETSSL 0 CACHE BOOL "")
        set (ENABLE_POCO_ODBC 0 CACHE BOOL "")
        set (ENABLE_ODBC 0 CACHE BOOL "")
        set (ENABLE_MYSQL 0 CACHE BOOL "")
        set (ENABLE_EMBEDDED_COMPILER 0 CACHE BOOL "")
        set (USE_INTERNAL_CAPNP_LIBRARY 0 CACHE BOOL "")
        set (USE_SIMDJSON 0 CACHE BOOL "")
        set (ENABLE_READLINE 0 CACHE BOOL "")
        set (ENABLE_ORC 0 CACHE BOOL "")
        set (ENABLE_PARQUET 0 CACHE BOOL "")
        set (USE_CAPNP 0 CACHE BOOL "")
        set (USE_INTERNAL_ORC_LIBRARY 0 CACHE BOOL "")
        set (USE_ORC 0 CACHE BOOL "")
        set (ENABLE_SSL 0 CACHE BOOL "")

    elseif (SANITIZE STREQUAL "thread")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=thread")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=thread")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libtsan")
        endif ()

    elseif (SANITIZE STREQUAL "undefined")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libubsan")
        endif ()

    elseif (SANITIZE STREQUAL "libfuzzer")
        # NOTE: Eldar Zaitov decided to name it "libfuzzer" instead of "fuzzer" to keep in mind another possible fuzzer backends.
        # NOTE: no-link means that all the targets are built with instrumentation for fuzzer, but only some of them (tests) have entry point for fuzzer and it's not checked.
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=fuzzer-no-link,address,undefined -fsanitize-address-use-after-scope")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=fuzzer-no-link,address,undefined -fsanitize-address-use-after-scope")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=fuzzer-no-link,address,undefined -fsanitize-address-use-after-scope")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libasan -static-libubsan")
        endif ()
        set (LIBFUZZER_CMAKE_CXX_FLAGS "-fsanitize=fuzzer,address,undefined -fsanitize-address-use-after-scope")
    else ()
        message (FATAL_ERROR "Unknown sanitizer type: ${SANITIZE}")
    endif ()
endif()
