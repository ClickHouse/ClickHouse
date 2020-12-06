# Possible values: `address` (ASan), `memory` (MSan), `thread` (TSan), `undefined` (UBSan), and "" (no sanitizing)
option (SANITIZE "Enable one of the code sanitizers" "")

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
        set (MSAN_FLAGS "-fsanitize=memory -fsanitize-memory-track-origins -fno-optimize-sibling-calls -fsanitize-blacklist=${CMAKE_SOURCE_DIR}/tests/msan_suppressions.txt")

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")

        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libmsan")
        endif ()

    elseif (SANITIZE STREQUAL "thread")
        set (TSAN_FLAGS "-fsanitize=thread")
        if (COMPILER_CLANG)
            set (TSAN_FLAGS "${TSAN_FLAGS} -fsanitize-blacklist=${CMAKE_SOURCE_DIR}/tests/tsan_suppressions.txt")
        else()
            message (WARNING "TSAN suppressions was not passed to the compiler (since the compiler is not clang)")
            message (WARNING "Use the following command to pass them manually:")
            message (WARNING "    export TSAN_OPTIONS=\"$TSAN_OPTIONS suppressions=${CMAKE_SOURCE_DIR}/tests/tsan_suppressions.txt\"")
        endif()


        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${TSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${TSAN_FLAGS}")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libtsan")
        endif ()

    elseif (SANITIZE STREQUAL "undefined")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero -fsanitize-blacklist=${CMAKE_SOURCE_DIR}/tests/ubsan_suppressions.txt")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero -fsanitize-blacklist=${CMAKE_SOURCE_DIR}/tests/ubsan_suppressions.txt")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
        endif()
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libubsan")
        endif ()

        # llvm-tblgen, that is used during LLVM build, doesn't work with UBSan.
        set (ENABLE_EMBEDDED_COMPILER 0 CACHE BOOL "")

    else ()
        message (FATAL_ERROR "Unknown sanitizer type: ${SANITIZE}")
    endif ()
endif()
