# Possible values:
# - `address` (ASan)
# - `memory` (MSan)
# - `thread` (TSan)
# - `undefined` (UBSan)
# - "" (no sanitizing)
option (SANITIZE "Enable one of the code sanitizers" "")

set (SAN_FLAGS "${SAN_FLAGS} -g -fno-omit-frame-pointer -DSANITIZER")

if (SANITIZE)
    if (SANITIZE STREQUAL "address")
        set (ASAN_FLAGS "-fsanitize=address -fsanitize-address-use-after-scope")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${ASAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${ASAN_FLAGS}")

    elseif (SANITIZE STREQUAL "memory")
        # MemorySanitizer flags are set according to the official documentation:
        # https://clang.llvm.org/docs/MemorySanitizer.html#usage

        # Linking can fail due to relocation overflows (see #49145), caused by too big object files / libraries.
        # Work around this with position-independent builds (-fPIC and -fpie), this is slightly slower than non-PIC/PIE but that's okay.
        set (MSAN_FLAGS "-fsanitize=memory -fsanitize-memory-use-after-dtor -fsanitize-memory-track-origins -fno-optimize-sibling-calls -fPIC -fpie")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")

    elseif (SANITIZE STREQUAL "thread")
        set (TSAN_FLAGS "-fsanitize=thread")
        set (TSAN_FLAGS "${TSAN_FLAGS} -fsanitize-ignorelist=${PROJECT_SOURCE_DIR}/tests/tsan_ignorelist.txt")

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${TSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${TSAN_FLAGS}")

    elseif (SANITIZE STREQUAL "undefined")
        set (UBSAN_FLAGS "-fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero")
        if (ENABLE_FUZZING)
            # Unsigned integer overflow is well defined behaviour from a perspective of C++ standard,
            # compilers or CPU. We use in hash functions like SipHash and many other places in our codebase.
            # This flag is needed only because fuzzers are run inside oss-fuzz infrastructure
            # and they have a bunch of flags not halt the program if UIO happend and even to silence that warnings.
            # But for unknown reason that flags don't work with ClickHouse or we don't understand how to properly use them,
            # that's why we often receive reports about UIO. The simplest way to avoid this is just  set this flag here.
            set(UBSAN_FLAGS "${UBSAN_FLAGS} -fno-sanitize=unsigned-integer-overflow")
        endif()
        set (UBSAN_FLAGS "${UBSAN_FLAGS} -fsanitize-ignorelist=${PROJECT_SOURCE_DIR}/tests/ubsan_ignorelist.txt")

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${UBSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${UBSAN_FLAGS}")

    else ()
        message (FATAL_ERROR "Unknown sanitizer type: ${SANITIZE}")
    endif ()
endif()

# Default coverage instrumentation (dumping the coverage map on exit)
option(WITH_COVERAGE "Instrumentation for code coverage with default implementation" OFF)

if (WITH_COVERAGE)
    message (STATUS "Enabled instrumentation for code coverage")
    set (COVERAGE_FLAGS -fprofile-instr-generate -fcoverage-mapping)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif()

option (SANITIZE_COVERAGE "Instrumentation for code coverage with custom callbacks" OFF)

if (SANITIZE_COVERAGE)
    message (STATUS "Enabled instrumentation for code coverage")

    # We set this define for whole build to indicate that at least some parts are compiled with coverage.
    # And to expose it in system.build_options.
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSANITIZE_COVERAGE=1")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSANITIZE_COVERAGE=1")

    # But the actual coverage will be enabled on per-library basis: for ClickHouse code, but not for 3rd-party.
    set (COVERAGE_FLAGS "-fsanitize-coverage=trace-pc-guard,pc-table")

    set (WITHOUT_COVERAGE_FLAGS "-fno-profile-instr-generate -fno-coverage-mapping -fno-sanitize-coverage=trace-pc-guard,pc-table")
    set (WITHOUT_COVERAGE_FLAGS_LIST -fno-profile-instr-generate -fno-coverage-mapping -fno-sanitize-coverage=trace-pc-guard,pc-table)
else()
    set (WITHOUT_COVERAGE_FLAGS "")
    set (WITHOUT_COVERAGE_FLAGS_LIST "")
endif()
