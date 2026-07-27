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
        set (MSAN_FLAGS "-fsanitize=memory -fsanitize-memory-use-after-dtor -fsanitize-memory-track-origins -fPIC -fpie")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${MSAN_FLAGS}")

        # NOTE: See also libcxxabi cmake rules
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

    elseif (SANITIZE STREQUAL "address,undefined")
        set (ASAN_UBSAN_FLAGS "-fsanitize=address,undefined -fsanitize-address-use-after-scope -fno-sanitize-recover=all -fno-sanitize=float-divide-by-zero")
        if (ENABLE_FUZZING)
            set (ASAN_UBSAN_FLAGS "${ASAN_UBSAN_FLAGS} -fno-sanitize=unsigned-integer-overflow")
        endif()
        set (ASAN_UBSAN_FLAGS "${ASAN_UBSAN_FLAGS} -fsanitize-ignorelist=${PROJECT_SOURCE_DIR}/tests/ubsan_ignorelist.txt")

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} ${ASAN_UBSAN_FLAGS}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} ${ASAN_UBSAN_FLAGS}")

    else ()
        message (FATAL_ERROR "Unknown sanitizer type: ${SANITIZE}")
    endif ()
endif()

# Default coverage instrumentation (dumping the coverage map on exit)
option(WITH_COVERAGE "Instrumentation for code coverage with default implementation" OFF)

option(WITH_COVERAGE_DEPTH "Shadow call-stack depth tracking via -finstrument-functions-after-inlining (requires WITH_COVERAGE)" OFF)

option(WITH_COVERAGE_XRAY
    "Use XRay instrumentation for exact call-depth tracking (requires WITH_COVERAGE and ENABLE_XRAY). Builds with -DCLICKHOUSE_XRAY_INSTRUMENT_COVERAGE=1. XRay maps runtime function text addresses to LLVM profile records, solving the PIE FunctionPointer=0 limitation."
    OFF)

if (WITH_COVERAGE)
    message (STATUS "Enabled instrumentation for code coverage")

    # But the actual coverage will be enabled on per-library basis: for ClickHouse code, but not for 3rd-party.
    set (COVERAGE_FLAGS -fprofile-instr-generate -fcoverage-mapping)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")

    if (WITH_COVERAGE_DEPTH)
        # WITH_COVERAGE_DEPTH enables per-test collection (CoverageCollection.cpp,
        # LLVMCoverageMapping.cpp, SYSTEM SET COVERAGE TEST).  All per-test-specific
        # flags are gated here — not in the base WITH_COVERAGE block — so that the
        # regular amd_llvm_coverage build (used for HTML coverage reports) is unchanged.
        message (STATUS "Enabled per-test coverage instrumentation (WITH_COVERAGE_DEPTH)")
        # Expose WITH_COVERAGE=1 as a C++ macro so that CoverageCollection.cpp,
        # LLVMCoverageMapping.cpp, and SYSTEM SET COVERAGE TEST are compiled in.
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITH_COVERAGE=1")
        set (CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -DWITH_COVERAGE=1")
        message (STATUS "Enabled call-depth shadow stack via -finstrument-functions-after-inlining")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -finstrument-functions-after-inlining")
        set (CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -finstrument-functions-after-inlining")
        # -mllvm -enable-value-profiling=true activates indirect-call value profiling so that
        # __llvm_profile_instrument_target() records virtual-dispatch targets at runtime.
        # Without this flag, LLVMProfileData::Values is always NULL and indirect-call data
        # cannot be collected. Only enabled for per-test coverage builds to avoid changing
        # the regular amd_llvm_coverage build behaviour.
        set (COVERAGE_FLAGS ${COVERAGE_FLAGS} -mllvm -enable-value-profiling=true)
    endif()

    if (WITH_COVERAGE_XRAY)
        message (STATUS "Enabled XRay-based exact call-depth tracking for coverage")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCLICKHOUSE_XRAY_INSTRUMENT_COVERAGE=1")
        set (CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -DCLICKHOUSE_XRAY_INSTRUMENT_COVERAGE=1")
    endif()

    set (WITHOUT_COVERAGE_FLAGS "-fno-profile-instr-generate -fno-coverage-mapping")
    set (WITHOUT_COVERAGE_FLAGS_LIST -fno-profile-instr-generate -fno-coverage-mapping)
else()
    set (WITHOUT_COVERAGE_FLAGS "")
    set (WITHOUT_COVERAGE_FLAGS_LIST "")
endif()

# Use our bundled compiler-rt headers (sanitizer/ and xray/ interfaces) instead of the ones
# from the compiler's resource directory. This avoids depending on the host compiler's headers:
# for example, sanitizer builds need <sanitizer/asan_interface.h> etc., but XRay is disabled
# for sanitizer builds, so those headers would otherwise come from the system compiler.
#
# The compiler searches -isystem paths before its implicit resource directory, so putting our
# bundled path here ensures it takes precedence without disrupting #include_next chains (which
# libcxx relies on to reach the compiler's own stddef.h, stdarg.h, etc.).
include_directories (SYSTEM "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/include")
