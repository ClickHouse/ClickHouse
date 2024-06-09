# Default coverage instrumentation (dumping the coverage map on exit)
option(WITH_COVERAGE "Instrumentation for code coverage with default implementation" OFF)

if (WITH_COVERAGE)
    message (STATUS "Enabled instrumentation for code coverage")

    set(COVERAGE_FLAGS "SHELL:-fprofile-instr-generate -fcoverage-mapping") # SHELL: disables splitup/de-duplication of compiler arguments
endif()

option(SANITIZE_COVERAGE "Instrumentation for code coverage with custom callbacks" OFF)

if (SANITIZE_COVERAGE)
    message (STATUS "Enabled instrumentation for code coverage")

    # We set this define for whole build to indicate that at least some parts are compiled with coverage.
    # And to expose it in system.build_options.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSANITIZE_COVERAGE=1")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSANITIZE_COVERAGE=1")

    # But the actual coverage will be enabled on per-library basis: for ClickHouse code, but not for 3rd-party.
    set(COVERAGE_FLAGS "-fsanitize-coverage=trace-pc-guard,pc-table")

    set(WITHOUT_COVERAGE_FLAGS "-fno-profile-instr-generate -fno-coverage-mapping -fno-sanitize-coverage=trace-pc-guard,pc-table")
else()
    set(WITHOUT_COVERAGE_FLAGS "")
endif()
