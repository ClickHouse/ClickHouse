# 3.18+ only
find_program(GEN_HTML genhtml REQUIRED)


# If this file was included, then the code coverage runtime is active
set(WITH_COVERAGE_FLAGS "-fsanitize-coverage=trace-pc-guard")
set(WITHOUT_COVERAGE "-fno-sanitize-coverage=trace-pc-guard")

# Needed for the symbolizer
set(SANITIZE "address")
set(ENABLE_JEMALLOC OFF)
set(ENABLE_THINLTO OFF)

# Needed for the clang coverage runtime
# ASAN_OPTIONS=coverage=1
