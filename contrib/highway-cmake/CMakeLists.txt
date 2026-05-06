set(HIGHWAY_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/highway")

# Core highway sources (no tests, no examples, no nanobenchmark).
set(HIGHWAY_SOURCES
    "${HIGHWAY_SOURCE_DIR}/hwy/abort.cc"
    "${HIGHWAY_SOURCE_DIR}/hwy/aligned_allocator.cc"
    "${HIGHWAY_SOURCE_DIR}/hwy/per_target.cc"
    "${HIGHWAY_SOURCE_DIR}/hwy/targets.cc"
    "${HIGHWAY_SOURCE_DIR}/hwy/timer.cc"
)

add_library(_highway ${HIGHWAY_SOURCES})

target_include_directories(_highway SYSTEM PUBLIC "${HIGHWAY_SOURCE_DIR}")

# Highway uses runtime multi-target dispatch and manages its own SIMD baseline
# detection — do not add explicit -mavx2 or similar flags here.
target_compile_options(_highway PRIVATE -w)

add_library(ch_contrib::highway ALIAS _highway)
