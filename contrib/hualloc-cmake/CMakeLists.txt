add_library(hualloc "${ClickHouse_SOURCE_DIR}/contrib/hualloc/hu_alloc.cpp")
add_library(malloc ALIAS hualloc)
target_compile_definitions(hualloc PUBLIC USE_HUALLOC=1 USE_JEMALLOC=0)
