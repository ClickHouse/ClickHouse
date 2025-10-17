set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/xxHash")
set (SRCS
    "${LIBRARY_DIR}/xxhash.c"
)

add_library(xxHash ${SRCS})
target_include_directories(xxHash SYSTEM BEFORE INTERFACE "${LIBRARY_DIR}")

# XXH_INLINE_ALL - Make all functions inline, with implementations being directly included within xxhash.h. Inlining functions is beneficial for speed on small keys.
# https://github.com/Cyan4973/xxHash/tree/v0.8.2#build-modifiers
target_compile_definitions(xxHash PUBLIC XXH_INLINE_ALL)

add_library(ch_contrib::xxHash ALIAS xxHash)
