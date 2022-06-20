set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/farsh")

set(SRCS
        "${LIBRARY_DIR}/farsh.h"
        "${LIBRARY_DIR}/farsh.c"
        )

add_library(farsh ${SRCS})
target_include_directories (farsh PUBLIC ${LIBRARY_DIR})

if(HAVE_AVX2)
    target_compile_options(farsh PUBLIC -mavx2 -DFARSH_AVX2)
elseif(HAVE_SSSE3)
    target_compile_options(farsh PUBLIC -msse2 -DFARSH_SSE2)
endif()

add_library(ch_contrib::farsh ALIAS farsh)