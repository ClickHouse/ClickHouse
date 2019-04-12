set(LFALLOC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/lfalloc/src)

set(SRCS
${LFALLOC_SOURCE_DIR}/malloc.cpp
${LFALLOC_SOURCE_DIR}/lf_allocX64.cpp
)

add_library(lfalloc STATIC ${SRCS})

target_include_directories(lfalloc PRIVATE
    ${LFALLOC_SOURCE_DIR}/src)
