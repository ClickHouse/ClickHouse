SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/highwayhash")

add_library(highwayhash "${LIBRARY_DIR}/highwayhash/highwayhash.h"
                        "${LIBRARY_DIR}/highwayhash/arch_specific.h"
                        "${LIBRARY_DIR}/highwayhash/arch_specific.cc"
                        "${LIBRARY_DIR}/highwayhash/compiler_specific.h"
        )

if (HAVE_AVX2)
    target_compile_options(highwayhash PUBLIC -mavx2)
elseif(HAVE_SSE41)
    target_compile_options(highwayhash PUBLIC -msse4.1)
endif()

target_include_directories(highwayhash PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::highwayhash ALIAS highwayhash)
