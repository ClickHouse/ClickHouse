if (NOT HAVE_AES)
    return()
endif ()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aquahash")

add_library(aquahash ${LIBRARY_DIR}/aquahash.h)
target_include_directories(aquahash SYSTEM PUBLIC ${LIBRARY_DIR})
target_compile_options(aquahash PUBLIC "-maes")
add_library(ch_contrib::aquahash ALIAS aquahash)
