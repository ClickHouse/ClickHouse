option(ENABLE_FSST "Enable FSST (Fast Static Symbol Table)" ${ENABLE_LIBRARIES})

if (NOT ENABLE_FSST)
    message (STATUS "Not using FSST (Fast Static Symbol Table)")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/fsst")
set(SRCS
    "${LIBRARY_DIR}/libfsst.cpp"
    "${LIBRARY_DIR}/fsst_avx512.cpp"
    "${LIBRARY_DIR}/libfsst12.cpp"
)

add_library(_fsst ${SRCS})
add_library(ch_contrib::fsst ALIAS _fsst)

target_include_directories(_fsst PUBLIC "${LIBRARY_DIR}")
target_compile_definitions(_fsst INTERFACE ENABLE_FSST)