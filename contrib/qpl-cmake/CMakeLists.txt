# The Intel® QPL provides high performance implementations of data processing functions for existing hardware accelerator, and/or software path in case if hardware accelerator is not available.

set (QPL_INCLUDE_DIRS
    "${ClickHouse_SOURCE_DIR}/contrib/qpl/include"
    "${ClickHouse_BINARY_DIR}/contrib/qpl/include"
)
set (LOG_HW_INIT ON)
set (EFFICIENT_WAIT ON)
set (LIB_FUZZING_ENGINE ON)

set (QPL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl")
set (QPL_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/qpl")
add_subdirectory ("${QPL_SOURCE_DIR}" "${QPL_BINARY_DIR}")

add_library (_qpl INTERFACE)
target_link_libraries (_qpl INTERFACE qpl)
target_include_directories (_qpl SYSTEM BEFORE INTERFACE ${QPL_INCLUDE_DIRS})
add_library(ch_contrib::qpl ALIAS _qpl)