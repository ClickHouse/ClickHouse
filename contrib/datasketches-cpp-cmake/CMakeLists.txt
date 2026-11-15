option (ENABLE_DATASKETCHES "Enable DataSketches" ${ENABLE_LIBRARIES})

if (NOT ENABLE_DATASKETCHES)
    message(STATUS "Not using DataSketches")
    return()
endif()

set(DATASKETCHES_LIBRARY theta)
add_library(_datasketches INTERFACE)
target_include_directories(_datasketches SYSTEM BEFORE INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/common/include"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/count/include"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/theta/include")

add_library(ch_contrib::datasketches ALIAS _datasketches)
