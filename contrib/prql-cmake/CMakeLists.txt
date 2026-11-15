option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
if (NOT ENABLE_PRQL)
    message(STATUS "Not using prql")
    return()
endif()

clickhouse_add_rust_member(_ch_rust_prql FEATURE prql)
target_include_directories(_ch_rust_prql INTERFACE include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)
