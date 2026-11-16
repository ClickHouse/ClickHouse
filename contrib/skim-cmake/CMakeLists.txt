option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})
if (NOT ENABLE_SKIM)
    message(STATUS "Not using skim")
    return()
endif()

clickhouse_add_rust_member(_ch_rust_skim_rust FEATURE skim)
target_include_directories(_ch_rust_skim_rust INTERFACE include)
add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
