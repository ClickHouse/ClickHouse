option (ENABLE_PCO "Enable pco (pcodec) compression codec" ${ENABLE_LIBRARIES})
if (NOT ENABLE_PCO)
    message(STATUS "Not using pco")
    return()
endif()

clickhouse_add_rust_member(_ch_rust_pco FEATURE pco)
target_include_directories(_ch_rust_pco INTERFACE include)
add_library(ch_rust::pco ALIAS _ch_rust_pco)
