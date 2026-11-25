option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
if (NOT ENABLE_POLYGLOT)
    message(STATUS "Not using polyglot")
    return()
endif()

clickhouse_add_rust_member(_ch_rust_polyglot FEATURE polyglot)
target_include_directories(_ch_rust_polyglot INTERFACE include)
add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)
