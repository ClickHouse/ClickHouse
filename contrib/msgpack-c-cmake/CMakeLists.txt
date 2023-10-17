option (ENABLE_MSGPACK "Enable msgpack library" ${ENABLE_LIBRARIES})

if(NOT ENABLE_MSGPACK)
    message(STATUS "Not using msgpack")
    return()
endif()

add_library(_msgpack INTERFACE)
target_include_directories(_msgpack SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include")
add_library(ch_contrib::msgpack ALIAS _msgpack)
