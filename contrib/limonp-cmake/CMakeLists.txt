set(LIMONP_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/limonp)

add_library(_limonp INTERFACE)
target_include_directories(_limonp SYSTEM INTERFACE ${LIMONP_LIBRARY_DIR}/include)
