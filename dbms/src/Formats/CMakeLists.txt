include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_formats .)
add_library(clickhouse_formats ${LINK_MODE} ${clickhouse_formats_sources})
target_link_libraries(clickhouse_formats PRIVATE clickhouse_parsers clickhouse_common_io pocoext)

if(USE_CAPNP)
    target_link_libraries(clickhouse_formats PRIVATE ${CAPNP_LIBRARY})
    if(NOT USE_INTERNAL_CAPNP_LIBRARY)
        target_include_directories(clickhouse_formats SYSTEM BEFORE PRIVATE ${CAPNP_INCLUDE_DIR})
    endif()
endif()

if(USE_ICU)
    target_link_libraries(clickhouse_formats PRIVATE ${ICU_LIBRARIES})
endif()

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
