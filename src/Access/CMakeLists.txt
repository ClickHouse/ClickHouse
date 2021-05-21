include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_access .)
add_library(clickhouse_access ${clickhouse_access_headers} ${clickhouse_access_sources})
target_link_libraries(clickhouse_access PUBLIC clickhouse_common_io)

if (USE_LDAP)
    target_include_directories(clickhouse_access SYSTEM BEFORE PRIVATE ${OPENLDAP_INCLUDE_DIRS})
    target_link_libraries(clickhouse_access PRIVATE ${OPENLDAP_LIBRARIES})
endif ()

if (USE_CYRUS_SASL)
    target_link_libraries(clickhouse_access PRIVATE ${CYRUS_SASL_LIBRARY})
endif()

if (USE_KRB5)
    target_include_directories(clickhouse_access SYSTEM BEFORE PRIVATE ${KRB5_INCLUDE_DIR})
    target_link_libraries(clickhouse_access PRIVATE ${KRB5_LIBRARY})
endif()
