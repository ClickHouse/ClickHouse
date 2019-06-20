include (${ClickHouse_SOURCE_DIR}/cmake/find_gperf.cmake)
configure_file(config_functions_url.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config_functions_url.h)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_functions_url .)

if (USE_GPERF)
    add_custom_command(
	    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tldLookup.cpp
	    COMMAND ${GPERF} ${CMAKE_CURRENT_SOURCE_DIR}/tldLookup.gperf --output-file=${CMAKE_CURRENT_BINARY_DIR}/tldLookup.cpp
    )
    list(APPEND clickhouse_functions_url_sources ${CMAKE_CURRENT_BINARY_DIR}/tldLookup.cpp)
endif ()

add_library(clickhouse_functions_url ${clickhouse_functions_url_sources} ${clickhouse_functions_url_headers})

target_link_libraries(clickhouse_functions_url PRIVATE clickhouse_common_io)
#target_include_directories(clickhouse_functions_url PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories(clickhouse_functions_url PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include ${CMAKE_CURRENT_BINARY_DIR}/../include)

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions_url PRIVATE "-g0")
endif ()

# TODO: move Functions/Regexps.h to some lib and use here
if(USE_HYPERSCAN)
    target_link_libraries(clickhouse_functions_url PRIVATE ${HYPERSCAN_LIBRARY})
    target_include_directories(clickhouse_functions_url SYSTEM PRIVATE ${HYPERSCAN_INCLUDE_DIR})
endif()
