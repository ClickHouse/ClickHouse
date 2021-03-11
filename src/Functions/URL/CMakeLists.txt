include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions_url .)
add_library(clickhouse_functions_url ${clickhouse_functions_url_sources} ${clickhouse_functions_url_headers})
target_link_libraries(clickhouse_functions_url PRIVATE dbms)

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions_url PRIVATE "-g0")
endif ()

# TODO: move Functions/Regexps.h to some lib and use here
target_link_libraries(clickhouse_functions_url PRIVATE hyperscan)

if (USE_GPERF)
    # Only for regenerate
    add_custom_target(generate-tldlookup-gperf ./tldLookup.sh
        SOURCES tldLookup.sh
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        BYPRODUCTS ${CMAKE_CURRENT_SOURCE_DIR}/tldLookup.gperf
    )
    add_custom_target(generate-tldlookup ${GPERF} tldLookup.gperf --output-file=tldLookup.generated.cpp && clang-format -i tldLookup.generated.cpp
        SOURCES tldLookup.gperf
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        # BYPRODUCTS ${CMAKE_CURRENT_SOURCE_DIR}/tldLookup.generated.cpp
    )
    add_dependencies(generate-tldlookup generate-tldlookup-gperf)
    if(NOT TARGET generate-source)
        add_custom_target(generate-source)
    endif()
    add_dependencies(generate-source generate-tldlookup)
endif ()
