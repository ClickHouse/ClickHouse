include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_url .)
add_library(clickhouse_functions_url OBJECT ${clickhouse_functions_url_sources} ${clickhouse_functions_url_headers})
target_link_libraries(clickhouse_functions_url PRIVATE dbms)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_url PRIVATE "-g0")
endif()

# TODO: move Functions/Regexps.h to some lib and use here
if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions_url PRIVATE ch_contrib::vectorscan)
    list (APPEND PRIVATE_LIBS ch_contrib::vectorscan PARENT_SCOPE)
endif()

if (USE_GPERF)
    # Only for regenerate
    add_custom_target(generate-tldlookup-gperf ./tldLookup.sh
        SOURCES tldLookup.sh
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        BYPRODUCTS "${CMAKE_CURRENT_SOURCE_DIR}/tldLookup.gperf"
    )
    add_custom_target(generate-tldlookup ${GPERF} tldLookup.gperf --output-file=tldLookup.generated.cpp && clang-format -i tldLookup.generated.cpp
        SOURCES tldLookup.gperf
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        # BYPRODUCTS "${CMAKE_CURRENT_SOURCE_DIR}/tldLookup.generated.cpp"
    )
    add_dependencies(generate-tldlookup generate-tldlookup-gperf)
    if(NOT TARGET generate-source)
        add_custom_target(generate-source)
    endif()
    add_dependencies(generate-source generate-tldlookup)
endif ()
