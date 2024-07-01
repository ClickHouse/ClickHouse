
include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources (clickhouse_rapidgzip .)

add_library(clickhouse_rapidgzip ${clickhouse_rapidgzip_sources})

target_include_directories(clickhouse_rapidgzip PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(clickhouse_rapidgzip PRIVATE clickhouse_common_io)


target_compile_options(clickhouse_rapidgzip PRIVATE
    "-Wno-comma"
    "-Wno-documentation"
    "-Wno-extra-semi-stmt"
    "-Wno-extra-semi"
    "-Wno-frame-larger-than"
    "-Wno-inconsistent-missing-destructor-override"
    "-Wno-old-style-cast"
    "-Wno-reserved-identifier"
    "-Wno-shadow"
    "-Wno-shorten-64-to-32"
    "-Wno-suggest-destructor-override"
    "-Wno-tautological-type-limit-compare"
    "-Wno-unused-exception-parameter"
    "-Wno-unused-parameter"
)

set_source_files_properties(${clickhouse_rapidgzip_sources} PROPERTIES COMPILE_FLAGS -fconstexpr-steps=99000100)

target_link_libraries (clickhouse_rapidgzip PRIVATE ch_contrib::rapidgzip)
