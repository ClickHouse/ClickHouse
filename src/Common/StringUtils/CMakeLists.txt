# These files are located in separate library, because they are used by Yandex.Metrika code
#  in places when no dependency on whole "dbms" library is possible.

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_stringutils .)

add_library(string_utils ${clickhouse_common_stringutils_headers} ${clickhouse_common_stringutils_sources})
