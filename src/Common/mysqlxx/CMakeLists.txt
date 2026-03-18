add_library (mysqlxx
    Connection.cpp
    Exception.cpp
    Query.cpp
    ResultBase.cpp
    UseQueryResult.cpp
    Row.cpp
    Value.cpp
    Pool.cpp
    PoolFactory.cpp
    PoolWithFailover.cpp
)

target_include_directories (mysqlxx PUBLIC .)

target_link_libraries (mysqlxx
    clickhouse_common_io
    ch_contrib::zlib
    ch_contrib::mariadbclient
)

add_library(ch::mysqlxx ALIAS mysqlxx)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
