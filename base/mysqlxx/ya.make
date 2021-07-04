# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

OWNER(g:clickhouse)

CFLAGS(-g0)

PEERDIR(
    contrib/restricted/boost/libs
    contrib/libs/libmysql_r
    contrib/libs/poco/Foundation
    contrib/libs/poco/Util
)

ADDINCL(
    GLOBAL clickhouse/base
    clickhouse/base
    contrib/libs/libmysql_r
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    Connection.cpp
    Exception.cpp
    Pool.cpp
    PoolFactory.cpp
    PoolWithFailover.cpp
    Query.cpp
    ResultBase.cpp
    Row.cpp
    UseQueryResult.cpp
    Value.cpp

)

END()
