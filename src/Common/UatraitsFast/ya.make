LIBRARY()

OWNER(g:metrika-core)

PEERDIR(
    library/cpp/on_disk/aho_corasick
    contrib/libs/clickhouse/libs/libcommon
    metrika/core/libs/appmetrica/types
    metrika/core/libs/metrikacommon
)

ADDINCL(
    contrib/libs/clickhouse
    contrib/libs/clickhouse/dbms/src
    contrib/libs/clickhouse/libs/libcommon/include
    contrib/libs/poco/Foundation/include
    contrib/libs/poco/Util/include
    contrib/libs/poco/XML/include
    metrika/core/tools
)

SRCS(
    uatraits-fast.cpp
    UserAgent.cpp
)

END()
