LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/poco/MongoDB
    contrib/libs/poco/Redis
)

SRCS(
    ClickHouseDictionarySource.cpp
    DictionarySourceFactory.cpp
    FileDictionarySource.cpp
    MongoDBDictionarySource.cpp
    MySQLDictionarySource.cpp
    RedisDictionarySource.cpp
    registerDictionaries.cpp
    XDBCDictionarySource.cpp
)

END()
