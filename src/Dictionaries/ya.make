# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Data
    contrib/libs/poco/MongoDB
    contrib/libs/poco/Redis
    contrib/libs/sparsehash
    contrib/restricted/abseil-cpp
)

IF (USE_ODBC)
    PEERDIR(contrib/libs/poco/Data/ODBC)
ENDIF ()

NO_COMPILER_WARNINGS()


SRCS(
    CacheDictionary.cpp
    CacheDictionaryUpdateQueue.cpp
    CassandraDictionarySource.cpp
    CassandraHelpers.cpp
    CassandraSource.cpp
    ClickHouseDictionarySource.cpp
    DictionaryFactory.cpp
    DictionaryHelpers.cpp
    DictionarySource.cpp
    DictionarySourceBase.cpp
    DictionarySourceFactory.cpp
    DictionarySourceHelpers.cpp
    DictionaryStructure.cpp
    DirectDictionary.cpp
    Embedded/GeoDictionariesLoader.cpp
    Embedded/GeodataProviders/HierarchiesProvider.cpp
    Embedded/GeodataProviders/HierarchyFormatReader.cpp
    Embedded/GeodataProviders/NamesFormatReader.cpp
    Embedded/GeodataProviders/NamesProvider.cpp
    Embedded/RegionsHierarchies.cpp
    Embedded/RegionsHierarchy.cpp
    Embedded/RegionsNames.cpp
    ExecutableDictionarySource.cpp
    ExecutablePoolDictionarySource.cpp
    ExternalQueryBuilder.cpp
    FileDictionarySource.cpp
    FlatDictionary.cpp
    HTTPDictionarySource.cpp
    HashedDictionary.cpp
    HierarchyDictionariesUtils.cpp
    IPAddressDictionary.cpp
    LibraryDictionarySource.cpp
    MongoDBDictionarySource.cpp
    MySQLDictionarySource.cpp
    PolygonDictionary.cpp
    PolygonDictionaryImplementations.cpp
    PolygonDictionaryUtils.cpp
    RangeHashedDictionary.cpp
    RedisDictionarySource.cpp
    RedisSource.cpp
    XDBCDictionarySource.cpp
    getDictionaryConfigurationFromAST.cpp
    readInvalidateQuery.cpp
    registerCacheDictionaries.cpp
    registerDictionaries.cpp
    writeParenthesisedString.cpp

)

END()
