# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Data
    contrib/libs/poco/Data/ODBC
    contrib/libs/poco/MongoDB
    contrib/libs/poco/Redis
    contrib/libs/sparsehash
)

NO_COMPILER_WARNINGS()


SRCS(
    CacheDictionary.cpp
    CacheDictionary_generate1.cpp
    CacheDictionary_generate2.cpp
    CacheDictionary_generate3.cpp
    CassandraBlockInputStream.cpp
    CassandraDictionarySource.cpp
    CassandraHelpers.cpp
    ClickHouseDictionarySource.cpp
    ComplexKeyCacheDictionary.cpp
    ComplexKeyCacheDictionary_createAttributeWithType.cpp
    ComplexKeyCacheDictionary_generate1.cpp
    ComplexKeyCacheDictionary_generate2.cpp
    ComplexKeyCacheDictionary_generate3.cpp
    ComplexKeyCacheDictionary_setAttributeValue.cpp
    ComplexKeyCacheDictionary_setDefaultAttributeValue.cpp
    ComplexKeyDirectDictionary.cpp
    ComplexKeyHashedDictionary.cpp
    DictionaryBlockInputStreamBase.cpp
    DictionaryFactory.cpp
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
    ExternalQueryBuilder.cpp
    FileDictionarySource.cpp
    FlatDictionary.cpp
    HTTPDictionarySource.cpp
    HashedDictionary.cpp
    IPAddressDictionary.cpp
    LibraryDictionarySource.cpp
    LibraryDictionarySourceExternal.cpp
    MongoDBDictionarySource.cpp
    MySQLDictionarySource.cpp
    PolygonDictionary.cpp
    PolygonDictionaryImplementations.cpp
    PolygonDictionaryUtils.cpp
    RangeHashedDictionary.cpp
    RedisBlockInputStream.cpp
    RedisDictionarySource.cpp
    SSDCacheDictionary.cpp
    SSDComplexKeyCacheDictionary.cpp
    XDBCDictionarySource.cpp
    getDictionaryConfigurationFromAST.cpp
    readInvalidateQuery.cpp
    registerDictionaries.cpp
    writeParenthesisedString.cpp

)

END()
