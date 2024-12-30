#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

namespace DB
{

class DictionarySourceFactory;

void registerDictionarySourceNull(DictionarySourceFactory & factory);
void registerDictionarySourceFile(DictionarySourceFactory & source_factory);
void registerDictionarySourceMysql(DictionarySourceFactory & source_factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & source_factory);
void registerDictionarySourceMongoDB(DictionarySourceFactory & source_factory);
void registerDictionarySourceCassandra(DictionarySourceFactory & source_factory);
void registerDictionarySourceRedis(DictionarySourceFactory & source_factory);
void registerDictionarySourceXDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceJDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourcePostgreSQL(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutable(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutablePool(DictionarySourceFactory & source_factory);
void registerDictionarySourceHTTP(DictionarySourceFactory & source_factory);
void registerDictionarySourceLibrary(DictionarySourceFactory & source_factory);
void registerDictionarySourceYAMLRegExpTree(DictionarySourceFactory & source_factory);

class DictionaryFactory;
void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyHashed(DictionaryFactory & factory);
void registerDictionaryTrie(DictionaryFactory & factory);
void registerDictionaryFlat(DictionaryFactory & factory);
void registerDictionaryRegExpTree(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryArrayHashed(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory);
void registerDictionaryPolygon(DictionaryFactory & factory);
void registerDictionaryDirect(DictionaryFactory & factory);


void registerDictionaries()
{
    {
        auto & source_factory = DictionarySourceFactory::instance();
        registerDictionarySourceNull(source_factory);
        #if REGISTER_DICTIONARY_SOURCE_FILE
        registerDictionarySourceFile(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_MYSQL
        registerDictionarySourceMysql(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_CLICKHOUSE
        registerDictionarySourceClickHouse(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_MONGODB
        registerDictionarySourceMongoDB(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_REDIS
        registerDictionarySourceRedis(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_CASSANDRA
        registerDictionarySourceCassandra(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_XDBC
        registerDictionarySourceXDBC(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_JDBC
        registerDictionarySourceJDBC(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_POSTGRESQL
        registerDictionarySourcePostgreSQL(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_EXECUTABLE
        registerDictionarySourceExecutable(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_EXECUTABLEPOOL
        registerDictionarySourceExecutablePool(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_HTTP
        registerDictionarySourceHTTP(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_LIBRARY
        registerDictionarySourceLibrary(source_factory);
        #endif
        #if REGISTER_DICTIONARY_SOURCE_YAMLREGEXPTREE
        registerDictionarySourceYAMLRegExpTree(source_factory);
        #endif
    }

    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
        registerDictionaryTrie(factory);
        registerDictionaryFlat(factory);
        registerDictionaryRegExpTree(factory);
        registerDictionaryHashed(factory);
        registerDictionaryArrayHashed(factory);
        registerDictionaryCache(factory);
        registerDictionaryPolygon(factory);
        registerDictionaryDirect(factory);
    }
}

}
