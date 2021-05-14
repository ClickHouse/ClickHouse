#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

namespace DB
{

class DictionarySourceFactory;

void registerDictionarySourceFile(DictionarySourceFactory & source_factory);
void registerDictionarySourceMysql(DictionarySourceFactory & source_factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & source_factory);
void registerDictionarySourceMongoDB(DictionarySourceFactory & source_factory);
void registerDictionarySourceCassandra(DictionarySourceFactory & source_factory);
void registerDictionarySourceRedis(DictionarySourceFactory & source_factory);
void registerDictionarySourceXDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceJDBC(DictionarySourceFactory & source_factory);
#if !defined(ARCADIA_BUILD)
void registerDictionarySourcePostgreSQL(DictionarySourceFactory & source_factory);
#endif
void registerDictionarySourceExecutable(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutablePool(DictionarySourceFactory & source_factory);
void registerDictionarySourceHTTP(DictionarySourceFactory & source_factory);
void registerDictionarySourceLibrary(DictionarySourceFactory & source_factory);

class DictionaryFactory;
void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyHashed(DictionaryFactory & factory);
void registerDictionaryTrie(DictionaryFactory & factory);
void registerDictionaryFlat(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory);
void registerDictionaryPolygon(DictionaryFactory & factory);
void registerDictionaryDirect(DictionaryFactory & factory);


void registerDictionaries()
{
    {
        auto & source_factory = DictionarySourceFactory::instance();
        registerDictionarySourceFile(source_factory);
        registerDictionarySourceMysql(source_factory);
        registerDictionarySourceClickHouse(source_factory);
        registerDictionarySourceMongoDB(source_factory);
        registerDictionarySourceRedis(source_factory);
        registerDictionarySourceCassandra(source_factory);
        registerDictionarySourceXDBC(source_factory);
        registerDictionarySourceJDBC(source_factory);
#if !defined(ARCADIA_BUILD)
        registerDictionarySourcePostgreSQL(source_factory);
#endif
        registerDictionarySourceExecutable(source_factory);
        registerDictionarySourceExecutablePool(source_factory);
        registerDictionarySourceHTTP(source_factory);
        registerDictionarySourceLibrary(source_factory);
    }

    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
        registerDictionaryTrie(factory);
        registerDictionaryFlat(factory);
        registerDictionaryHashed(factory);
        registerDictionaryCache(factory);
        registerDictionaryPolygon(factory);
        registerDictionaryDirect(factory);
    }
}

}
