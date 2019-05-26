#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

namespace DB
{
void registerDictionarySourceFile(DictionarySourceFactory & source_factory);
void registerDictionarySourceMysql(DictionarySourceFactory & source_factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & source_factory);
void registerDictionarySourceMongoDB(DictionarySourceFactory & source_factory);
void registerDictionarySourceXDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceJDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutable(DictionarySourceFactory & source_factory);
void registerDictionarySourceHTTP(DictionarySourceFactory & source_factory);
void registerDictionarySourceLibrary(DictionarySourceFactory & source_factory);

void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyCache(DictionaryFactory & factory);
void registerDictionaryTrie(DictionaryFactory & factory);
void registerDictionaryFlat(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory);


void registerDictionaries()
{
    {
        auto & source_factory = DictionarySourceFactory::instance();
        registerDictionarySourceFile(source_factory);
        registerDictionarySourceMysql(source_factory);
        registerDictionarySourceClickHouse(source_factory);
        registerDictionarySourceMongoDB(source_factory);
        registerDictionarySourceXDBC(source_factory);
        registerDictionarySourceJDBC(source_factory);
        registerDictionarySourceExecutable(source_factory);
        registerDictionarySourceHTTP(source_factory);
        registerDictionarySourceLibrary(source_factory);
    }

    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
        registerDictionaryComplexKeyHashed(factory);
        registerDictionaryComplexKeyCache(factory);
        registerDictionaryTrie(factory);
        registerDictionaryFlat(factory);
        registerDictionaryHashed(factory);
        registerDictionaryCache(factory);
    }
}

}
