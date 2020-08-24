#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

namespace DB
{

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
        registerDictionarySourceExecutable(source_factory);
        registerDictionarySourceHTTP(source_factory);
        registerDictionarySourceLibrary(source_factory);
    }

    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
        registerDictionaryComplexKeyHashed(factory);
        registerDictionaryComplexKeyCache(factory);
        registerDictionaryComplexKeyDirect(factory);
#if !defined(ARCADIA_BUILD)
        registerDictionaryTrie(factory);
#endif
        registerDictionaryFlat(factory);
        registerDictionaryHashed(factory);
        registerDictionaryCache(factory);
#if defined(OS_LINUX) || defined(__FreeBSD__)
        registerDictionarySSDCache(factory);
        registerDictionarySSDComplexKeyCache(factory);
#endif
        registerDictionaryPolygon(factory);
        registerDictionaryDirect(factory);
    }
}

}
