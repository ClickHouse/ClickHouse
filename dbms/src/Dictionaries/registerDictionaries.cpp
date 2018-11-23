#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

#include "ClickHouseDictionarySource.h"
#include "ExecutableDictionarySource.h"
#include "FileDictionarySource.h"
#include "HTTPDictionarySource.h"
#include "LibraryDictionarySource.h"
#include "XDBCDictionarySource.h"
#include "MongoDBDictionarySource.h"
#include "MySQLDictionarySource.h"

#include "RangeHashedDictionary.h"
#include "ComplexKeyHashedDictionary.h"
#include "ComplexKeyCacheDictionary.h"
#include "TrieDictionary.h"
#include "FlatDictionary.h"
#include "HashedDictionary.h"
#include "CacheDictionary.h"

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
