#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

#include "ClickHouseDictionarySource.h"
//#include "DictionaryFactory.h"
//#include "DictionaryStructure.h"
#include "ExecutableDictionarySource.h"
#include "FileDictionarySource.h"
#include "HTTPDictionarySource.h"
#include "LibraryDictionarySource.h"
#include "XDBCDictionarySource.h"
#include "MongoDBDictionarySource.h"
#include "MySQLDictionarySource.h"

#include "RangeHashedDictionary.h"

namespace DB
{

void registerDictionaries()
{
    {

        /*
DB::DictionarySourcePtr CreateTableDictionarySource(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string& tableName,
    const DB::Block& sampleBlock)
{
    return std::make_unique<TTableDictionarySource>(
        std::move(storage),
        std::move(authToken),
        tableName,
        sampleBlock.getNamesAndTypesList());
}

void RegisterTableDictionarySource(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken)
{
    auto createTableSource = [=] (
        const DB::DictionaryStructure& dictStructure,
        const Poco::Util::AbstractConfiguration& config,
        const std::string& dictSectionPath,
        DB::Block& sampleBlock,
        const DB::Context& context) -> DB::DictionarySourcePtr
    {
        const auto tableName = config.getString(dictSectionPath + ".yt_table.path");
        return CreateTableDictionarySource(storage, authToken, tableName, sampleBlock);
    };

    DB::DictionarySourceFactory::instance().registerSource("yt_table", createTableSource);
}
*/

        auto & factory = DictionarySourceFactory::instance();
        registerDictionarySourceFile(factory);
        registerDictionarySourceMysql(factory);
        registerDictionarySourceClickHouse(factory);
        registerDictionarySourceMongoDB(factory);
        registerDictionarySourceXDBC(factory);
        registerDictionarySourceJDBC(factory);
        registerDictionarySourceExecutable(factory);
        registerDictionarySourceHTTP(factory);
        registerDictionarySourceLibrary(factory);
    }
    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
    }

}

}
