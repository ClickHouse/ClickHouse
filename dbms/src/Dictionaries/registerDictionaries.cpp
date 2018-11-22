#include "DictionaryFactory.h"

#include "ClickHouseDictionarySource.h"
#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "ExecutableDictionarySource.h"
#include "FileDictionarySource.h"
#include "HTTPDictionarySource.h"
#include "LibraryDictionarySource.h"
#include "XDBCDictionarySource.h"

#include <Common/config.h>
#if USE_POCO_MONGODB
#include "MongoDBDictionarySource.h"
#endif
//#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
//    #include <Poco/Data/ODBC/Connector.h>
//#endif
#if USE_MYSQL
#include "MySQLDictionarySource.h"
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}


void registerDictionarySourceFile(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        DUMP("INREGFILE");
        //const auto tableName = config.getString(dictSectionPath + ".yt_table.path");
        //return CreateTableDictionarySource(storage, authToken, tableName, sampleBlock);

        if (dict_struct.has_expressions)
            throw Exception {"Dictionary source of type `file` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        const auto filename = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");
        //return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);

        return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
    };


    DB::DictionarySourceFactory::instance().registerSource("file", createTableSource);
}

void registerDictionarySourceMysql(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
#if USE_MYSQL
        return std::make_unique<MySQLDictionarySource>(dict_struct, config, config_prefix + ".mysql", sample_block);
#else
        throw Exception {"Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.",
                         ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    DB::DictionarySourceFactory::instance().registerSource("mysql", createTableSource);
}
void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 Context & context) -> DictionarySourcePtr {
        return std::make_unique<ClickHouseDictionarySource>(dict_struct, config, config_prefix + ".clickhouse", sample_block, context);
    };
    DB::DictionarySourceFactory::instance().registerSource("clickhouse", createTableSource);
}
void registerDictionarySourceMongoDB(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 Context & context) -> DictionarySourcePtr {
#if USE_POCO_MONGODB
        return std::make_unique<MongoDBDictionarySource>(dict_struct, config, config_prefix + ".mongodb", sample_block);
#else
        throw Exception {"Dictionary source of type `mongodb` is disabled because poco library was built without mongodb support.",
                         ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    DB::DictionarySourceFactory::instance().registerSource("mongodb", createTableSource);
}
void registerDictionarySourceXDBC(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
        const auto & global_config = context.getConfigRef();
        BridgeHelperPtr bridge = std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(
            global_config, context.getSettings().http_receive_timeout, config.getString(config_prefix + ".odbc.connection_string"));
        return std::make_unique<XDBCDictionarySource>(dict_struct, config, config_prefix + ".odbc", sample_block, context, bridge);
#else
        throw Exception {"Dictionary source of type `odbc` is disabled because poco library was built without ODBC support.",
                         ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    DB::DictionarySourceFactory::instance().registerSource("odbc", createTableSource);
}
void registerDictionarySourceJDBC(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        throw Exception {"Dictionary source of type `jdbc` is disabled until consistent support for nullable fields.",
                         ErrorCodes::SUPPORT_IS_DISABLED};
        //        BridgeHelperPtr bridge = std::make_shared<XDBCBridgeHelper<JDBCBridgeMixin>>(config, context.getSettings().http_receive_timeout, config.getString(config_prefix + ".connection_string"));
        //        return std::make_unique<XDBCDictionarySource>(dict_struct, config, config_prefix + ".jdbc", sample_block, context, bridge);
    };
    DB::DictionarySourceFactory::instance().registerSource("jdbc", createTableSource);
}
void registerDictionarySourceExecutable(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        if (dict_struct.has_expressions)
            throw Exception {"Dictionary source of type `executable` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<ExecutableDictionarySource>(dict_struct, config, config_prefix + ".executable", sample_block, context);
    };
    DB::DictionarySourceFactory::instance().registerSource("executable", createTableSource);
}
void registerDictionarySourceHTTP(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        if (dict_struct.has_expressions)
            throw Exception {"Dictionary source of type `http` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<HTTPDictionarySource>(dict_struct, config, config_prefix + ".http", sample_block, context);
    };
    DB::DictionarySourceFactory::instance().registerSource("http", createTableSource);
}
void registerDictionarySourceLibrary(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        return std::make_unique<LibraryDictionarySource>(dict_struct, config, config_prefix + ".library", sample_block, context);
    };
    DB::DictionarySourceFactory::instance().registerSource("library", createTableSource);
}


void registerDictionaries()
{
    DUMP("");
    {
        auto & factory = DictionaryFactory::instance();
    }

    {
        auto & factory = DictionarySourceFactory::instance();

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

        DUMP("REGs :");

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
}

}
