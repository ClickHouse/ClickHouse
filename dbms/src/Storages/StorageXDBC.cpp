#include <Dictionaries/ODBCBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageXDBC.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Common/ShellCommand.h>
#include <ext/range.h>
namespace DB
{
    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int EXTERNAL_EXECUTABLE_NOT_FOUND;
        extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
    }


    StorageXDBC::StorageXDBC(const std::string & table_name_,
                             const std::string & remote_database_name_,
                             const std::string & remote_table_name_,
                             const ColumnsDescription & columns_,
                             const Context & context_,
                             const BridgeHelperPtr bridge_helper_)
            : IStorageURLBase(Poco::URI(), context_, table_name_, IXDBCBridgeHelper::DEFAULT_FORMAT, columns_)
            , bridge_helper(bridge_helper_)
            , remote_database_name(remote_database_name_)
            , remote_table_name(remote_table_name_)
    {
        // @todo give it appropriate name
        log = &Poco::Logger::get("StorageXDBC");
        uri = bridge_helper->getMainURI();
    }

    std::string StorageXDBC::getReadMethod() const
    {
        return Poco::Net::HTTPRequest::HTTP_POST;
    }

    std::vector<std::pair<std::string, std::string>> StorageXDBC::getReadURIParams(const Names & column_names,
                                                                                   const SelectQueryInfo & /*query_info*/,
                                                                                   const Context & /*context*/,
                                                                                   QueryProcessingStage::Enum & /*processed_stage*/,
                                                                                   size_t max_block_size) const
    {
        NamesAndTypesList cols;
        for (const String & name : column_names)
        {
            auto column_data = getColumn(name);
            cols.emplace_back(column_data.name, column_data.type);
        }
        return bridge_helper->getURLParams(cols.toString(), max_block_size);
    }

    std::function<void(std::ostream &)> StorageXDBC::getReadPOSTDataCallback(const Names & /*column_names*/,
                                                                             const SelectQueryInfo & query_info,
                                                                             const Context & context,
                                                                             QueryProcessingStage::Enum & /*processed_stage*/,
                                                                             size_t /*max_block_size*/) const
    {
        String query = transformQueryForExternalDatabase(
                *query_info.query, getColumns().ordinary, bridge_helper->getIdentifierQuotingStyle(), remote_database_name, remote_table_name, context);

        return [query](std::ostream & os) { os << "query=" << query; };
    }

    BlockInputStreams StorageXDBC::read(const Names & column_names,
                                        const SelectQueryInfo & query_info,
                                        const Context & context,
                                        QueryProcessingStage::Enum processed_stage,
                                        size_t max_block_size,
                                        unsigned num_streams)
    {
        check(column_names);

        bridge_helper->startBridgeSync();
        return IStorageURLBase::read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    }


    Block StorageXDBC::getHeaderBlock(const Names & column_names) const
    {
        return getSampleBlockForColumns(column_names);
    }

    void registerStorageJDBC(StorageFactory & factory)
    {
        factory.registerStorage("JDBC", [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() != 3)
                throw Exception(
                        "Storage JDBC requires exactly 3 parameters: JDBC('DSN', database or schema, table)", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            for (size_t i = 0; i < 3; ++i)
                engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<JDBCBridgeMixin>>(
                    args.context.getConfigRef(),
                    args.context.getSettingsRef().http_receive_timeout.value,
                    static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>()
            );
            return std::make_shared<StorageJDBC>(args.table_name,
                                                 static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>(),
                                                 static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>(),
                                                 args.columns,
                                                 args.context,
                                                 bridge_helper
            );

        });
    }

    void registerStorageIDBC(StorageFactory & factory)
    {
        factory.registerStorage("IDBC", [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() != 3)
                throw Exception(
                        "Storage IDBC requires exactly 3 parameters: IDBC('DSN', database or schema, table)", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            for (size_t i = 0; i < 3; ++i)
                engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(
                    args.context.getConfigRef(),
                    args.context.getSettingsRef().http_receive_timeout.value,
                    static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>()
            );
            return std::make_shared<StorageIDBC>(args.table_name,
                                                 static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>(),
                                                 static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>(),
                                                 args.columns,
                                                 args.context,
                                                 bridge_helper
            );

        });
    }

}
