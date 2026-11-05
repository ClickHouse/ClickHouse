#include <Storages/StorageXDBC.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Net/HTTPRequest.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsBool odbc_bridge_use_connection_pooling;
}

namespace ServerSetting
{
    extern const ServerSettingsSeconds keep_alive_timeout;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StorageXDBC::StorageXDBC(
    const StorageID & table_id_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    ColumnsDescription columns_,
    ConstraintsDescription constraints_,
    const String & comment,
    ContextPtr context_,
    const BridgeHelperPtr bridge_helper_)
    : IStorageURLBase(
        "",
        context_,
        table_id_,
        IXDBCBridgeHelper::DEFAULT_FORMAT,
        getFormatSettings(context_),
        columns_,
        constraints_,
        comment,
        "" /* CompressionMethod */)
    , bridge_helper(bridge_helper_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , log(getLogger("Storage" + bridge_helper->getName()))
{
    uri = bridge_helper->getMainURI().toString();
}

std::string StorageXDBC::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_POST;
}

std::vector<std::pair<std::string, std::string>> StorageXDBC::getReadURIParams(
    const Names & /* column_names */,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    const ContextPtr & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size) const
{
    return bridge_helper->getURLParams(max_block_size);
}

std::function<void(std::ostream &)> StorageXDBC::getReadPOSTDataCallback(
    const Names & column_names,
    const ColumnsDescription & columns_description,
    const SelectQueryInfo & query_info,
    const ContextPtr & local_context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    String query = transformQueryForExternalDatabase(
        query_info,
        column_names,
        columns_description.getOrdinary(),
        bridge_helper->getIdentifierQuotingStyle(),
        LiteralEscapingStyle::Regular,
        remote_database_name,
        remote_table_name,
        local_context);
    LOG_TRACE(log, "Query: {}", query);

    NamesAndTypesList cols;
    for (const String & name : column_names)
    {
        auto column_data = columns_description.getPhysical(name);
        cols.emplace_back(column_data.name, column_data.type);
    }

    auto write_body_callback = [query, cols](std::ostream & os)
    {
        os << "sample_block=" << escapeForFileName(cols.toString());
        os << "&";
        os << "query=" << escapeForFileName(query);
    };

    return write_body_callback;
}

void StorageXDBC::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    bridge_helper->startBridgeSync();
    IStorageURLBase::read(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageXDBC::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    bridge_helper->startBridgeSync();

    auto request_uri = Poco::URI(uri);
    request_uri.setPath("/write");

    auto url_params = bridge_helper->getURLParams(65536);
    for (const auto & [param, value] : url_params)
        request_uri.addQueryParameter(param, value);

    request_uri.addQueryParameter("db_name", remote_database_name);
    request_uri.addQueryParameter("table_name", remote_table_name);
    request_uri.addQueryParameter("format_name", format_name);
    request_uri.addQueryParameter("sample_block", metadata_snapshot->getSampleBlock().getNamesAndTypesList().toString());


    return std::make_shared<StorageURLSink>(
        request_uri.toString(),
        format_name,
        getFormatSettings(local_context),
        metadata_snapshot->getSampleBlock(),
        local_context,
        ConnectionTimeouts::getHTTPTimeouts(
            local_context->getSettingsRef(),
            local_context->getServerSettings()),
        compression_method);
}

bool StorageXDBC::supportsSubsetOfColumns(const ContextPtr &) const
{
    return true;
}

Block StorageXDBC::getHeaderBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot) const
{
    return storage_snapshot->getSampleBlockForColumns(column_names);
}

std::string StorageXDBC::getName() const
{
    return bridge_helper->getName();
}

namespace
{
    template <typename BridgeHelperMixin>
    void registerXDBCStorage(StorageFactory & factory, const std::string & name)
    {
        factory.registerStorage(name, [name](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            String connection_string;
            String database_or_schema;
            String table;

            if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
            {
                if (Poco::toLower(name) == "jdbc")
                {
                    validateNamedCollection<>(*named_collection, {"datasource"}, {"schema", "external_database",
                                                                                  "external_table", "table"});

                    /// There are aliases for better compatibility and similarity between JDBC and ODBC
                    /// Both aliases cannot be specified simultaneously.

                    connection_string = named_collection->get<String>("datasource");

                    if (named_collection->has("external_database") == named_collection->has("schema"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `external_database` / `schema` argument", name);
                    database_or_schema = named_collection->getAny<String>({"external_database", "schema"});

                    if (named_collection->has("external_table") == named_collection->has("table"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `external_table` / `table` argument", name);
                    table = named_collection->getAny<String>({"external_table", "table"});
                }
                else
                {
                    validateNamedCollection<>(*named_collection, {"external_database", "external_table"}, {"datasource", "connection_settings"});

                    if (named_collection->has("datasource") == named_collection->has("connection_settings"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Table function '{0}' must have exactly one `datasource` / `connection_settings` argument", name);
                    connection_string = named_collection->getAny<String>({"datasource", "connection_settings"});

                    database_or_schema = named_collection->get<String>("external_database");
                    table = named_collection->get<String>("external_table");
                }
            }
            else
            {
                if (engine_args.size() != 3)
                    throw Exception(
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage {} requires exactly 3 parameters: {}('DSN', database or schema, table)",
                        name,
                        name);

                for (size_t i = 0; i < 3; ++i)
                    engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());

                connection_string = checkAndGetLiteralArgument<String>(engine_args[0], "datasource");
                database_or_schema = checkAndGetLiteralArgument<String>(engine_args[1], "external_database");
                table = checkAndGetLiteralArgument<String>(engine_args[2], "external_table");
            }

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<BridgeHelperMixin>>(
                args.getContext(),
                args.getContext()->getSettingsRef()[Setting::http_receive_timeout].value,
                connection_string,
                args.getContext()->getSettingsRef()[Setting::odbc_bridge_use_connection_pooling].value);
            return std::make_shared<StorageXDBC>(
                args.table_id,
                database_or_schema,
                table,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                bridge_helper);

        },
        {
            .source_access_type = BridgeHelperMixin::getSourceAccessObject(),
        });
    }
}

void registerStorageJDBC(StorageFactory & factory)
{
    registerXDBCStorage<JDBCBridgeMixin>(factory, "JDBC");
}

void registerStorageODBC(StorageFactory & factory)
{
    registerXDBCStorage<ODBCBridgeMixin>(factory, "ODBC");
}
}
