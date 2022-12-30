#include <Storages/StorageXDBC.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Formats/FormatFactory.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Net/HTTPRequest.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageXDBC::StorageXDBC(
    const StorageID & table_id_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const ColumnsDescription & columns_,
    const String & comment,
    ContextPtr context_,
    const BridgeHelperPtr bridge_helper_)
    /// Please add support for constraints as soon as StorageODBC or JDBC will support insertion.
    : IStorageURLBase(
        "",
        context_,
        table_id_,
        IXDBCBridgeHelper::DEFAULT_FORMAT,
        getFormatSettings(context_),
        columns_,
        ConstraintsDescription{},
        comment,
        "" /* CompressionMethod */)
    , bridge_helper(bridge_helper_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , log(&Poco::Logger::get("Storage" + bridge_helper->getName()))
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
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size) const
{
    return bridge_helper->getURLParams(max_block_size);
}

std::function<void(std::ostream &)> StorageXDBC::getReadPOSTDataCallback(
    const Names & column_names,
    const ColumnsDescription & columns_description,
    const SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    String query = transformQueryForExternalDatabase(query_info,
        columns_description.getOrdinary(),
        bridge_helper->getIdentifierQuotingStyle(),
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

Pipe StorageXDBC::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    bridge_helper->startBridgeSync();
    return IStorageURLBase::read(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageXDBC::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
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
        ConnectionTimeouts::getHTTPTimeouts(local_context),
        chooseCompressionMethod(uri, compression_method));
}

bool StorageXDBC::supportsSubsetOfColumns() const
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

            if (engine_args.size() != 3)
                throw Exception("Storage " + name + " requires exactly 3 parameters: " + name + "('DSN', database or schema, table)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            for (size_t i = 0; i < 3; ++i)
                engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<BridgeHelperMixin>>(args.getContext(),
                args.getContext()->getSettingsRef().http_receive_timeout.value,
                checkAndGetLiteralArgument<String>(engine_args[0], "connection_string"));
            return std::make_shared<StorageXDBC>(
                args.table_id,
                checkAndGetLiteralArgument<String>(engine_args[1], "database_name"),
                checkAndGetLiteralArgument<String>(engine_args[2], "table_name"),
                args.columns,
                args.comment,
                args.getContext(),
                bridge_helper);

        },
        {
            .source_access_type = BridgeHelperMixin::getSourceAccessType(),
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
