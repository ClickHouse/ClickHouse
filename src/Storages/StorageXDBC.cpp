#include "StorageXDBC.h"
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Common/ShellCommand.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Processors/Pipe.h>

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
    const Context & context_,
    const BridgeHelperPtr bridge_helper_)
    /// Please add support for constraints as soon as StorageODBC or JDBC will support insertion.
    : IStorageURLBase(Poco::URI(),
                      context_,
                      table_id_,
                      IXDBCBridgeHelper::DEFAULT_FORMAT,
                      columns_,
                      ConstraintsDescription{},
                      "" /* CompressionMethod */)
    , bridge_helper(bridge_helper_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
{
    log = &Poco::Logger::get("Storage" + bridge_helper->getName());
    uri = bridge_helper->getMainURI();
}

std::string StorageXDBC::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_POST;
}

std::vector<std::pair<std::string, std::string>> StorageXDBC::getReadURIParams(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size) const
{
    NamesAndTypesList cols;
    for (const String & name : column_names)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(name);
        cols.emplace_back(column_data.name, column_data.type);
    }
    return bridge_helper->getURLParams(cols.toString(), max_block_size);
}

std::function<void(std::ostream &)> StorageXDBC::getReadPOSTDataCallback(
    const Names & /*column_names*/,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    String query = transformQueryForExternalDatabase(query_info,
        metadata_snapshot->getColumns().getOrdinary(),
        bridge_helper->getIdentifierQuotingStyle(),
        remote_database_name,
        remote_table_name,
        context);

    return [query](std::ostream & os) { os << "query=" << query; };
}

Pipes StorageXDBC::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    bridge_helper->startBridgeSync();
    return IStorageURLBase::read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

BlockOutputStreamPtr StorageXDBC::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    bridge_helper->startBridgeSync();

    NamesAndTypesList cols;
    Poco::URI request_uri = uri;
    request_uri.setPath("/write");
    for (const String & name : metadata_snapshot->getSampleBlock().getNames())
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(name);
        cols.emplace_back(column_data.name, column_data.type);
    }
    auto url_params = bridge_helper->getURLParams(cols.toString(), 65536);
    for (const auto & [param, value] : url_params)
        request_uri.addQueryParameter(param, value);
    request_uri.addQueryParameter("db_name", remote_database_name);
    request_uri.addQueryParameter("table_name", remote_table_name);
    request_uri.addQueryParameter("format_name", format_name);

    return std::make_shared<StorageURLBlockOutputStream>(
        request_uri,
        format_name,
        metadata_snapshot->getSampleBlock(),
        context,
        ConnectionTimeouts::getHTTPTimeouts(context),
        chooseCompressionMethod(uri.toString(), compression_method));
}

Block StorageXDBC::getHeaderBlock(const Names & column_names, const StorageMetadataPtr & metadata_snapshot) const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
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
                engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

            BridgeHelperPtr bridge_helper = std::make_shared<XDBCBridgeHelper<BridgeHelperMixin>>(args.context,
                args.context.getSettingsRef().http_receive_timeout.value,
                engine_args[0]->as<ASTLiteral &>().value.safeGet<String>());
            return std::make_shared<StorageXDBC>(args.table_id,
                engine_args[1]->as<ASTLiteral &>().value.safeGet<String>(),
                engine_args[2]->as<ASTLiteral &>().value.safeGet<String>(),
                args.columns,
                args.context,
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
