#include "StorageArrowFlight.h"
#include <Common/logger_useful.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/Pipe.h>
#include <arrow/flight/client.h>
#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SortNode.h>
#include <Core/Names.h>
#include <Common/parseAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
}

StorageArrowFlight::StorageArrowFlight(
    const StorageID & table_id_,
    const String & host_,
    const int port_,
    const String & dataset_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("StorageArrowFlight (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    config.host = host_;
    config.port = port_;
    config.dataset_name = dataset_name_;

    arrow::flight::Location location;
    auto location_result = arrow::flight::Location::ForGrpcTcp(host_, port_);
    if (!location_result.ok())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid Arrow Flight endpoint specified: {}",
            location_result.status().ToString()
        );
    }
    location = std::move(location_result).ValueOrDie();
    auto client_result = arrow::flight::FlightClient::Connect(location);
    if (!client_result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE,
            "Failed to connect to Arrow Flight server: {}",
            client_result.status().ToString()
        );
    }
    client = std::move(client_result).ValueOrDie();    
}

std::string buildArrowFlightQueryString(
    const SelectQueryInfo & /*query*/,
    const std::vector<std::string> & /*column_names*/)
{
    return "";
}

Names StorageArrowFlight::getColumnNames() {
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({config.dataset_name});
    auto status = client->GetSchema(descriptor);
    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get table schema: {}",
            status.status().ToString()
        );
    }
    arrow::ipc::DictionaryMemo dict;
    auto schema = status.ValueOrDie()->GetSchema(&dict).ValueOrDie();

    return schema->field_names();
}

Pipe StorageArrowFlight::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<ArrowFlightSource>(client, buildArrowFlightQueryString(query_info, column_names), sample_block, column_names, max_block_size));
}

class ArrowFlightSink : public SinkToStorage
{
public:
    explicit ArrowFlightSink(
        const StorageArrowFlight & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const String & host_,
        const int port_,
        const String & dataset_name_,
        bool)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , host(host_)
        , port(port_)
        , dataset_name(dataset_name_)
    {
    }

    String getName() const override { return "ArrowFlightSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        // auto writer = std::make_shared<ArrowFlightSink>(client, dataset_name, block);
        // writer->write(block);
    }

private:
    [[maybe_unused]] const StorageArrowFlight & storage;
    StorageMetadataPtr metadata_snapshot;
    [[maybe_unused]] String host;
    [[maybe_unused]] int port;
    String dataset_name;
};

SinkToStoragePtr StorageArrowFlight::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool async_write)
{
    return std::make_shared<ArrowFlightSink>(*this, metadata_snapshot, config.host, config.port, config.dataset_name, async_write);
}

void registerStorageArrowFlight(StorageFactory & factory)
{
    factory.registerStorage("ArrowFlight", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arrow Flight storage requires 2 arguments: flight endpoint, dataset name");
        }

        for (auto & engine_arg : engine_args)
        {
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());
        }

        const auto flight_endpoint = checkAndGetLiteralArgument<String>(engine_args[0], "flight_endpoint");
        const auto dataset_name = checkAndGetLiteralArgument<String>(engine_args[1], "dataset_name");

        auto parsed_host_port = parseAddress(flight_endpoint, 6379);

        return std::make_shared<StorageArrowFlight>(args.table_id, parsed_host_port.first, parsed_host_port.second, dataset_name, args.columns, args.constraints, args.getContext());
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessType::ARROW_FLIGHT,
    });
}

}
