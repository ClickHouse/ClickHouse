#include <Storages/StorageArrowFlight.h>

#if USE_ARROWFLIGHT
#include <sstream>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Names.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/flight/client.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>

const int ARROWFLIGHT_DEFAULT_PORT = 8815;

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
extern const int ARROWFLIGHT_CONNECTION_FAILURE;
extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
extern const int ARROWFLIGHT_WRITE_ERROR;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Arrow Flight endpoint specified: {}", location_result.status().ToString());
    }
    location = std::move(location_result).ValueOrDie();
    auto client_result = arrow::flight::FlightClient::Connect(location);
    if (!client_result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to connect to Arrow Flight server: {}", client_result.status().ToString());
    }
    client = std::move(client_result).ValueOrDie();
}

std::string buildArrowFlightQueryString(const std::vector<std::string> & column_names, const std::string & dataset_name)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss << "{";
    oss << R"("dataset": ")" << dataset_name << R"(", )";
    oss << "\"columns\": [";

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        oss << "\"" << column_names[i] << "\"";
        if (i + 1 != column_names.size())
            oss << ", ";
    }

    oss << "]";
    oss << "}";

    return oss.str();
}

Names StorageArrowFlight::getColumnNames()
{
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({config.dataset_name});
    auto status = client->GetSchema(descriptor);
    if (!status.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", status.status().ToString());
    }
    arrow::ipc::DictionaryMemo dict;
    auto schema_result = status.ValueOrDie()->GetSchema(&dict);
    if (!schema_result.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", schema_result.status().ToString());
    }
    auto schema = std::move(schema_result).ValueOrDie();

    return schema->field_names();
}

Pipe StorageArrowFlight::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
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
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<ArrowFlightSource>(
        client, buildArrowFlightQueryString(column_names, config.dataset_name), sample_block, column_names, max_block_size));
}

class ArrowFlightSink : public SinkToStorage
{
public:
    using FlightClientPtr = std::shared_ptr<arrow::flight::FlightClient>;

    explicit ArrowFlightSink(
        const StorageMetadataPtr & metadata_snapshot_,
        const FlightClientPtr & client_,
        const String & dataset_name_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , metadata_snapshot(metadata_snapshot_)
        , client(client_)
        , dataset_name(dataset_name_)
    {
    }

    String getName() const override { return "ArrowFlightSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({dataset_name});

        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;

        CHColumnToArrowColumn converter(getHeader(), "Arrow", arrow_settings);
        std::shared_ptr<arrow::Table> table;
        std::vector<Chunk> chunks;
        chunks.emplace_back(std::move(chunk));
        converter.chChunkToArrowTable(table, chunks, getHeader().columns());

        auto maybe_combined_table = table->CombineChunks();
        if (!maybe_combined_table.ok())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot combine chunks: {}", maybe_combined_table.status().ToString());
        }

        auto combined_table = std::move(maybe_combined_table).ValueOrDie();
        arrow::TableBatchReader reader(combined_table);

        std::shared_ptr<arrow::RecordBatch> batch;
        bool first_batch = true;
        std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

        while (true)
        {
            auto status = reader.ReadNext(&batch);
            if (!status.ok())
            {
                throw Exception(ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "Failed to read record batch: {}", status.ToString());
            }

            if (!batch)
                break;

            if (first_batch)
            {
                auto write_result = client->DoPut(descriptor, batch->schema());
                if (!write_result.ok())
                {
                    throw Exception(ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "DoPut failed: {}", write_result.status().ToString());
                }

                auto stream = std::move(write_result).ValueOrDie();
                writer = std::move(stream.writer);
                if (!writer)
                {
                    throw Exception(ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "No data was written: writer was never initialized.");
                }
                first_batch = false;
            }

            auto write_result = writer->WriteRecordBatch(*batch);
            if (!write_result.ok())
            {
                throw Exception(
                    ErrorCodes::ARROWFLIGHT_WRITE_ERROR,
                    "Failed to write record batch to Arrow Flight server: {}",
                    write_result.ToString());
            }
        }

        auto close_result = writer->Close();
        if (!close_result.ok())
        {
            throw Exception(
                ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "Failed to close writer after sending record batches: {}", close_result.ToString());
        }
    }


private:
    StorageMetadataPtr metadata_snapshot;
    FlightClientPtr client;
    String dataset_name;
};

SinkToStoragePtr
StorageArrowFlight::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool /*async_write*/)
{
    return std::make_shared<ArrowFlightSink>(metadata_snapshot, client, config.dataset_name);
}

void registerStorageArrowFlight(StorageFactory & factory)
{
    factory.registerStorage(
        "ArrowFlight",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() != 2)
            {
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Arrow Flight storage requires 2 arguments: flight endpoint, dataset name");
            }

            for (auto & engine_arg : engine_args)
            {
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());
            }

            const auto flight_endpoint = checkAndGetLiteralArgument<String>(engine_args[0], "flight_endpoint");
            const auto dataset_name = checkAndGetLiteralArgument<String>(engine_args[1], "dataset_name");

            auto parsed_host_port = parseAddress(flight_endpoint, ARROWFLIGHT_DEFAULT_PORT);

            return std::make_shared<StorageArrowFlight>(
                args.table_id,
                parsed_host_port.first,
                parsed_host_port.second,
                dataset_name,
                args.columns,
                args.constraints,
                args.getContext());
        },
        {
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::ARROW_FLIGHT,
        });
}

}

#endif
