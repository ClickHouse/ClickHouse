#include <Storages/StorageArrowFlight.h>

#if USE_ARROWFLIGHT
#include <Common/parseAddress.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ArrowFlight/ArrowFlightConnection.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <arrow/flight/client.h>


const int ARROWFLIGHT_DEFAULT_PORT = 8815;

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
    extern const int ARROWFLIGHT_WRITE_ERROR;
}

StorageArrowFlight::Configuration StorageArrowFlight::getConfiguration(ASTs & args, ContextPtr context_)
{
    StorageArrowFlight::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context_))
    {
        configuration = StorageArrowFlight::processNamedCollectionResult(*named_collection);
    }
    else
    {
        if (!(args.size() == 2 || args.size() == 4))
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage ArrowFlight requires 2 or 4 parameters: "
                            "ArrowFlight('host:port', 'dataset' [, 'user', 'password']).");

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(args[0], "host:port"), ARROWFLIGHT_DEFAULT_PORT);
        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;

        configuration.dataset_name = checkAndGetLiteralArgument<String>(args[1], "dataset_name");

        configuration.use_basic_authentication = (args.size() == 4);
        if (configuration.use_basic_authentication)
        {
            configuration.username = checkAndGetLiteralArgument<String>(args[2], "username");
            configuration.password = checkAndGetLiteralArgument<String>(args[3], "password");
        }
    }
    return configuration;
}

StorageArrowFlight::Configuration StorageArrowFlight::processNamedCollectionResult(const NamedCollection & named_collection)
{
    StorageArrowFlight::Configuration configuration;

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments = {
        "host", "hostname", "dataset", "use_basic_authentication", "user", "username", "password",
        "enable_ssl", "ssl_ca", "ssl_override_hostname"
    };
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"port"};
    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(named_collection, required_arguments, optional_arguments);

    configuration.host = named_collection.getAnyOrDefault<String>({"host", "hostname"}, "");
    configuration.port = static_cast<UInt16>(named_collection.get<UInt64>("port"));
    configuration.dataset_name = named_collection.getOrDefault<String>("dataset", "");

    configuration.dataset_name = named_collection.get<String>("dataset");

    configuration.use_basic_authentication = named_collection.getOrDefault<bool>("use_basic_authentication", true);
    bool is_username_set = named_collection.has("username") || named_collection.has("user");
    if (configuration.use_basic_authentication && !is_username_set)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Basic authentication requires the 'username' to be specified in named collection");
    if (!configuration.use_basic_authentication && is_username_set)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'username' is specified however the basic authentication is disabled in named collection");

    if (is_username_set)
    {
        configuration.username = named_collection.getAny<String>({"username", "user"});
        configuration.password = named_collection.getOrDefault<String>("password", "");
    }

    configuration.enable_ssl = named_collection.getOrDefault<bool>("enable_ssl", false);
    if (configuration.enable_ssl)
    {
        configuration.ssl_ca = named_collection.getOrDefault<String>("ssl_ca", "");
        configuration.ssl_override_hostname = named_collection.getOrDefault<String>("ssl_override_hostname", "");
    }

    return configuration;
}

StorageArrowFlight::StorageArrowFlight(
    const StorageID & table_id_,
    std::shared_ptr<ArrowFlightConnection> connection_,
    const String & dataset_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , connection(connection_)
    , dataset_name(dataset_name_)
    , log(&Poco::Logger::get("StorageArrowFlight (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
        storage_metadata.setColumns(getTableStructureFromData(connection_, dataset_name_));
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StorageArrowFlight::getTableStructureFromData(
    std::shared_ptr<ArrowFlightConnection> connection_,
    const String & dataset_name_)
{
    auto client = connection_->getClient();
    auto options = connection_->getOptions();

    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({dataset_name_});
    auto status = client->GetSchema(*options, descriptor);
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

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, nullptr, "Arrow", /* format_settings= */ {});
    return ColumnsDescription::fromNamesAndTypes(header.getNamesAndTypes());
}

Pipe StorageArrowFlight::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<ArrowFlightSource>(connection, dataset_name, sample_block));
}

class ArrowFlightSink : public SinkToStorage
{
public:
    explicit ArrowFlightSink(
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<ArrowFlightConnection> connection_,
        const String & dataset_name_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , metadata_snapshot(metadata_snapshot_)
        , connection(connection_)
        , dataset_name(dataset_name_)
    {
    }

    String getName() const override { return "ArrowFlightSink"; }

    void consume(Chunk & chunk) override
    {
        auto client = connection->getClient();
        auto options = connection->getOptions();

        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({dataset_name});

        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;

        CHColumnToArrowColumn converter(getHeader(), "Arrow", arrow_settings);
        std::shared_ptr<arrow::Table> table;
        std::vector<Chunk> chunks;
        chunks.emplace_back(std::move(chunk));
        converter.chChunkToArrowTable(table, chunks, getHeader().columns());

        auto reader_res = arrow::RecordBatchReader::MakeFromIterator(
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>{arrow::TableBatchReader{table}}, converter.getArrowSchema());
        if (!reader_res.ok())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert chunk to arrow stream: {}", reader_res.status().ToString());
        }
        const auto & reader = reader_res.ValueOrDie();

        std::shared_ptr<arrow::RecordBatch> batch;
        bool first_batch = true;
        std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

        while (true)
        {
            auto status = reader->ReadNext(&batch);
            if (!status.ok())
            {
                throw Exception(ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "Failed to read record batch: {}", status.ToString());
            }

            if (!batch)
                break;

            if (first_batch)
            {
                auto write_result = client->DoPut(*options, descriptor, batch->schema());
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
    std::shared_ptr<ArrowFlightConnection> connection;
    String dataset_name;
};

SinkToStoragePtr
StorageArrowFlight::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool /*async_write*/)
{
    return std::make_shared<ArrowFlightSink>(metadata_snapshot, connection, dataset_name);
}

void registerStorageArrowFlight(StorageFactory & factory)
{
    factory.registerStorage(
        "ArrowFlight",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            ASTs & engine_args = args.engine_args;
            auto config = StorageArrowFlight::getConfiguration(engine_args, args.getLocalContext());
            auto connection = std::make_shared<ArrowFlightConnection>(config);

            return std::make_shared<StorageArrowFlight>(
                args.table_id,
                connection,
                config.dataset_name,
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
