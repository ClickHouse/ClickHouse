#include <Storages/ColumnsDescription.h>
#include <Storages/StorageArrowFlight.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/VirtualColumnUtils.h>

#if USE_ARROWFLIGHT
#include <Common/Logger.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseAddress.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCMessages.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ArrowFlight/ArrowFlightConnection.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>


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

namespace Setting
{
    extern const SettingsArrowFlightDescriptorType arrow_flight_request_descriptor_type;
    extern const SettingsUInt64 arrow_flight_request_timeout_sec;
}

StorageArrowFlight::Configuration StorageArrowFlight::getConfiguration(ASTs & args, ContextPtr context_, const StorageID * table_id)
{
    StorageArrowFlight::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context_, true, nullptr, table_id))
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

    /// Enforce <remote_url_allow_hosts> before any connection is established.
    context_->getGlobalContext()->getRemoteHostFilter().checkHostAndPort(
        configuration.host, std::to_string(configuration.port));

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
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , connection(connection_)
    , dataset_name(dataset_name_)
    , log(&Poco::Logger::get("StorageArrowFlight (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
        storage_metadata.setColumns(getTableStructureFromData(connection_, dataset_name_, context_));
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageArrowFlight::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

namespace
{

/// The descriptor of a dataset: its path, or a query reading it with `arrow_flight_request_descriptor_type = 'command'`.
arrow::flight::protocol::FlightDescriptor makeDescriptor(const String & dataset_name, const ContextPtr & context)
{
    arrow::flight::protocol::FlightDescriptor descriptor;
    if (context && context->getSettingsRef()[Setting::arrow_flight_request_descriptor_type] == ArrowFlightDescriptorType::Command)
    {
        descriptor.set_type(arrow::flight::protocol::FlightDescriptor::CMD);
        descriptor.set_cmd("SELECT * FROM " + dataset_name);
    }
    else
    {
        descriptor.set_type(arrow::flight::protocol::FlightDescriptor::PATH);
        descriptor.add_path(dataset_name);
    }
    return descriptor;
}

}

ColumnsDescription StorageArrowFlight::getTableStructureFromData(
    std::shared_ptr<ArrowFlightConnection> connection_,
    const String & dataset_name_,
    ContextPtr context_)
{
    UInt64 timeout_sec = context_->getSettingsRef()[Setting::arrow_flight_request_timeout_sec];
    auto stub = connection_->getStub(timeout_sec);

    auto descriptor = makeDescriptor(dataset_name_, context_);
    grpc::ClientContext call;
    connection_->prepareCall(call, timeout_sec);
    arrow::flight::protocol::SchemaResult schema_result;
    auto status = stub->GetSchema(&call, descriptor, &schema_result);
    if (!status.ok())
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", ArrowFlightConnection::formatError(status, call));

    /// The schema is a serialized `Schema` message; it is read as a stream of that message only.
    auto schema_message = ArrowIPC::splitStream(schema_result.schema());
    if (schema_message.empty())
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: the server returned an empty schema");
    ArrowIPC::IPCMessagesReadBuffer buffer([&schema_message, done = false](ArrowIPC::IPCMessage & message) mutable
    {
        if (done)
            return false;
        message = schema_message.front();
        done = true;
        return true;
    });
    ArrowIPCSchemaReader schema_reader(buffer, /* stream_ */ true, getFormatSettings(context_));
    return ColumnsDescription(schema_reader.readSchema());
}

Pipe StorageArrowFlight::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context_,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto [physical_columns, virtual_columns] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(column_names, storage_snapshot);
    Block sample_block = storage_snapshot->getSampleBlockForColumns(physical_columns);
    Block virtual_header = storage_snapshot->getSampleBlockForColumns(virtual_columns);

    return Pipe(std::make_shared<ArrowFlightSource>(
        connection,
        dataset_name,
        sample_block,
        virtual_header,
        context_));
}

class ArrowFlightSink final : public SinkToStorage
{
public:
    explicit ArrowFlightSink(
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<ArrowFlightConnection> connection_,
        const String & dataset_name_,
        ContextPtr context_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , metadata_snapshot(metadata_snapshot_)
        , connection(connection_)
        , dataset_name(dataset_name_)
        , context(context_)
        , request_timeout_sec(context_ ? context_->getSettingsRef()[Setting::arrow_flight_request_timeout_sec] : 0)
    {
    }

    String getName() const override { return "ArrowFlightSink"; }

    void consume(Chunk & chunk) override
    {
        if (chunk.getNumRows() == 0)
            return;

        auto stub = connection->getStub(request_timeout_sec);
        auto descriptor = makeDescriptor(dataset_name, context);

        /// Mirrors `ArrowFlight::flightFormatSettings`, including which schema settings it does not read from the
        /// context; see the note there.
        FormatSettings format_settings;
        /// Without a context there are no settings to read: reject a type with no Arrow mapping rather than
        /// silently push it to the remote server as opaque bytes.
        auto unsupported_types = FormatSettings::ArrowUnsupportedTypes::THROW;
        if (context)
        {
            format_settings = getFormatSettings(context);
            unsupported_types = format_settings.arrow.output_unsupported_types;
        }
        format_settings.arrow = FormatSettings{}.arrow;
        format_settings.arrow.output_string_as_string = true;
        format_settings.arrow.output_unsupported_types = unsupported_types;

        Chunks chunks;
        chunks.emplace_back(std::move(chunk));
        auto encoded = ArrowIPC::encodeStream(getHeader(), chunks, format_settings);

        grpc::ClientContext call;
        connection->prepareCall(call, request_timeout_sec);
        auto stream = stub->DoPut(&call);

        auto write = [&](const ArrowIPC::IPCMessage & message, bool with_descriptor)
        {
            arrow::flight::protocol::FlightData data;
            if (with_descriptor)
                *data.mutable_flight_descriptor() = descriptor;
            data.set_data_header(message.header);
            data.set_data_body(message.body);
            return stream->Write(data);
        };

        /// A failed write means that the call has ended: its status says why.
        bool written = write(encoded.schema, /* with_descriptor */ true);
        for (size_t i = 0; written && i < encoded.batches.size(); ++i)
            written = write(encoded.batches[i], /* with_descriptor */ false);
        if (written)
            stream->WritesDone();

        arrow::flight::protocol::PutResult result;
        while (stream->Read(&result))
        {
        }
        auto status = stream->Finish();
        if (!status.ok())
            throw Exception(
                ErrorCodes::ARROWFLIGHT_WRITE_ERROR,
                "Failed to write record batch to Arrow Flight server: {}",
                ArrowFlightConnection::formatError(status, call));
        if (!written)
            throw Exception(ErrorCodes::ARROWFLIGHT_WRITE_ERROR, "Failed to write record batch to Arrow Flight server: the stream was closed");
    }


private:
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<ArrowFlightConnection> connection;
    String dataset_name;
    ContextPtr context;
    UInt64 request_timeout_sec;
};

SinkToStoragePtr
StorageArrowFlight::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_write*/)
{
    return std::make_shared<ArrowFlightSink>(metadata_snapshot, connection, dataset_name, context_);
}

void registerStorageArrowFlight(StorageFactory & factory)
{
    factory.registerStorage(
        "ArrowFlight",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            ASTs & engine_args = args.engine_args;
            auto config = StorageArrowFlight::getConfiguration(engine_args, args.getLocalContext(), &args.table_id);
            auto connection = std::make_shared<ArrowFlightConnection>(config);

            return std::make_shared<StorageArrowFlight>(
                args.table_id,
                connection,
                config.dataset_name,
                args.columns,
                args.constraints,
                args.getLocalContext());
        },
        /// ArrowFlight('host:port', 'dataset', 'username', 'password')
        SecretArgumentsSpec{.positional_secret_slots = {3}, .secret_keys = {"password"}},
        {
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::ARROW_FLIGHT,
        },
        Documentation{
            .description = R"DOCS_MD(
The ArrowFlight table engine enables ClickHouse to read from and write to remote datasets via the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol.
This integration allows ClickHouse to interact with external Flight-enabled servers in a columnar Arrow format with high performance.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Engine Parameters**

- `host:port` — Address of the remote Arrow Flight server. If the port is omitted, the default port `8815` is used. [String](/reference/data-types/string).
- `dataset_name` — Identifier of the dataset on the Flight server (used as a PATH descriptor or in a `SELECT *` query depending on the `arrow_flight_request_descriptor_type` setting). [String](/reference/data-types/string).
- `username` — Username for basic HTTP authentication. [String](/reference/data-types/string).
- `password` — Password for basic HTTP authentication. [String](/reference/data-types/string).

If `username` and `password` are omitted, authentication is not used (this works only if the Arrow Flight server allows unauthenticated access).

The column list is optional — if omitted, the schema is inferred from the remote Arrow Flight server via `GetSchema`.

## Named Collections {#named-collections}

The engine supports [named collections](/concepts/features/configuration/server-config/named-collections) for storing connection parameters:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Named collection parameters:

| Parameter | Required | Default | Description |
|---|---|---|---|
| `host` or `hostname` | No | `""` | Server hostname. |
| `port` | Yes | — | Server port. |
| `dataset` | No | `""` | Dataset name or descriptor. |
| `use_basic_authentication` | No | `true` | Enable basic authentication. |
| `user` or `username` | If auth enabled | — | Username for authentication. |
| `password` | No | `""` | Password for authentication. |
| `enable_ssl` | No | `false` | Enable TLS encryption. |
| `ssl_ca` | No | `""` | Path to the CA certificate file for TLS verification. |
| `ssl_override_hostname` | No | `""` | Override the hostname checked during TLS verification. |

## Settings {#settings}

- `arrow_flight_request_descriptor_type` — Controls how the dataset name is sent to the Flight server. Possible values: `path` (default, sends as a PATH descriptor) or `command` (sends as a CMD descriptor with `SELECT * FROM <dataset>`). Use `command` for Flight servers that expect SQL commands (e.g., Dremio).
- `arrow_flight_request_timeout_sec` — Timeout in seconds for a single Arrow Flight request, default `300`. It bounds the whole request: for a `SELECT` that is the entire result stream, not just the wait for the first record batch. `0` means no timeout, in which case a Flight server that accepts a request and never answers blocks the query until the connection is closed.

## Usage Example {#usage-example}

Reading data from a remote Arrow Flight server:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserting data into a remote Arrow Flight server:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

## Notes {#notes}

- If columns are specified in the `CREATE TABLE` statement, they must match the schema returned by the Flight server.
- If columns are omitted, the schema is inferred automatically from the remote server.
- Both reading (`SELECT`) and writing (`INSERT`) are supported.
- The `arrow_flight_request_descriptor_type` setting controls whether the dataset name is sent as a PATH descriptor or as a CMD descriptor wrapping a `SELECT *` query.

## See Also {#see-also}

- [arrowFlight table function](/reference/functions/table-functions/arrowflight)
- [Arrow Flight Interface](/concepts/features/interfaces/arrowflight)
- [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
- [Arrow format integration in ClickHouse](/reference/formats/Arrow/Arrow)
)DOCS_MD",
            .syntax = "ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password'])",
        });
}

}

#endif
