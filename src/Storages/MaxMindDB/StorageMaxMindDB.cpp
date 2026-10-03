#include <Storages/MaxMindDB/StorageMaxMindDB.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <tuple>
#include <arpa/inet.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
extern const SettingsBool allow_experimental_maxminddb_table_engine;
}

namespace
{
struct MaxMindDBSnapshotData : StorageSnapshot::Data
{
    explicit MaxMindDBSnapshotData(MaxMindDBGenerationPtr generation_)
        : generation(std::move(generation_))
    {
    }
    MaxMindDBGenerationPtr generation;
};

class LookupReader
{
public:
    LookupReader(MaxMindDBGenerationPtr generation_, const NamesAndTypesList & columns_, TypeIndex ip_type_)
        : generation(std::move(generation_))
        , columns(columns_)
        , decoder(columns)
        , ip_type(ip_type_)
    {
        for (const auto & column : columns)
        {
            if (column.name == "ip")
                ip_position = header.columns();
            ColumnPtr initial;
            if (MaxMindDBGeneration::isMetadataColumn(column.name))
            {
                auto value = generation->getMetadataColumn(column.name);
                metadata_columns.emplace_back(header.columns(), value);
                initial = ColumnConst::create(value, 0);
            }
            else
                initial = column.type->createColumn();
            header.insert({initial, column.type, column.name});
        }
    }

    Chunk read(const IColumn & keys, size_t begin, size_t end, PaddedPODArray<UInt8> * null_map = nullptr)
    {
        auto output = header.cloneEmptyColumns();
        if (null_map)
            for (const auto & [position, value] : metadata_columns)
                output[position] = header.getByPosition(position).type->createColumn();
        for (auto & column : output)
            column->reserve(end - begin);
        decoder.startBlock(end - begin);
        const auto * nullable = typeid_cast<const ColumnNullable *>(&keys);
        const auto & values = nullable ? nullable->getNestedColumn() : keys;
        size_t rows = 0;
        for (size_t row = begin; row < end; ++row)
        {
            MMDB_lookup_result_s result{};
            if (!nullable || !nullable->isNullAt(row))
            {
                int status = MMDB_SUCCESS;
                if (ip_type == TypeIndex::IPv4)
                {
                    sockaddr_in address{};
                    address.sin_family = AF_INET;
                    address.sin_addr.s_addr = htonl(assert_cast<const ColumnVector<IPv4> &>(values).getData()[row].toUnderType());
                    result = MMDB_lookup_sockaddr(&generation->database(), reinterpret_cast<const sockaddr *>(&address), &status);
                }
                else
                {
                    sockaddr_in6 address{};
                    address.sin6_family = AF_INET6;
                    const auto & ip = assert_cast<const ColumnVector<IPv6> &>(values).getData()[row];
                    memcpy(address.sin6_addr.s6_addr, &ip.toUnderType(), 16);
                    result = MMDB_lookup_sockaddr(&generation->database(), reinterpret_cast<const sockaddr *>(&address), &status);
                }
                if (status != MMDB_SUCCESS)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB lookup failed: {}", MMDB_strerror(status));
            }
            if (null_map)
                (*null_map)[row] = result.found_entry;
            if (!result.found_entry)
            {
                if (!null_map)
                    continue;
                for (auto & column : output)
                    column->insertDefault();
            }
            else
            {
                decoder.append(result.entry, output);
                if (ip_position)
                    output[*ip_position]->insertFrom(values, row);
                if (null_map)
                    for (const auto & [position, value] : metadata_columns)
                        output[position]->insertFrom(*value, 0);
            }
            ++rows;
        }
        if (!null_map)
            for (const auto & [position, value] : metadata_columns)
                output[position] = ColumnConst::create(value, rows);
        return Chunk(std::move(output), rows);
    }

private:
    MaxMindDBGenerationPtr generation;
    NamesAndTypesList columns;
    MaxMindDBColumnDecoder decoder;
    TypeIndex ip_type;
    Block header;
    std::optional<size_t> ip_position;
    std::vector<std::pair<size_t, ColumnPtr>> metadata_columns;
};

class MaxMindDBSourceProcessor final : public ISource
{
public:
    MaxMindDBSourceProcessor(
        SharedHeader header,
        MaxMindDBGenerationPtr generation,
        const NamesAndTypesList & columns,
        ColumnPtr keys_,
        size_t begin,
        size_t end_,
        size_t block_size,
        TypeIndex ip_type)
        : ISource(std::move(header))
        , reader(std::move(generation), columns, ip_type)
        , keys(std::move(keys_))
        , position(begin)
        , end(end_)
        , max_block_size(std::max<size_t>(1, block_size))
    {
    }

    String getName() const override { return "MaxMindDB"; }

    Chunk generate() override
    {
        while (position < end)
        {
            const size_t next = position + std::min(max_block_size, end - position);
            auto chunk = reader.read(*keys, position, next);
            position = next;
            if (chunk.getNumRows())
                return chunk;
        }
        return {};
    }

private:
    LookupReader reader;
    ColumnPtr keys;
    size_t position;
    size_t end;
    size_t max_block_size;
};

class MaxMindDBLookupSnapshot final : public IKeyValueEntity
{
public:
    MaxMindDBLookupSnapshot(MaxMindDBGenerationPtr generation_, ColumnsDescription columns_)
        : generation(std::move(generation_))
        , columns(std::move(columns_))
    {
        lookup_columns = columns;
        lookup_columns.add({MaxMindDBGeneration::metadata_column_name, MaxMindDBGeneration::getMetadataType()});
    }

    Names getPrimaryKey() const override { return {"ip"}; }

    Block getSampleBlock(const Names & required_columns) const override
    {
        Block result;
        const auto names = required_columns.empty()
            ? lookup_columns.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()).getNames()
            : required_columns;
        for (const auto & column : lookup_columns.getByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), names))
            result.insert({column.type->createColumn(), column.type, column.name});
        return result;
    }

    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const override
    {
        if (keys.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB lookup requires one IP key column");
        out_offsets.clear();
        const auto names = required_columns.empty()
            ? lookup_columns.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()).getNames()
            : required_columns;
        const auto requested = lookup_columns.getByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), names);
        const auto key_type = columns.getPhysical("ip").type;
        auto converted = removeNullable(keys.front().type)->equals(*key_type)
            ? keys.front().column->convertToFullIfWrapped()
            : castColumn(keys.front(), makeNullable(key_type))->convertToFullIfWrapped();
        out_null_map.resize(converted->size());
        LookupReader reader(generation, requested, key_type->getTypeId());
        return reader.read(*converted, 0, converted->size(), &out_null_map);
    }

private:
    MaxMindDBGenerationPtr generation;
    ColumnsDescription columns;
    ColumnsDescription lookup_columns;
};

class ReadFromMaxMindDB final : public SourceStepWithFilter
{
public:
    ReadFromMaxMindDB(
        SharedHeader header,
        const Names & names,
        const SelectQueryInfo & info,
        const StorageSnapshotPtr & snapshot,
        ContextPtr context_,
        size_t block_size,
        size_t num_streams_)
        : SourceStepWithFilter(std::move(header), names, info, snapshot, context_)
        , max_block_size(block_size)
        , num_streams(num_streams_)
    {
    }

    String getName() const override { return "ReadFromMaxMindDB"; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
        const auto ip_type = storage_snapshot->metadata->columns.getPhysical("ip").type;
        std::tie(fields, full_scan) = getFilterKeys("ip", ip_type, filter_actions_dag.get(), context);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        const auto ip_type = storage_snapshot->metadata->columns.getPhysical("ip").type;
        if (full_scan)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "MaxMindDB requires an equality or IN predicate on the ip column; full scans are not supported");
        if (fields->empty())
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
            return;
        }
        ::sort(fields->begin(), fields->end());
        fields->erase(std::unique(fields->begin(), fields->end()), fields->end());
        auto keys = ip_type->createColumn();
        keys->reserve(fields->size());
        for (const auto & field : *fields)
            keys->insert(field);
        ColumnPtr shared_keys = std::move(keys);

        const auto & generation = assert_cast<const MaxMindDBSnapshotData &>(*storage_snapshot->data).generation;
        const auto columns = storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All)
                .withSubcolumns()
                .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader),
            required_source_columns);
        Pipes pipes;
        const size_t streams = std::min(std::max<size_t>(1, num_streams), shared_keys->size());
        for (size_t stream = 0; stream < streams; ++stream)
        {
            auto processor = std::make_shared<MaxMindDBSourceProcessor>(
                getOutputHeader(),
                generation,
                columns,
                shared_keys,
                shared_keys->size() * stream / streams,
                shared_keys->size() * (stream + 1) / streams,
                max_block_size,
                ip_type->getTypeId());
            processor->setStorageLimits(query_info.storage_limits);
            pipes.emplace_back(std::move(processor));
        }
        pipeline.init(Pipe::unitePipes(std::move(pipes)));
    }

private:
    FieldVectorPtr fields;
    bool full_scan = true;
    size_t max_block_size;
    size_t num_streams;
};
}

StorageMaxMindDB::StorageMaxMindDB(
    const StorageID & table_id,
    std::unique_ptr<MaxMindDBSource> source_,
    std::unique_ptr<MaxMindDBSettings> settings_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const String & comment,
    const ASTPtr & primary_key,
    ContextPtr context_)
    : IStorage(table_id)
    , WithContext(context_->getGlobalContext())
    , source(std::move(source_))
    , settings(std::move(settings_))
    , refresh_interval_ms(settings->refreshIntervalMilliseconds())
    , log(getLogger("StorageMaxMindDB"))
{
    auto opened = std::make_unique<MaxMindDBGeneration>(source->load({}, *settings));
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns.empty() ? opened->inferSchema() : columns);
    metadata.setConstraints(constraints);
    metadata.setComment(comment);
    opened->validateSchema(metadata.columns);
    VirtualColumnsDescription virtuals;
    const ColumnsDescription metadata_columns(
        NamesAndTypesList{{MaxMindDBGeneration::metadata_column_name, MaxMindDBGeneration::getMetadataType()}});
    for (const auto & column : metadata_columns.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()))
        virtuals.addEphemeral(column.name, column.type, "", VirtualsMaterializationPlace::Reader);
    metadata.setVirtuals(virtuals);
    metadata.primary_key
        = KeyDescription::getKeyFromAST(primary_key ? primary_key : make_intrusive<ASTIdentifier>("ip"), metadata.columns, {}, context_);
    if (metadata.primary_key.column_names != Names{"ip"})
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB PRIMARY KEY must be the ip column");
    source->checkVersion(opened->sourceFile());
    setInMemoryMetadata(metadata);
    generation.set(std::move(opened));
}

StorageMaxMindDB::~StorageMaxMindDB()
{
    shutdown(false);
}

void StorageMaxMindDB::startup()
{
    if (refresh_interval_ms)
    {
        refresh_task = getContext()->getSchedulePool()->createTask(getStorageID(), "MaxMindDBRefresh", [this] { refresh(); });
        refresh_task->activate();
        refresh_task->scheduleAfter(refresh_interval_ms);
    }
}

void StorageMaxMindDB::shutdown(bool)
{
    if (refresh_task)
        refresh_task->deactivate();
}

void StorageMaxMindDB::refresh()
{
    std::string_view stage = "source metadata or download";
    try
    {
        auto current = generation.get();
        if (auto file = source->load(current->sourceFile().version, *settings))
        {
            stage = "opening the database";
            auto next = std::make_unique<MaxMindDBGeneration>(std::move(file));
            stage = "schema validation";
            const auto metadata = getInMemoryMetadataPtr(getContext(), false);
            next->validateSchema(metadata->columns);
            stage = "checking the source version";
            source->checkVersion(next->sourceFile());
            generation.set(std::move(next));
        }
    }
    catch (...)
    {
        /// Exception bodies from remote servers can contain credentials echoed by the server.
        LOG_ERROR(
            log,
            "MaxMindDB refresh for {} failed during {} with error code {}; retaining the current generation",
            getStorageID().getNameForLogs(),
            stage,
            getCurrentExceptionCode());
    }
    refresh_task->scheduleAfter(refresh_interval_ms);
}

StorageSnapshotPtr StorageMaxMindDB::getStorageSnapshot(const StorageMetadataPtr & metadata, ContextPtr) const
{
    return std::make_shared<StorageSnapshot>(*this, metadata, std::make_shared<MaxMindDBSnapshotData>(generation.get()));
}

void StorageMaxMindDB::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    snapshot->check(column_names);
    auto block = snapshot->getSampleBlockForColumns(column_names);
    const auto & current = assert_cast<const MaxMindDBSnapshotData &>(*snapshot->data).generation;
    for (auto & column : block)
        if (MaxMindDBGeneration::isMetadataColumn(column.name))
            column.column = ColumnConst::create(current->getMetadataColumn(column.name), 0);
    auto header = std::make_shared<const Block>(std::move(block));
    query_plan.addStep(
        std::make_unique<ReadFromMaxMindDB>(header, column_names, query_info, snapshot, query_context, max_block_size, num_streams));
}

Block StorageMaxMindDB::getSampleBlock(const Names & required_columns) const
{
    return getLookupSnapshot()->getSampleBlock(required_columns);
}

Chunk StorageMaxMindDB::getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets) const
{
    return getLookupSnapshot()->getByKeys(keys, required_columns, out_null_map, out_offsets);
}

std::shared_ptr<const IKeyValueEntity> StorageMaxMindDB::getLookupSnapshot() const
{
    const auto metadata = getInMemoryMetadataPtr(getContext(), false);
    return std::make_shared<MaxMindDBLookupSnapshot>(generation.get(), metadata->columns);
}

void registerStorageMaxMindDB(StorageFactory & factory);
void registerStorageMaxMindDB(StorageFactory & factory)
{
    factory.registerStorage(
        "MaxMindDB",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            const auto local_context = args.getLocalContext();
            if (args.mode == LoadingStrictnessLevel::CREATE
                && !local_context->getSettingsRef()[Setting::allow_experimental_maxminddb_table_engine])
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "The MaxMindDB table engine is experimental; set allow_experimental_maxminddb_table_engine = 1");
            checkStorageSettingNames(args);
            if (args.storage_def->order_by || args.storage_def->partition_by || args.storage_def->sample_by)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MaxMindDB supports only PRIMARY KEY ip; ORDER BY, PARTITION BY, and SAMPLE BY are not supported");
            auto settings = std::make_unique<MaxMindDBSettings>();
            settings->loadFromQuery(*args.storage_def);
            auto source = std::make_unique<MaxMindDBSource>(
                args.engine_args, local_context, args.table_id, isFreshTableDefinition(args.mode, args.query.attach_short_syntax));
            return std::make_shared<StorageMaxMindDB>(
                args.table_id,
                std::move(source),
                std::move(settings),
                args.columns,
                args.constraints,
                args.comment,
                args.storage_def->primary_key ? args.storage_def->primary_key->ptr() : ASTPtr{},
                local_context);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            /// Classify the engine as external for restore; the source checks the actual transport's grant.
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = MaxMindDBSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
`MaxMindDB` is a read-only table engine for querying [MaxMind DB](https://maxmind.github.io/MaxMind-DB/) files through `libmaxminddb`. It performs longest-prefix-match lookups directly in a memory-mapped database, without importing its records into ClickHouse.

Enable the experimental engine before creating a table:

```sql
SET allow_experimental_maxminddb_table_engine = 1;
```

## Creating a table {#creating-a-table}

```sql
CREATE TABLE geoip
ENGINE = MaxMindDB('GeoLite2-City.mmdb')
SETTINGS refresh_interval = '10m';
```

The column list is optional. Without it, ClickHouse infers the schema from all distinct data records. The inferred columns are persisted as normal table metadata.

For a manual schema, use the MMDB map keys as column names and named `Tuple` elements:

```sql
CREATE TABLE geoip
(
    ip IPv4,
    country Tuple(iso_code Nullable(String), names Map(String, String)),
    city Tuple(names Map(String, String)),
    location Tuple(latitude Nullable(Float64), longitude Nullable(Float64))
)
ENGINE = MaxMindDB('GeoLite2-City.mmdb')
PRIMARY KEY ip
SETTINGS refresh_interval = '5m';
```

The synthetic `ip` column is required in manual schemas and must have type `IPv4` or `IPv6`. If provided, `PRIMARY KEY` must be `ip`. Other sorting and partitioning clauses, default expressions, writes, mutations, and column alterations are not supported. Column comments and table comments can be altered.

## Sources and authentication {#sources-and-authentication}

The engine accepts one `.mmdb` file or a `tar.gz` archive containing exactly one `.mmdb` file. Gzip archives are detected from their contents, so download URLs do not need a filename extension. Archive member syntax, globs, failover lists, and URLs containing userinfo or fragments are rejected. HTTP query parameters are preserved, including parameters used for authentication; query-bearing URLs are hidden in formatted DDL and query parameters are masked in HTTP diagnostics. Prefer headers or named collections for authentication.

### Local files {#local-files}

```sql
CREATE TABLE geoip ENGINE = MaxMindDB('GeoLite2-City.mmdb');
```

On the server, relative paths are resolved inside `user_files_path`. Absolute paths and resolved symlinks must also remain inside that directory. `clickhouse-local` can read other paths. Creating a table requires `READ ON FILE`.

### HTTP and HTTPS {#http-and-https}

```sql
CREATE TABLE geoip
ENGINE = MaxMindDB('https://example.org/GeoLite2-City.mmdb');

CREATE TABLE authenticated_geoip
ENGINE = MaxMindDB(
    'https://example.org/GeoLite2-City.mmdb',
    headers('Authorization' = 'Bearer example-token')
);
```

HTTP authentication uses the existing `headers` syntax, including `Authorization` for Basic or bearer authentication. Header values are hidden when ClickHouse formats queries without permission to display secrets. Prefer [named collections](/concepts/features/configuration/server-config/named-collections) to storing credentials in DDL. The server's remote host filter, HTTP header filter, TLS configuration, timeouts, and read settings apply. Creating a table requires `READ ON URL`.

#### Downloading a database from MaxMind {#downloading-from-maxmind}

The engine downloads and extracts MaxMind's binary database archives automatically. MaxMind's [current download API](https://dev.maxmind.com/geoip/updating-databases/#automating-downloads) uses HTTPS and Basic Authentication with the account ID as the username and the license key as the password. Its `suffix=tar.gz` parameter is passed unchanged:

```sql
SET allow_experimental_maxminddb_table_engine = 1;
SET max_http_get_redirects = 5;

CREATE TABLE geoip
(
    ip IPv4,
    country Tuple(iso_code Nullable(String))
)
ENGINE = MaxMindDB(
    'https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz',
    headers('Authorization' = 'Basic BASE64_ACCOUNT_ID_COLON_LICENSE_KEY')
)
SETTINGS refresh_interval = '10m';

SELECT country.iso_code
FROM geoip
WHERE ip = toIPv4('8.8.8.8');
```

Replace `BASE64_ACCOUNT_ID_COLON_LICENSE_KEY` with the Base64 encoding of `YOUR_ACCOUNT_ID:YOUR_LICENSE_KEY`. Base64 is an encoding, not encryption. Prefer storing the authorization header in a named collection in the server configuration. The manual `IPv4` schema above can query IPv4 records in an IPv4 or IPv6 database; omitting the column list infers the database's IP type.

The HTTP client follows redirects up to `max_http_get_redirects`. Configure this setting in the server or user profile as well as the creating session so that table attachment after a restart can follow redirects. Background refresh retains the creating query's HTTP settings. The client does not forward `Authorization`, Basic credentials, or cookies to a different origin. MaxMind redirects downloads to a signed Cloudflare R2 URL; allow both hosts in the server's remote host filter and network configuration. Metadata checks use `ETag` or `Last-Modified` to avoid downloading unchanged archives. A refreshed archive is extracted and validated before its MMDB becomes active.

An account permalink containing a license key in a query parameter can also be passed unchanged, for example:

```sql
CREATE TABLE legacy_geoip
ENGINE = MaxMindDB(
    'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=YOUR_LICENSE_KEY&suffix=tar.gz'
)
SETTINGS refresh_interval = '10m';
```

Use the permalink supplied by your MaxMind account and its required authentication. The engine accepting a URL parameter does not guarantee that a provider still accepts an older endpoint or authentication method. The Basic Authentication example uses the documented current API. A command-line client can echo the original SQL when reporting an exception; named collections keep credentials out of that SQL.

### S3 {#s3}

```sql
CREATE TABLE public_geoip
ENGINE = MaxMindDB('https://bucket.s3.amazonaws.com/GeoLite2-City.mmdb', NOSIGN);

CREATE TABLE private_geoip
ENGINE = MaxMindDB(
    'https://bucket.s3.amazonaws.com/GeoLite2-City.mmdb',
    'example-access-key', 'example-secret-key'
);
```

S3 signatures are `MaxMindDB(url)`, `MaxMindDB(url, NOSIGN)`, and `MaxMindDB(url, access_key_id, secret_access_key[, session_token])`. The engine reuses ClickHouse's S3 configuration and credential providers, including endpoint configuration and environment credentials where the server's security policy allows them. Explicit credentials and tokens are hidden in formatted DDL and queries. Creating a table requires `READ ON S3`.

An `s3://` URL and HTTPS URLs without query parameters with hosts ending in `.amazonaws.com` select the S3 transport automatically. A presigned HTTPS URL without explicit S3 credentials uses the HTTP transport and preserves its query parameters. Other S3-compatible HTTP endpoints must use `NOSIGN`, an explicit credential pair, or a named collection with `no_sign_request` or credential settings, to distinguish them from ordinary HTTP sources.

### Named collections {#named-collections}

```xml
<named_collections>
    <geoip_source>
        <url>https://bucket.s3.amazonaws.com/GeoLite2-City.mmdb</url>
        <access_key_id>example-access-key</access_key_id>
        <secret_access_key>example-secret-key</secret_access_key>
        <no_sign_request>false</no_sign_request>
    </geoip_source>
</named_collections>
```

```sql
CREATE TABLE geoip ENGINE = MaxMindDB(geoip_source);
```

HTTP collections support `url` and the existing `headers.headerN.name` / `headers.headerN.value` keys. S3 collections use the existing S3 collection keys, including `session_token`, `no_sign_request`, and `use_environment_credentials`. Collections containing only `url` can describe a local source. Format, compression, and structure arguments have no meaning for an MMDB source.

## Lookups and joins {#lookups-and-joins}

```sql
SELECT country.iso_code, country.names['fr'], city.names['fr'], location.latitude
FROM geoip
WHERE ip = toIPv4('8.8.8.8');

SELECT ip, country.iso_code
FROM geoip
WHERE ip IN (toIPv4('8.8.8.8'), toIPv4('1.1.1.1'));
```

Each distinct requested IP produces at most one row. The engine returns the record for the longest matching network prefix and puts the requested IP, rather than the network address, in `ip`. Addresses without a matching record produce no row. The engine deduplicates keys extracted from equality and `IN` predicates. Other predicates are applied to the resulting rows by ClickHouse.

Queries need an extractable equality or `IN` predicate on `ip`. An unfiltered `SELECT`, a range predicate alone, or an `OR` branch without a bounded IP predicate raises an exception. There is no full scan.

For an inferred language-name tuple, use `country.names.fr`. A manual `Map` column supports `country.names['fr']`.

The engine implements direct key access and supports `direct` joins without scanning the database:

```sql
SELECT visits.ip, geoip.country.iso_code
FROM visits
LEFT ANY JOIN geoip ON visits.ip = geoip.ip
SETTINGS join_algorithm = 'direct';
```

Unmatched join keys use normal ClickHouse join defaults or nulls according to `join_use_nulls`. A direct join pins one database generation for its lifetime.

## Schema inference and compatibility {#schema-inference-and-compatibility}

Inference traverses the search tree and deduplicates data offsets before reading records. MMDB types map to ClickHouse types as follows:

| MMDB type | ClickHouse type |
| --- | --- |
| UTF-8 string, bytes | `String` |
| double, float | `Float64`, `Float32` |
| int32 | `Int32` |
| uint16, uint32, uint64, uint128 | `UInt16`, `UInt32`, `UInt64`, `UInt128` |
| boolean | `Bool` |
| array | `Array` of the recursively merged element type |
| map | `Tuple` with named, recursively merged elements |

Integers widen to a compatible common numeric type, including `Int128` when a signed integer is mixed with `UInt64`. Mixed integers and floating-point values use the common floating-point type; large integers can lose precision in floating-point representations. Incompatible scalar and container structures raise an exception. Fields missing from some records become `Nullable` where ClickHouse supports it. Missing arrays and maps in a manual schema are represented by empty containers, because `Nullable(Array)` and `Nullable(Map)` are unsupported. An array with no elements in any record has type `Array(Nothing)`.

Manual schemas may select a subset of payload fields and use `Map(String, T)` for maps whose keys are dynamic. Missing non-nullable scalar fields and values that cannot fit the declared integer type reject the database. Map keys must be strings; inferred tuple element names must satisfy ClickHouse's named tuple rules. Root payload fields named `ip` or `_mmdb_metadata` conflict with reserved engine columns and are rejected explicitly.

A refreshed database is validated against the persisted column definitions before publication. Additional payload fields are ignored. Existing column types, names, and optionality never change automatically. A new database that removes a required field or changes an existing field incompatibly is rejected. Recreate the table to infer a different schema.

### IPv4 and IPv6 {#ipv4-and-ipv6}

Inference derives `ip` from MMDB `metadata.ip_version`: version 4 gives `IPv4`, version 6 gives `IPv6`. Manual `IPv4` keys can query IPv4 records in an IPv6 database using MaxMind's IPv4 search subtree. `IPv6` keys require an IPv6 database. IPv4 addresses represented as IPv6, including mapped addresses, follow the database's own search-tree aliases. IP lookups use binary socket addresses without converting IPs to strings.

## Virtual database metadata {#virtual-database-metadata}

Every table exposes `_mmdb_metadata`, an automatically generated virtual column. It does not need a declaration in `CREATE TABLE` and is excluded from `SELECT *`. Its type is:

```text
Tuple(
    node_count UInt32,
    record_size UInt16,
    ip_version UInt16,
    database_type String,
    languages Array(String),
    binary_format_major_version UInt16,
    binary_format_minor_version UInt16,
    build_epoch UInt64,
    description Map(String, String)
)
```

These are the MMDB header metadata, rather than the payload for an IP address. `build_epoch` is the database build time in Unix seconds; `record_size` is the search tree record size in bits. For example:

```sql
SELECT
    _mmdb_metadata.database_type,
    toDateTime(_mmdb_metadata.build_epoch, 'UTC') AS database_build_time,
    _mmdb_metadata.ip_version,
    _mmdb_metadata.description['en']
FROM geoip
WHERE ip = toIPv4('8.8.8.8');
```

An equality or `IN` predicate on `ip` is still required. An IP with no match produces no row, even when only metadata is selected. Direct joins can select this column; unmatched join keys receive normal join defaults or nulls. The column name is reserved and cannot be declared as a payload column.

The engine prepares one immutable metadata value per database generation. `SELECT` reads share it as a constant column; direct joins materialize matched values in their result columns, and subsequent query operations and output serialization can materialize or copy values. The metadata and payload always refer to the same pinned generation. After a successful refresh, new queries see the new database metadata while existing queries keep the old metadata. No source URLs or credentials are included in this column.

## Refresh, hot reload, and cache {#refresh-hot-reload-and-cache}

| Setting | Default | Meaning |
| --- | --- | --- |
| `refresh_interval` | `'5m'` | Interval between background version checks. Duration syntax includes `'5m'`, `'10m'`, `'20h'`, and `'100ms'`. `'0'` disables checks. |
| `disk` | `'default'` | Unencrypted local ClickHouse disk for downloaded and extracted files. |
| `max_download_size` | `1073741824` | Maximum download size and total uncompressed archive entry size in bytes. `0` removes the size limit. |

Checks run on ClickHouse's background schedule pool. Local versions compare device, inode, size, and modification time, including subsecond precision. Update local files by writing a separate file and atomically renaming it over the source. Do not modify or truncate an actively mapped file in place.

For HTTP, refresh compares `ETag`, or `Last-Modified` and `Content-Length` when the latter is available. For S3, it uses object metadata including `ETag`. An unchanged version performs metadata requests without downloading the file. Sources lacking usable version metadata are downloaded on each check. Metadata requests reuse the normal HTTP or S3 client rather than a separate networking implementation.

Remote files are streamed to private temporary files on `disk`, bounded by `max_download_size`, and then mapped. For a gzip archive, ClickHouse streams its contents through the existing archive reader and writes only the `.mmdb` entry to a second private file. The archive must contain exactly one `.mmdb` entry and at most 10,000 entries, with no absolute paths or parent-directory traversal. The size limit also bounds the total uncompressed entry bytes, including skipped files. Archive member paths are never used as filesystem destinations. The compressed cache file is removed after extraction; the extracted file is memory-mapped. The same extraction applies to local gzip archives, using `disk` for the extracted file. Archive support requires a build with `libarchive`. The engine checks that source metadata remained stable during preparation. It opens the complete file and validates every distinct record against the existing schema before publishing a generation. It does not keep the whole file in a RAM buffer.

The cache belongs to the table's active generations. Lookups reuse the mapped file without network requests. Older cache files are removed after their last reader releases the generation. Detached tables and server restarts reload remote files; this cache does not deduplicate downloads across tables or persist validated cache entries across restarts.

Publication replaces a shared generation pointer. Queries already using an older generation continue safely on that mapping; new queries acquire the new generation. Lookup threads do not share a global lookup mutex. If refresh fails, the old generation remains usable and the next scheduled check retries. The refresh error log contains an error code without remote exception bodies that could echo credentials. The initial load propagates its exception and prevents creation or attachment.

With `refresh_interval = '0'`, the table keeps its current generation. There is no dedicated `SYSTEM RELOAD` command; detach and attach the table to reopen the source when automatic refresh is disabled. Settings are fixed at table creation.

## Projection and performance {#projection-and-performance}

The engine prepares requested payload paths once per read stream or direct lookup batch and decodes only those paths. A projection such as `country.iso_code` accesses that nested MMDB value directly. Scalar strings and bytes are copied from the mapped buffer directly into `ColumnString`, and numeric values are inserted directly into typed columns. Selected arrays, maps, and tuples use `libmaxminddb`'s entry list for that subtree. Computed array subcolumns can require decoding their parent container.

For batches of at least 128 keys, bounded caches reuse decoded records and shared container subtrees within each output block. Cache entries identify MMDB offsets and rows in the result columns; collisions only cause another decode. Each stream or direct lookup batch owns its caches, and each block clears them. The lookup still resolves every requested IP independently, preserving longest-prefix matching and the original query key. Cached results are copied into the owning result columns; the caches do not retain pointers across blocks or generations.

The database mapping and scalar string views avoid intermediate payload copies. ClickHouse's result columns own their values, so producing `String` results requires a copy; this is not an entirely zero-copy result path. Schema inference and reload validation are separate from query decoding and can traverse the complete database once per preparation pass.

A reproducible benchmark can use an existing table and `clickhouse-benchmark`:

```bash
clickhouse-benchmark --iterations 1000 --query "SELECT country.iso_code FROM geoip WHERE ip = toIPv4('8.8.8.8')"
clickhouse-benchmark --iterations 1000 --query "SELECT * FROM geoip WHERE ip = toIPv4('8.8.8.8')"
clickhouse-benchmark --iterations 100 --query "SELECT country.iso_code FROM geoip WHERE ip IN (SELECT toIPv4(number + 134744064) FROM numbers(10))"
clickhouse-benchmark --iterations 100 --query "SELECT country.iso_code FROM geoip WHERE ip IN (SELECT toIPv4(number + 134744064) FROM numbers(1000))"
```

Use the same MMDB, table schema, and query settings when comparing projection sizes. Repeat with `IPv6`, a local source, and an already cached remote source. To measure reload, atomically replace the source and measure query latency while refresh validates the next generation. Creation and refresh validation cost scales with the database's search tree and distinct data records.
)DOCS_MD",
            .syntax = "ENGINE = MaxMindDB(source[, NOSIGN | access_key_id, secret_access_key[, session_token]])",
            .related = {"File", "URL", "S3", "EmbeddedRocksDB"},
        });
}
}
