#include <Storages/MaxMindDB/StorageMaxMindDB.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
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
`MaxMindDB` is an experimental read-only table engine for local MaxMind DB files. It uses `libmaxminddb` to perform binary longest-prefix IP lookups from a memory-mapped database without importing records into ClickHouse.

```sql
SET allow_experimental_maxminddb_table_engine = 1;

CREATE TABLE geoip ENGINE = MaxMindDB('GeoLite2-City.mmdb')
SETTINGS refresh_interval = '5m';

SELECT country.iso_code FROM geoip WHERE ip = toIPv4('8.8.8.8');
```

The engine accepts manual schemas or infers types from MMDB records. The `ip` column must be IPv4 or IPv6, and bounded `ip = ...` or `ip IN (...)` predicates are required. Unbounded scans are unsupported. Local sources require READ ON FILE and must be inside the server's `user_files_path`.

`refresh_interval` controls checks for local file replacements (`'0'` disables checking). Updates should atomically replace the source. A new generation must validate against the persisted schema before publication.
)DOCS_MD",
            .syntax = "ENGINE = MaxMindDB(local_mmdb_path)",
            .related = {"File", "EmbeddedRocksDB"},
        });
}
}
