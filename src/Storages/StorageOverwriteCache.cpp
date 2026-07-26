#include <Storages/StorageOverwriteCache.h>

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/StorageFactory.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <limits>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TYPE_MISMATCH;
extern const int UNKNOWN_SETTING;
}

namespace FailPoints
{
extern const char overwrite_cache_pause_before_commit[];
extern const char overwrite_cache_throw_during_publish[];
}

namespace
{

Names parseColumnList(const String & value, const String & setting_name)
{
    Names result;
    size_t begin = 0;
    while (begin <= value.size())
    {
        size_t end = value.find(',', begin);
        if (end == String::npos)
            end = value.size();

        size_t first = begin;
        while (first < end && std::isspace(static_cast<unsigned char>(value[first])))
            ++first;
        size_t last = end;
        while (last > first && std::isspace(static_cast<unsigned char>(value[last - 1])))
            --last;

        if (last > first)
            result.emplace_back(value.substr(first, last - first));
        else if (!value.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} contains an empty column name", backQuote(setting_name));

        if (end == value.size())
            break;
        begin = end + 1;
    }
    return result;
}

UInt64 getUInt64Setting(const SettingChange & change)
{
    if (change.value.getType() == Field::Types::UInt64)
        return change.value.safeGet<UInt64>();
    if (change.value.getType() == Field::Types::Int64)
    {
        const auto value = change.value.safeGet<Int64>();
        if (value >= 0)
            return static_cast<UInt64>(value);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} must be a non-negative UInt64", backQuote(change.name));
}

String getStringSetting(const SettingChange & change)
{
    if (change.value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} must be a String", backQuote(change.name));
    return change.value.safeGet<String>();
}

OverwriteCacheSettings parseSettings(const ASTStorage & storage_def)
{
    OverwriteCacheSettings result;
    if (!storage_def.settings)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage `OverwriteCache` requires setting `max_memory_bytes`");

    for (const auto & change : storage_def.settings->changes)
    {
        if (change.name == "max_memory_bytes")
            result.max_memory_bytes = getUInt64Setting(change);
        else if (change.name == "equal_version_tiebreak_columns")
            result.equal_version_tiebreak_columns = parseColumnList(getStringSetting(change), change.name);
        else if (change.name == "secondary_index_columns")
            result.secondary_index_columns = parseColumnList(getStringSetting(change), change.name);
        else if (change.name == "secondary_index_segment_column")
        {
            auto value = getStringSetting(change);
            if (!value.empty())
                result.secondary_index_segment_column = std::move(value);
        }
        else if (change.name == "max_secondary_index_rows")
            result.max_secondary_index_rows = getUInt64Setting(change);
        else
            throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting {} for storage `OverwriteCache`", backQuote(change.name));
    }

    if (!result.max_memory_bytes)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage `OverwriteCache` requires a positive `max_memory_bytes`");
    if (!result.secondary_index_columns.empty() && !result.max_secondary_index_rows)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Storage `OverwriteCache` requires a positive `max_secondary_index_rows` when `secondary_index_columns` is set");

    return result;
}

Names extractIdentifierList(const IAST & key_ast, const String & clause_name)
{
    auto list = key_ast.as<ASTExpressionList>() ? key_ast.clone() : extractKeyExpressionList(key_ast.clone());
    Names result;
    std::unordered_set<String> seen;
    for (const auto & child : list->children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        if (!identifier || identifier->compound())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "{} of storage `OverwriteCache` must contain only unqualified column identifiers", clause_name);
        const auto & name = identifier->name();
        if (!seen.emplace(name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate column {} in {}", backQuote(name), clause_name);
        result.push_back(name);
    }
    if (result.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} of storage `OverwriteCache` cannot be empty", clause_name);
    return result;
}

void validateColumn(const ColumnsDescription & columns, const String & name, const String & role, bool require_comparable)
{
    if (!columns.has(name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown {} column {} for storage `OverwriteCache`", role, backQuote(name));
    const auto & type = columns.get(name).type;
    if (require_comparable && !type->isComparable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} column {} has non-comparable type {}", role, backQuote(name), type->getName());
}

bool isOverwriteCacheSetting(std::string_view name)
{
    return name == "max_memory_bytes" || name == "equal_version_tiebreak_columns" || name == "secondary_index_columns"
        || name == "secondary_index_segment_column" || name == "max_secondary_index_rows";
}

class OverwriteCacheSink final : public SinkToStorage
{
public:
    OverwriteCacheSink(StorageOverwriteCache & storage_, const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage(storage_)
    {
    }

    String getName() const override { return "OverwriteCacheSink"; }

    void consume(Chunk & chunk) override { storage.insertBlock(getHeader().cloneWithColumns(chunk.getColumns())); }

private:
    StorageOverwriteCache & storage;
};

class OverwriteCacheSource final : public ISource
{
public:
    OverwriteCacheSource(
        SharedHeader header_, StorageOverwriteCache::RowDataPtrs rows_, std::vector<size_t> positions_, size_t max_block_size_)
        : ISource(std::move(header_))
        , rows(std::move(rows_))
        , positions(std::move(positions_))
        , max_block_size(std::max<size_t>(1, max_block_size_))
    {
    }

    String getName() const override { return "OverwriteCacheSource"; }

protected:
    Chunk generate() override
    {
        if (offset >= rows.size())
            return {};

        const size_t rows_to_emit = std::min(max_block_size, rows.size() - offset);
        const auto & header = getPort().getHeader();
        MutableColumns columns(header.columns());
        for (size_t i = 0; i < columns.size(); ++i)
            columns[i] = header.getByPosition(i).type->createColumn();

        for (size_t row = offset; row < offset + rows_to_emit; ++row)
        {
            for (size_t column = 0; column < positions.size(); ++column)
                columns[column]->insert(rows[row]->values[positions[column]]);
        }

        offset += rows_to_emit;
        return Chunk(std::move(columns), rows_to_emit);
    }

private:
    StorageOverwriteCache::RowDataPtrs rows;
    std::vector<size_t> positions;
    size_t max_block_size;
    size_t offset = 0;
};

class ReadFromOverwriteCache final : public SourceStepWithFilter
{
public:
    ReadFromOverwriteCache(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        SharedHeader sample_block_,
        const StorageOverwriteCache & storage_,
        size_t max_block_size_)
        : SourceStepWithFilter(std::move(sample_block_), column_names_, query_info_, storage_snapshot_, context_)
        , storage(storage_)
        , max_block_size(std::max<size_t>(1, max_block_size_))
    {
    }

    String getName() const override { return "ReadFromOverwriteCache"; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

        std::tie(filter_keys, all_scan)
            = getFilterKeys(storage.getKeyColumns(), storage.getKeyColumnTypes(), filter_actions_dag.get(), context);
        if (!all_scan)
        {
            read_kind = ReadKind::Primary;
            return;
        }

        if (!storage.getSecondaryIndexColumns().empty())
        {
            if (!storage.getSegmentedSecondaryIndexColumns().empty())
            {
                std::tie(filter_keys, all_scan) = getFilterKeys(
                    storage.getSegmentedSecondaryIndexColumns(),
                    storage.getSegmentedSecondaryIndexColumnTypes(),
                    filter_actions_dag.get(),
                    context);
                if (!all_scan)
                {
                    read_kind = ReadKind::SegmentedSecondary;
                    return;
                }
            }

            std::tie(filter_keys, all_scan) = getFilterKeys(
                storage.getSecondaryIndexColumns(), storage.getSecondaryIndexColumnTypes(), filter_actions_dag.get(), context);
            if (!all_scan)
                read_kind = ReadKind::Secondary;
        }
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        if (read_kind == ReadKind::None || all_scan || !filter_keys)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Storage `OverwriteCache` requires a complete `KEYS` predicate or an equality/IN predicate on all "
                "`secondary_index_columns`");

        auto iterator = filter_keys->cbegin();
        std::vector<String> serialized_keys;
        StorageOverwriteCache::RowDataPtrs rows;
        if (read_kind == ReadKind::Primary)
        {
            while (iterator != filter_keys->cend())
            {
                auto batch = serializeKeysToRawString(iterator, filter_keys->cend(), storage.getKeyColumnTypes(), max_block_size);
                serialized_keys.insert(serialized_keys.end(), std::make_move_iterator(batch.begin()), std::make_move_iterator(batch.end()));
            }
            rows = storage.getRowsForPrimaryKeys(serialized_keys);
        }
        else if (read_kind == ReadKind::Secondary)
        {
            while (iterator != filter_keys->cend())
            {
                auto batch
                    = serializeKeysToRawString(iterator, filter_keys->cend(), storage.getSecondaryIndexColumnTypes(), max_block_size);
                serialized_keys.insert(serialized_keys.end(), std::make_move_iterator(batch.begin()), std::make_move_iterator(batch.end()));
            }
            rows = storage.getRowsForSecondaryKeys(serialized_keys);
        }
        else
        {
            while (iterator != filter_keys->cend())
            {
                auto batch = serializeKeysToRawString(
                    iterator, filter_keys->cend(), storage.getSegmentedSecondaryIndexColumnTypes(), max_block_size);
                serialized_keys.insert(serialized_keys.end(), std::make_move_iterator(batch.begin()), std::make_move_iterator(batch.end()));
            }
            rows = storage.getRowsForSegmentedSecondaryKeys(serialized_keys);
        }

        std::vector<size_t> positions;
        positions.reserve(getOutputHeader()->columns());
        for (const auto & column : *getOutputHeader())
            positions.push_back(storage.getColumnPosition(column.name));

        auto source = std::make_shared<OverwriteCacheSource>(getOutputHeader(), std::move(rows), std::move(positions), max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipeline.init(Pipe(std::move(source)));
    }

    void describeActions(FormatSettings & format_settings) const override
    {
        SourceStepWithFilter::describeActions(format_settings);
        format_settings.out << format_settings.detail_prefix
                            << "ReadType: " << (read_kind == ReadKind::Primary ? "PrimaryKey" : "SecondaryIndex") << '\n';
    }

private:
    enum class ReadKind : UInt8
    {
        None,
        Primary,
        Secondary,
        SegmentedSecondary,
    };

    const StorageOverwriteCache & storage;
    size_t max_block_size;
    FieldVectorPtr filter_keys;
    bool all_scan = true;
    ReadKind read_kind = ReadKind::None;
};

}

StorageOverwriteCache::StorageOverwriteCache(
    const StorageID & table_id_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    String comment_,
    String version_column_,
    Names key_columns_,
    OverwriteCacheSettings settings_,
    ASTPtr settings_changes_)
    : IStorage(table_id_)
    , version_column(std::move(version_column_))
    , key_columns(std::move(key_columns_))
    , settings(std::move(settings_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(std::move(columns_description_));
    metadata.setConstraints(std::move(constraints_));
    metadata.setComment(std::move(comment_));
    metadata.setSettingsChanges(std::move(settings_changes_));
    setInMemoryMetadata(metadata);

    sample_block = metadata.getSampleBlock();
    serializations = sample_block.getSerializations();
    column_types = sample_block.getDataTypes();

    for (size_t position = 0; position < sample_block.columns(); ++position)
        column_positions.emplace(sample_block.getByPosition(position).name, position);

    version_position = getColumnPosition(version_column);
    for (const auto & name : key_columns)
    {
        const auto position = getColumnPosition(name);
        key_positions.push_back(position);
        key_column_types.push_back(sample_block.getByPosition(position).type);
    }
    for (const auto & name : settings.equal_version_tiebreak_columns)
        tiebreak_positions.push_back(getColumnPosition(name));
    for (const auto & name : settings.secondary_index_columns)
    {
        const auto position = getColumnPosition(name);
        secondary_index_positions.push_back(position);
        secondary_index_column_types.push_back(sample_block.getByPosition(position).type);
    }
    if (settings.secondary_index_segment_column)
    {
        segmented_secondary_index_columns.push_back(*settings.secondary_index_segment_column);
        segmented_secondary_index_columns.insert(
            segmented_secondary_index_columns.end(), settings.secondary_index_columns.begin(), settings.secondary_index_columns.end());
        for (const auto & name : segmented_secondary_index_columns)
        {
            const auto position = getColumnPosition(name);
            segmented_secondary_index_positions.push_back(position);
            segmented_secondary_index_column_types.push_back(sample_block.getByPosition(position).type);
        }
    }
}

size_t StorageOverwriteCache::getColumnPosition(const String & column_name) const
{
    const auto it = column_positions.find(column_name);
    if (it == column_positions.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown column {} in storage `OverwriteCache`", backQuote(column_name));
    return it->second;
}

String StorageOverwriteCache::serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
        serializations[position]->serializeBinary(*block.getByPosition(position).column, row, out, {});
    return out.str();
}

String StorageOverwriteCache::serializeFields(const std::vector<Field> & fields, const std::vector<size_t> & positions) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
        serializations[position]->serializeBinary(fields[position], out, {});
    return out.str();
}

int StorageOverwriteCache::compareWinner(const RowData & lhs, const RowData & rhs) const
{
    const auto compare_field = [](const Field & left, const Field & right)
    {
        if (left < right)
            return -1;
        if (right < left)
            return 1;
        return 0;
    };

    if (const int result = compare_field(lhs.values[version_position], rhs.values[version_position]))
        return result;

    for (const auto position : tiebreak_positions)
    {
        if (const int result = compare_field(lhs.values[position], rhs.values[position]))
            return result;
    }

    if (!rowsEqual(lhs, rhs))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Conflicting rows for the same `OverwriteCache` key have identical version and tie-break values");
    return 0;
}

bool StorageOverwriteCache::rowsEqual(const RowData & lhs, const RowData & rhs) const
{
    return lhs.values == rhs.values;
}

UInt64 StorageOverwriteCache::estimateRowBytes(const Block & block, size_t row, const RowData & data) const
{
    UInt64 payload_bytes = 0;
    for (const auto & column : block)
        payload_bytes += column.column->byteSizeAt(row);

    constexpr UInt64 entry_overhead = 256;
    return entry_overhead + static_cast<UInt64>(sizeof(Field) * data.values.size()) + static_cast<UInt64>(data.primary_key.size())
        + static_cast<UInt64>(data.secondary_key.size()) + static_cast<UInt64>(data.segmented_secondary_key.size()) + payload_bytes;
}

StorageOverwriteCache::ReadGuard::ReadGuard(const StorageOverwriteCache & storage_)
    : storage(storage_)
{
    while (true)
    {
        epoch = storage.active_reader_epoch.load(std::memory_order_acquire);
        storage.active_readers[epoch].fetch_add(1, std::memory_order_acq_rel);
        if (epoch == storage.active_reader_epoch.load(std::memory_order_acquire))
            break;

        if (storage.active_readers[epoch].fetch_sub(1, std::memory_order_acq_rel) == 1)
            storage.active_readers[epoch].notify_all();
    }

    snapshot_generation = storage.published_generation.load(std::memory_order_acquire);
}

StorageOverwriteCache::ReadGuard::~ReadGuard()
{
    if (storage.active_readers[epoch].fetch_sub(1, std::memory_order_acq_rel) == 1)
        storage.active_readers[epoch].notify_all();
}

size_t StorageOverwriteCache::primaryShardIndex(const String & key) const
{
    return std::hash<String>{}(key) % primary_shard_count;
}

size_t StorageOverwriteCache::postingShardIndex(const String & key) const
{
    return std::hash<String>{}(key) % posting_shard_count;
}

size_t StorageOverwriteCache::rowLockIndex(EntryId entry_id) const
{
    return std::hash<EntryId>{}(entry_id) % row_lock_count;
}

StorageOverwriteCache::EntryPtr StorageOverwriteCache::findEntry(const String & key) const
{
    const auto & shard = primary_shards[primaryShardIndex(key)];
    std::shared_lock lock(shard.mutex);
    if (const auto it = shard.entries.find(key); it != shard.entries.end())
        return it->second;
    return {};
}

StorageOverwriteCache::RowDataPtr StorageOverwriteCache::resolveEntry(const EntryPtr & entry, UInt64 snapshot_generation) const
{
    std::lock_guard lock(row_mutexes[rowLockIndex(entry->id)]);
    if (entry->pending_publication && entry->pending_publication->generation <= snapshot_generation
        && entry->pending_publication->state.load(std::memory_order_acquire) == PublicationState::Committed)
        return entry->pending;
    return entry->committed;
}

void StorageOverwriteCache::insertBlock(const Block & block)
{
    std::unordered_map<String, std::shared_ptr<RowData>> candidates;
    candidates.reserve(block.rows());

    for (size_t row = 0; row < block.rows(); ++row)
    {
        auto candidate = std::make_shared<RowData>();
        candidate->values.reserve(block.columns());
        for (const auto & column : block)
        {
            Field value;
            column.column->get(row, value);
            candidate->values.push_back(std::move(value));
        }
        candidate->primary_key = serializeColumns(block, row, key_positions);
        if (!secondary_index_positions.empty())
            candidate->secondary_key = serializeColumns(block, row, secondary_index_positions);
        if (!segmented_secondary_index_positions.empty())
            candidate->segmented_secondary_key = serializeColumns(block, row, segmented_secondary_index_positions);
        candidate->accounted_bytes = estimateRowBytes(block, row, *candidate);

        auto [it, inserted] = candidates.emplace(candidate->primary_key, candidate);
        if (!inserted && compareWinner(*candidate, *it->second) > 0)
            it->second = std::move(candidate);
    }

    struct Mutation
    {
        String key;
        EntryPtr entry;
        std::shared_ptr<RowData> row;
        bool is_new = false;
        bool primary_inserted = false;
        bool pending_installed = false;
    };

    std::lock_guard writer_lock(writer_mutex);
    UInt64 prospective_bytes = total_size_bytes.load(std::memory_order_relaxed);
    std::vector<Mutation> mutations;
    mutations.reserve(candidates.size());

    for (auto & [key, candidate] : candidates)
    {
        auto entry = findEntry(key);
        if (!entry)
        {
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - candidate->accounted_bytes)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
            prospective_bytes += candidate->accounted_bytes;
            mutations.push_back({key, {}, std::move(candidate), true});
            continue;
        }

        const auto current = resolveEntry(entry, published_generation.load(std::memory_order_acquire));
        if (!current)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Corrupted `OverwriteCache` primary index");

        const int winner = compareWinner(*candidate, *current);
        if (winner <= 0)
            continue;

        prospective_bytes -= current->accounted_bytes;
        if (prospective_bytes > std::numeric_limits<UInt64>::max() - candidate->accounted_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
        prospective_bytes += candidate->accounted_bytes;
        mutations.push_back({key, std::move(entry), std::move(candidate), false});
    }

    if (prospective_bytes > settings.max_memory_bytes)
        throw Exception(
            ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "`OverwriteCache` insert requires {} bytes, exceeding `max_memory_bytes` = {}",
            prospective_bytes,
            settings.max_memory_bytes);

    if (mutations.empty())
        return;

    const auto new_entries = static_cast<EntryId>(std::ranges::count_if(mutations, [](const auto & mutation) { return mutation.is_new; }));
    if (new_entries > std::numeric_limits<EntryId>::max() - next_entry_id)
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` entry identifier space is exhausted");

    EntryId staged_next_entry_id = next_entry_id;
    std::unordered_map<String, size_t> secondary_additions;
    std::unordered_map<String, size_t> segmented_secondary_additions;
    secondary_additions.reserve(new_entries);
    segmented_secondary_additions.reserve(new_entries);
    for (auto & mutation : mutations)
    {
        if (mutation.is_new)
        {
            mutation.entry = std::make_shared<Entry>();
            mutation.entry->id = staged_next_entry_id++;
            if (!mutation.row->secondary_key.empty())
                ++secondary_additions[mutation.row->secondary_key];
            if (!mutation.row->segmented_secondary_key.empty())
                ++segmented_secondary_additions[mutation.row->segmented_secondary_key];
        }
    }

    struct PreparedPosting
    {
        bool segmented = false;
        size_t shard_index = 0;
        String key;
        size_t additional_rows = 0;
        size_t old_size = 0;
        bool inserted = false;
        bool prepared = false;
    };

    std::vector<PreparedPosting> prepared_postings;
    prepared_postings.reserve(secondary_additions.size() + segmented_secondary_additions.size());
    for (const auto & [key, count] : secondary_additions)
        prepared_postings.push_back({false, postingShardIndex(key), key, count});
    for (const auto & [key, count] : segmented_secondary_additions)
        prepared_postings.push_back({true, postingShardIndex(key), key, count});

    const UInt64 current_generation = published_generation.load(std::memory_order_acquire);
    if (current_generation == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`OverwriteCache` publication generation space is exhausted");
    const auto publication = std::make_shared<Publication>(current_generation + 1);
    for (auto & mutation : mutations)
    {
        if (mutation.is_new)
        {
            mutation.entry->pending = mutation.row;
            mutation.entry->pending_publication = publication;
        }
    }

    try
    {
        for (auto & prepared : prepared_postings)
        {
            auto & shards = prepared.segmented ? segmented_secondary_shards : secondary_shards;
            auto & shard = shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            auto [it, inserted] = shard.postings.try_emplace(prepared.key);
            prepared.old_size = it->second.size();
            prepared.inserted = inserted;
            prepared.prepared = true;
            it->second.reserve(it->second.size() + prepared.additional_rows);
        }

        for (auto & mutation : mutations)
        {
            if (mutation.is_new)
            {
                auto & primary_shard = primary_shards[primaryShardIndex(mutation.key)];
                {
                    std::unique_lock lock(primary_shard.mutex);
                    mutation.primary_inserted = primary_shard.entries.emplace(mutation.key, mutation.entry).second;
                }
                if (!mutation.primary_inserted)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate `OverwriteCache` key during publication");

                if (!mutation.row->secondary_key.empty())
                {
                    auto & shard = secondary_shards[postingShardIndex(mutation.row->secondary_key)];
                    std::unique_lock lock(shard.mutex);
                    shard.postings.find(mutation.row->secondary_key)->second.push_back(mutation.entry);
                }
                if (!mutation.row->segmented_secondary_key.empty())
                {
                    auto & shard = segmented_secondary_shards[postingShardIndex(mutation.row->segmented_secondary_key)];
                    std::unique_lock lock(shard.mutex);
                    shard.postings.find(mutation.row->segmented_secondary_key)->second.push_back(mutation.entry);
                }
            }
            else
            {
                /// Every indexed column is part of the immutable composite key, so a replacement
                /// publishes only the new payload and retains the existing postings.
                std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry->id)]);
                if (mutation.entry->pending_publication)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Concurrent `OverwriteCache` publication for the same entry");
                mutation.entry->pending = mutation.row;
                mutation.entry->pending_publication = publication;
                mutation.pending_installed = true;
            }

            fiu_do_on(FailPoints::overwrite_cache_throw_during_publish, {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure during `OverwriteCache` publication");
            });
        }

        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_commit);
    }
    catch (...)
    {
        publication->state.store(PublicationState::Aborted, std::memory_order_release);
        for (auto & mutation : mutations)
        {
            if (!mutation.pending_installed)
                continue;
            std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry->id)]);
            mutation.entry->pending.reset();
            mutation.entry->pending_publication.reset();
        }
        for (const auto & prepared : prepared_postings)
        {
            if (!prepared.prepared)
                continue;
            auto & shards = prepared.segmented ? segmented_secondary_shards : secondary_shards;
            auto & shard = shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            if (const auto it = shard.postings.find(prepared.key); it != shard.postings.end())
            {
                it->second.resize(prepared.old_size);
                if (prepared.inserted)
                    shard.postings.erase(it);
            }
        }
        for (const auto & mutation : mutations)
        {
            if (!mutation.primary_inserted)
                continue;
            auto & shard = primary_shards[primaryShardIndex(mutation.key)];
            std::unique_lock lock(shard.mutex);
            shard.entries.erase(mutation.key);
        }
        throw;
    }

    publication->state.store(PublicationState::Committed, std::memory_order_release);
    published_generation.store(publication->generation, std::memory_order_release);
    next_entry_id = staged_next_entry_id;
    total_size_bytes.store(prospective_bytes, std::memory_order_relaxed);
    total_size_rows.fetch_add(new_entries, std::memory_order_relaxed);

    const UInt8 old_epoch = active_reader_epoch.fetch_xor(1, std::memory_order_acq_rel);
    UInt64 active = active_readers[old_epoch].load(std::memory_order_acquire);
    while (active != 0)
    {
        active_readers[old_epoch].wait(active, std::memory_order_relaxed);
        active = active_readers[old_epoch].load(std::memory_order_acquire);
    }

    for (auto & mutation : mutations)
    {
        std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry->id)]);
        mutation.entry->committed = std::move(mutation.entry->pending);
        mutation.entry->pending_publication.reset();
    }
}

StorageOverwriteCache::RowDataPtrs StorageOverwriteCache::getRowsForPrimaryKeys(const std::vector<String> & serialized_keys) const
{
    ReadGuard read_guard(*this);
    RowDataPtrs result;
    result.reserve(serialized_keys.size());
    std::unordered_set<EntryId> seen;
    for (const auto & key : serialized_keys)
    {
        const auto entry = findEntry(key);
        if (!entry || !seen.emplace(entry->id).second)
            continue;
        if (auto row = resolveEntry(entry, read_guard.generation()))
            result.push_back(std::move(row));
    }
    return result;
}

StorageOverwriteCache::RowDataPtrs StorageOverwriteCache::getRowsForPostingKeys(
    const std::vector<String> & serialized_keys,
    const std::array<PostingShard, posting_shard_count> & index,
    const char * index_name) const
{
    ReadGuard read_guard(*this);
    std::unordered_set<String> seen_keys;
    seen_keys.reserve(serialized_keys.size());
    std::vector<EntryPtr> entries;
    for (const auto & key : serialized_keys)
    {
        if (!seen_keys.emplace(key).second)
            continue;
        const auto & shard = index[postingShardIndex(key)];
        std::shared_lock lock(shard.mutex);
        if (const auto it = shard.postings.find(key); it != shard.postings.end())
        {
            const UInt64 remaining_rows
                = settings.max_secondary_index_rows - std::min<UInt64>(entries.size(), settings.max_secondary_index_rows);
            if (it->second.size() > remaining_rows)
                throw Exception(
                    ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                    "`OverwriteCache` {} lookup exceeds `max_secondary_index_rows` = {}",
                    index_name,
                    settings.max_secondary_index_rows);
            entries.insert(entries.end(), it->second.begin(), it->second.end());
        }
    }

    RowDataPtrs result;
    result.reserve(std::min<UInt64>(entries.size(), settings.max_secondary_index_rows));
    for (const auto & entry : entries)
    {
        auto row = resolveEntry(entry, read_guard.generation());
        if (!row)
            continue;
        if (result.size() >= settings.max_secondary_index_rows)
            throw Exception(
                ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                "`OverwriteCache` {} lookup exceeds `max_secondary_index_rows` = {}",
                index_name,
                settings.max_secondary_index_rows);
        result.push_back(std::move(row));
    }
    return result;
}

StorageOverwriteCache::RowDataPtrs StorageOverwriteCache::getRowsForSecondaryKeys(const std::vector<String> & serialized_keys) const
{
    return getRowsForPostingKeys(serialized_keys, secondary_shards, "secondary");
}

StorageOverwriteCache::RowDataPtrs
StorageOverwriteCache::getRowsForSegmentedSecondaryKeys(const std::vector<String> & serialized_keys) const
{
    return getRowsForPostingKeys(serialized_keys, segmented_secondary_shards, "segmented secondary");
}

Block StorageOverwriteCache::getSampleBlock(const Names & required_columns) const
{
    if (required_columns.empty())
        return sample_block;

    Block result;
    for (const auto & name : required_columns)
        result.insert(sample_block.getByName(name));
    return result;
}

Chunk StorageOverwriteCache::getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets) const
{
    if (keys.size() != key_columns.size())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage `OverwriteCache` requires {} key columns, got {}",
            key_columns.size(),
            keys.size());

    const size_t rows = keys.empty() ? 0 : keys.front().column->size();
    for (size_t index = 0; index < keys.size(); ++index)
    {
        if (keys[index].column->size() != rows)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All `OverwriteCache` key columns must have equal size");
        if (!key_column_types[index]->equals(*keys[index].type))
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Key column {} has type {}, expected {}",
                backQuote(key_columns[index]),
                keys[index].type->getName(),
                key_column_types[index]->getName());
    }

    const Block result_header = getSampleBlock(required_columns);
    MutableColumns result_columns = result_header.cloneEmptyColumns();
    std::vector<size_t> result_positions;
    result_positions.reserve(result_header.columns());
    for (const auto & column : result_header)
        result_positions.push_back(getColumnPosition(column.name));

    out_offsets.clear();
    out_null_map.clear();
    out_null_map.resize_fill(rows, 0);

    std::vector<String> serialized_keys;
    serialized_keys.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        WriteBufferFromOwnString out;
        for (size_t key_index = 0; key_index < keys.size(); ++key_index)
            key_column_types[key_index]->getDefaultSerialization()->serializeBinary(*keys[key_index].column, row, out, {});
        serialized_keys.push_back(out.str());
    }

    std::vector<RowDataPtr> resolved_rows(rows);
    {
        ReadGuard read_guard(*this);
        for (size_t row = 0; row < rows; ++row)
        {
            if (const auto entry = findEntry(serialized_keys[row]))
                resolved_rows[row] = resolveEntry(entry, read_guard.generation());
        }
    }

    for (size_t row = 0; row < rows; ++row)
    {
        if (!resolved_rows[row])
        {
            for (auto & column : result_columns)
                column->insertDefault();
            continue;
        }
        out_null_map[row] = 1;
        for (size_t column = 0; column < result_positions.size(); ++column)
            result_columns[column]->insert(resolved_rows[row]->values[result_positions[column]]);
    }

    return Chunk(std::move(result_columns), rows);
}

void StorageOverwriteCache::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t)
{
    storage_snapshot->check(column_names);
    auto sample = std::make_shared<const Block>(storage_snapshot->metadata->getSampleBlock());
    query_plan.addStep(
        std::make_unique<ReadFromOverwriteCache>(
            column_names, query_info, storage_snapshot, context, std::move(sample), *this, max_block_size));
}

SinkToStoragePtr StorageOverwriteCache::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool)
{
    return std::make_shared<OverwriteCacheSink>(*this, metadata_snapshot);
}

void StorageOverwriteCache::clearData()
{
    std::lock_guard writer_lock(writer_mutex);
    for (auto & shard : secondary_shards)
    {
        std::unique_lock lock(shard.mutex);
        shard.postings.clear();
    }
    for (auto & shard : segmented_secondary_shards)
    {
        std::unique_lock lock(shard.mutex);
        shard.postings.clear();
    }
    for (auto & shard : primary_shards)
    {
        std::unique_lock lock(shard.mutex);
        shard.entries.clear();
    }
    next_entry_id = 1;
    published_generation.store(0, std::memory_order_release);
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

void StorageOverwriteCache::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    clearData();
}

void StorageOverwriteCache::drop()
{
    clearData();
}

std::optional<UInt64> StorageOverwriteCache::totalRows(ContextPtr) const
{
    return total_size_rows.load(std::memory_order_relaxed);
}

std::optional<UInt64> StorageOverwriteCache::totalBytes(ContextPtr) const
{
    return total_size_bytes.load(std::memory_order_relaxed);
}

void registerStorageOverwriteCache(StorageFactory & factory)
{
    factory.registerStorage(
        "OverwriteCache",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            if (args.engine_args.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine `OverwriteCache` requires exactly one version-column argument");

            const auto * version_identifier = args.engine_args[0]->as<ASTIdentifier>();
            if (!version_identifier || version_identifier->compound())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "The argument of engine `OverwriteCache` must be an unqualified version-column identifier");
            const String version_column = version_identifier->name();

            if (!args.storage_def->keys)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `OverwriteCache` requires a `KEYS (...)` clause");
            Names key_columns = extractIdentifierList(*args.storage_def->keys, "`KEYS` clause");
            OverwriteCacheSettings settings = parseSettings(*args.storage_def);

            validateColumn(args.columns, version_column, "version", true);
            std::unordered_set<String> key_set;
            for (const auto & key : key_columns)
            {
                validateColumn(args.columns, key, "key", true);
                key_set.emplace(key);
            }
            if (key_set.contains(version_column))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Version column {} cannot be declared in `KEYS`", backQuote(version_column));

            std::unordered_set<String> tiebreak_set;
            for (const auto & column : settings.equal_version_tiebreak_columns)
            {
                validateColumn(args.columns, column, "tie-break", true);
                if (!tiebreak_set.emplace(column).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate tie-break column {}", backQuote(column));
            }

            std::unordered_set<String> secondary_set;
            for (const auto & column : settings.secondary_index_columns)
            {
                if (!key_set.contains(column))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Secondary-index column {} must be declared in `KEYS`", backQuote(column));
                if (!secondary_set.emplace(column).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate secondary-index column {}", backQuote(column));
            }

            if (settings.secondary_index_segment_column)
            {
                const auto & segment = *settings.secondary_index_segment_column;
                if (settings.secondary_index_columns.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Setting `secondary_index_segment_column` requires `secondary_index_columns`");
                if (!key_set.contains(segment))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Secondary-index segment column {} must be declared in `KEYS`", backQuote(segment));
                if (secondary_set.contains(segment))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Secondary-index segment column {} cannot also be a secondary-index column",
                        backQuote(segment));
            }

            return std::make_shared<StorageOverwriteCache>(
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                version_column,
                std::move(key_columns),
                std::move(settings),
                args.storage_def->settings->clone());
        },
        {
            .supports_settings = true,
            .supports_keys = true,
            .has_builtin_setting_fn = isOverwriteCacheSetting,
        });
}

}
