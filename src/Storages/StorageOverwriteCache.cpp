#include <Storages/StorageOverwriteCache.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Block.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <limits>
#include <utility>

namespace ProfileEvents
{
extern const Event OverwriteCacheEqualVersionTies;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_RESTORE_TABLE;
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int QUERY_WAS_CANCELLED;
extern const int TYPE_MISMATCH;
extern const int UNKNOWN_SETTING;
}

namespace FailPoints
{
extern const char overwrite_cache_pause_after_lookup_catalog_snapshot[];
extern const char overwrite_cache_pause_after_lookup_ids[];
extern const char overwrite_cache_pause_after_drop_index_publication[];
extern const char overwrite_cache_pause_before_rollback[];
extern const char overwrite_cache_pause_before_commit[];
extern const char overwrite_cache_pause_during_index_build[];
extern const char overwrite_cache_pause_during_lookup[];
extern const char overwrite_cache_throw_during_index_build[];
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
    result.max_memory_bytes = std::numeric_limits<UInt64>::max();
    if (!storage_def.settings)
        return result;

    for (const auto & change : storage_def.settings->changes)
    {
        if (change.name == "max_memory_bytes")
            result.max_memory_bytes = getUInt64Setting(change);
        else if (change.name == "equal_version_tiebreak_columns")
            result.equal_version_tiebreak_columns = parseColumnList(getStringSetting(change), change.name);
        else if (change.name == "compress_segments")
            result.compress_segments = getUInt64Setting(change) != 0;
        else if (change.name == "persist_mode")
            result.persist_mode = parseOverwriteCachePersistMode(getStringSetting(change));
        else if (change.name == "disk")
            result.disk_name = getStringSetting(change);
        else
            throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting {} for storage `OverwriteCache`", backQuote(change.name));
    }

    if (!result.max_memory_bytes)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage `OverwriteCache` requires a positive `max_memory_bytes`");
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

ASTPtr makeLookupIndexAST(const Names & columns)
{
    auto result = make_intrusive<ASTExpressionList>();
    result->children.reserve(columns.size());
    for (const auto & column : columns)
        result->children.push_back(make_intrusive<ASTIdentifier>(column));
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
    return name == "max_memory_bytes" || name == "equal_version_tiebreak_columns" || name == "compress_segments"
        || name == "persist_mode" || name == "disk";
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
        SharedHeader header_,
        StorageOverwriteCache::ReadResult result_,
        std::vector<size_t> positions_,
        size_t max_block_size_)
        : ISource(std::move(header_))
        , read_guard(std::move(result_.guard))
        , rows(std::move(result_.rows))
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

        for (size_t output_position = 0; output_position < positions.size(); ++output_position)
        {
            for (size_t row = offset; row < offset + rows_to_emit; ++row)
            {
                const auto & source_column = segment_columns.get(*rows[row].segment, positions[output_position]);
                columns[output_position]->insertFrom(source_column, rows[row].segment_row);
            }
        }

        offset += rows_to_emit;
        return Chunk(std::move(columns), rows_to_emit);
    }

private:
    /// Keep the guard before rows so rows release their segment references before the epoch is released.
    StorageOverwriteCache::ReadGuardPtr read_guard;
    StorageOverwriteCache::RowDataPtrs rows;
    StorageOverwriteCache::SegmentColumnCache segment_columns;
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

        /// A mutation hands its predicate over through `SelectQueryInfo` rather than through a `FilterStep`
        /// the optimizer could push down, and when the predicate contains a subquery the resulting
        /// `DelayedCreatingSetsStep` stops the push-down walk before it reaches that step at all.
        /// `ReadFromMergeTree` reconciles the same two sources of the predicate for the same reason.
        const ActionsDAG * filter_dag
            = filter_actions_dag ? filter_actions_dag.get() : query_info.filter_actions_dag.get();

        std::tie(filter_keys, all_scan)
            = getFilterKeys(storage.getKeyColumns(), storage.getKeyColumnTypes(), filter_dag, context);
        if (!all_scan)
        {
            read_kind = ReadKind::Primary;
            return;
        }

        auto indexes = storage.getLookupIndexSnapshot();
        for (auto & index : indexes)
        {
            auto [keys, requires_scan]
                = getFilterKeys(index.columns, index.types, filter_dag, context);
            if (!requires_scan)
            {
                lookup_filters.push_back({std::move(index), std::move(keys)});
                all_scan = false;
            }
        }
        std::ranges::stable_sort(lookup_filters, [&](const LookupFilter & lhs, const LookupFilter & rhs)
        {
            return lhs.index.columns.size() > rhs.index.columns.size();
        });
        std::unordered_set<String> covered_columns;
        std::erase_if(lookup_filters, [&](const LookupFilter & filter)
        {
            bool contributes_column = false;
            for (const auto & column : filter.index.columns)
                contributes_column |= covered_columns.emplace(column).second;
            return !contributes_column;
        });
        if (!all_scan)
            read_kind = ReadKind::Lookup;
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        if (read_kind == ReadKind::None || all_scan || (read_kind == ReadKind::Primary && !filter_keys)
            || (read_kind == ReadKind::Lookup && lookup_filters.empty()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Storage `OverwriteCache` requires a complete `KEYS` predicate or an equality/IN predicate on a declared "
                "lookup `INDEX`");

        StorageOverwriteCache::ReadResult result;
        if (read_kind == ReadKind::Primary)
        {
            auto iterator = filter_keys->cbegin();
            std::vector<String> serialized_keys;
            while (iterator != filter_keys->cend())
            {
                auto batch = serializeKeysToRawString(iterator, filter_keys->cend(), storage.getKeyColumnTypes(), max_block_size);
                serialized_keys.insert(serialized_keys.end(), std::make_move_iterator(batch.begin()), std::make_move_iterator(batch.end()));
            }
            result = storage.getRowsForPrimaryKeys(serialized_keys);
        }
        else
        {
            std::vector<StorageOverwriteCache::LookupRequest> requests;
            requests.reserve(lookup_filters.size());
            for (const auto & lookup_filter : lookup_filters)
            {
                auto iterator = lookup_filter.keys->cbegin();
                std::vector<String> serialized_keys;
                while (iterator != lookup_filter.keys->cend())
                {
                    auto batch = serializeKeysToRawString(
                        iterator, lookup_filter.keys->cend(), lookup_filter.index.types, max_block_size);
                    serialized_keys.insert(
                        serialized_keys.end(), std::make_move_iterator(batch.begin()), std::make_move_iterator(batch.end()));
                }
                requests.push_back(
                    {lookup_filter.index.guard, lookup_filter.index.index, std::move(serialized_keys)});
            }
            result = storage.getRowsForLookupRequests(requests);
        }

        std::vector<size_t> positions;
        positions.reserve(getOutputHeader()->columns());
        for (const auto & column : *getOutputHeader())
            positions.push_back(storage.getColumnPosition(column.name));

        auto source = std::make_shared<OverwriteCacheSource>(
            getOutputHeader(), std::move(result), std::move(positions), max_block_size);
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
        Lookup,
    };

    struct LookupFilter
    {
        StorageOverwriteCache::LookupIndexSnapshot index;
        FieldVectorPtr keys;
    };

    const StorageOverwriteCache & storage;
    size_t max_block_size;
    FieldVectorPtr filter_keys;
    std::vector<LookupFilter> lookup_filters;
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
    std::vector<Names> lookup_indexes_,
    ASTPtr lookup_indexes_ast_,
    OverwriteCacheSettings settings_,
    ASTPtr settings_changes_,
    DiskPtr disk_,
    const String & relative_data_path_)
    : IStorage(table_id_)
    , version_column(std::move(version_column_))
    , key_columns(std::move(key_columns_))
    , lookup_index_columns(std::move(lookup_indexes_))
    , settings(std::move(settings_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(std::move(columns_description_));
    metadata.setConstraints(std::move(constraints_));
    metadata.setComment(std::move(comment_));
    metadata.setSettingsChanges(std::move(settings_changes_));
    metadata.lookup_indexes = std::move(lookup_indexes_ast_);
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

    keep_uncompressed.assign(sample_block.columns(), !settings.compress_segments);
    keep_uncompressed[version_position] = true;
    for (const auto position : tiebreak_positions)
        keep_uncompressed[position] = true;

    lookup_index_positions.reserve(lookup_index_columns.size());
    lookup_index_column_types.reserve(lookup_index_columns.size());
    lookup_indexes.reserve(lookup_index_columns.size());
    for (const auto & index_columns : lookup_index_columns)
    {
        std::vector<size_t> positions;
        DataTypes types;
        positions.reserve(index_columns.size());
        types.reserve(index_columns.size());
        for (const auto & name : index_columns)
        {
            const auto position = getColumnPosition(name);
            positions.push_back(position);
            types.push_back(sample_block.getByPosition(position).type);
        }
        lookup_index_positions.push_back(std::move(positions));
        lookup_index_column_types.push_back(std::move(types));
        lookup_indexes.push_back(std::make_shared<LookupIndex>());
    }

    persistence = std::make_shared<OverwriteCachePersistence>(
        settings.persist_mode,
        std::move(disk_),
        relative_data_path_,
        sample_block,
        getPersistenceFingerprint(),
        fmt::format("StorageOverwriteCache ({})", table_id_.getNameForLogs()));
    loadPersistedData();
}

String StorageOverwriteCache::getPersistenceFingerprint() const
{
    WriteBufferFromOwnString out;
    for (const auto & column : sample_block)
    {
        writeString(column.name, out);
        writeChar(' ', out);
        writeString(column.type->getName(), out);
        writeChar('\n', out);
    }
    writeString("KEYS ", out);
    for (const auto & name : key_columns)
    {
        writeString(name, out);
        writeChar(',', out);
    }
    writeString("\nVERSION ", out);
    writeString(version_column, out);
    writeString("\nTIEBREAK ", out);
    for (const auto & name : settings.equal_version_tiebreak_columns)
    {
        writeString(name, out);
        writeChar(',', out);
    }
    /// Lookup indexes are deliberately absent: they are rebuilt from the segments, so `ALTER ADD INDEX`
    /// must not invalidate a log.
    return out.str();
}

void StorageOverwriteCache::loadPersistedData()
{
    if (!persistence->isEnabled())
        return;

    loading = true;
    try
    {
        persistence->load([&](OverwriteCachePersistence::LoadedRecord && record)
        {
            if (!record.deleted_keys.empty())
                deleteKeys(record.deleted_keys);
            else
                insertBlock(record.block, record.segment_id);
        });
    }
    catch (...)
    {
        loading = false;
        throw;
    }
    loading = false;

    persistence->start();

    /// A file whose every row lost to a later one contributes nothing, so the log stops referring to it.
    if (!retired_during_load.empty())
    {
        OverwriteCachePersistence::Commit commit;
        commit.generation = published_generation.load(std::memory_order_relaxed);
        commit.removed = std::move(retired_during_load);
        persistence->enqueue(std::move(commit));
        retired_during_load.clear();
    }
}

std::vector<StorageOverwriteCache::LookupIndexSnapshot> StorageOverwriteCache::getLookupIndexSnapshot() const
{
    auto guard = std::make_shared<ReadGuard>(*this);
    std::shared_lock lock(lookup_catalog_mutex);
    std::vector<LookupIndexSnapshot> result;
    result.reserve(lookup_indexes.size());
    for (size_t index = 0; index < lookup_indexes.size(); ++index)
        result.push_back({guard, lookup_index_columns[index], lookup_index_column_types[index], lookup_indexes[index]});
    return result;
}

size_t StorageOverwriteCache::getColumnPosition(const String & column_name) const
{
    const auto it = column_positions.find(column_name);
    if (it == column_positions.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown column {} in storage `OverwriteCache`", backQuote(column_name));
    return it->second;
}

void StorageOverwriteCache::serializeKeys(const Block & block, const std::vector<size_t> & positions, SerializedKeys & result) const
{
    const size_t rows = block.rows();
    result.offsets.resize(rows);
    result.hashes.resize(rows);
    {
        WriteBufferFromVector<PODArray<char>> out(result.data);
        for (size_t row = 0; row < rows; ++row)
        {
            for (const auto position : positions)
                serializations[position]->serializeBinary(*block.getByPosition(position).column, row, out, format_settings);
            result.offsets[row] = out.count();
        }
        out.finalize();
    }
    for (size_t row = 0; row < rows; ++row)
        result.hashes[row] = StringViewHash{}(result.at(row));
}

String StorageOverwriteCache::serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
        serializations[position]->serializeBinary(*block.getByPosition(position).column, row, out, format_settings);
    return out.str();
}

String StorageOverwriteCache::serializeRowColumns(
    const RowData & row, const std::vector<size_t> & positions, SegmentColumnCache & cache) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
        serializations[position]->serializeBinary(cache.get(*row.segment, position), row.segment_row, out, format_settings);
    return out.str();
}

void StorageOverwriteCache::insertValueIntoColumn(
    const RowData & row, size_t position, IColumn & column, SegmentColumnCache & cache) const
{
    column.insertFrom(cache.get(*row.segment, position), row.segment_row);
}

int StorageOverwriteCache::compareWinner(const Block & block, size_t lhs_row, size_t rhs_row) const
{
    const auto & version = *block.getByPosition(version_position).column;
    if (const int result = version.compareAt(lhs_row, rhs_row, version, 1))
        return result;

    for (const auto position : tiebreak_positions)
    {
        const auto & column = *block.getByPosition(position).column;
        if (const int result = column.compareAt(lhs_row, rhs_row, column, 1))
            return result;
    }

    return 0;
}

int StorageOverwriteCache::compareWinner(const Block & block, size_t lhs_row, const RowData & rhs) const
{
    /// The version and tie-break columns of a segment are never compressed, so they can be compared in place.
    if (const int result = block.getByPosition(version_position).column->compareAt(
            lhs_row, rhs.segment_row, *rhs.segment->columns[version_position], 1))
        return result;

    for (const auto position : tiebreak_positions)
    {
        if (const int result = block.getByPosition(position).column->compareAt(
                lhs_row, rhs.segment_row, *rhs.segment->columns[position], 1))
            return result;
    }

    return 0;
}

void StorageOverwriteCache::EntryTable::grow(size_t new_size)
{
    while (allocated_entries < new_size)
    {
        if (chunk_count == max_chunks)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` entry table is exhausted");
        const size_t capacity = chunkCapacity(chunk_count);
        chunks[chunk_count].store(new Entry[capacity], std::memory_order_release);
        ++chunk_count;
        allocated_entries += capacity;
    }
    entry_count.store(new_size, std::memory_order_release);
}

void StorageOverwriteCache::EntryTable::clear()
{
    entry_count.store(0, std::memory_order_release);
    for (size_t level = 0; level < chunk_count; ++level)
    {
        delete[] chunks[level].load(std::memory_order_relaxed);
        chunks[level].store(nullptr, std::memory_order_release);
    }
    chunk_count = 0;
    allocated_entries = 0;
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

    /// Registering the snapshot under the same lock a writer uses to compute the pruning watermark
    /// keeps a writer from dropping a version this guard is about to rely on.
    std::lock_guard registry_lock(storage.snapshot_registry_mutex);
    snapshot_generation = storage.published_generation.load(std::memory_order_acquire);
    ++storage.live_snapshots[snapshot_generation];
}

StorageOverwriteCache::ReadGuard::~ReadGuard()
{
    {
        std::lock_guard registry_lock(storage.snapshot_registry_mutex);
        const auto it = storage.live_snapshots.find(snapshot_generation);
        if (it != storage.live_snapshots.end() && --it->second == 0)
            storage.live_snapshots.erase(it);
    }

    if (storage.active_readers[epoch].fetch_sub(1, std::memory_order_acq_rel) == 1)
        storage.active_readers[epoch].notify_all();
}

UInt64 StorageOverwriteCache::oldestLiveGeneration() const
{
    std::lock_guard registry_lock(snapshot_registry_mutex);
    if (live_snapshots.empty())
        return published_generation.load(std::memory_order_acquire);
    return live_snapshots.begin()->first;
}

std::unique_ptr<StorageOverwriteCache::EntryVersion> StorageOverwriteCache::takeVersion()
{
    if (!recycled_versions)
        return std::make_unique<EntryVersion>();
    auto version = std::move(recycled_versions);
    recycled_versions = std::move(version->older);
    --recycled_version_count;
    return version;
}

void StorageOverwriteCache::recycleVersions(std::unique_ptr<EntryVersion> chain)
{
    while (chain)
    {
        auto version = std::move(chain);
        chain = std::move(version->older);
        if (recycled_version_count >= max_recycled_versions)
            continue;
        /// Drop the row so a recycled version stops keeping its segment alive.
        version->row = RowData{};
        version->generation = 0;
        version->older = std::move(recycled_versions);
        recycled_versions = std::move(version);
        ++recycled_version_count;
    }
}

void StorageOverwriteCache::drainReaders()
{
    const UInt8 old_epoch = active_reader_epoch.fetch_xor(1, std::memory_order_acq_rel);
    UInt64 active = active_readers[old_epoch].load(std::memory_order_acquire);
    while (active != 0)
    {
        active_readers[old_epoch].wait(active, std::memory_order_relaxed);
        active = active_readers[old_epoch].load(std::memory_order_acquire);
    }
}

size_t StorageOverwriteCache::rowLockIndex(EntryId entry_id) const
{
    return std::hash<EntryId>{}(entry_id) % row_lock_count;
}

std::optional<StorageOverwriteCache::EntryId> StorageOverwriteCache::findEntry(std::string_view key, size_t hash) const
{
    const auto & shard = primary_shards[shardIndex(hash)];
    std::shared_lock lock(shard.mutex);
    if (const auto * it = shard.entries.find(key, hash))
        return it->getMapped();
    return {};
}

StorageOverwriteCache::RowDataPtr StorageOverwriteCache::resolveEntry(EntryId entry_id, UInt64 snapshot_generation) const
{
    if (entry_id == 0 || entry_id > entries.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` entry identifier");
    std::lock_guard row_lock(row_mutexes[rowLockIndex(entry_id)]);
    for (const auto * version = entries.at(entry_id).head.get(); version; version = version->older.get())
    {
        if (version->generation > snapshot_generation)
            continue;
        /// A version without a segment is the tombstone left by `DELETE`. The key is gone as of this
        /// snapshot, so older versions must not be consulted.
        if (!version->row.segment)
            return {};
        return version->row;
    }
    return {};
}

void StorageOverwriteCache::insertBlock(const Block & input_block)
{
    insertBlock(input_block, 0);
}

void StorageOverwriteCache::insertBlock(const Block & input_block, UInt64 replay_segment_id)
{
    const size_t input_rows = input_block.rows();
    if (input_rows == 0)
        return;

    /// Normalize column representations once, so that every later comparison between an input row and a
    /// stored row is between columns of the same implementation.
    Block block = input_block;
    for (auto & element : block)
        element.column = recursiveRemoveSparse(element.column->convertToFullColumnIfConst());

    std::vector<std::vector<size_t>> lookup_positions_snapshot;
    {
        std::shared_lock catalog_lock(lookup_catalog_mutex);
        lookup_positions_snapshot = lookup_index_positions;
    }

    SerializedKeys primary_keys;
    serializeKeys(block, key_positions, primary_keys);

    std::vector<SerializedKeys> lookup_keys;
    const auto build_lookup_keys = [&]
    {
        std::vector<SerializedKeys> result(lookup_positions_snapshot.size());
        for (size_t index = 0; index < lookup_positions_snapshot.size(); ++index)
            serializeKeys(block, lookup_positions_snapshot[index], result[index]);
        lookup_keys = std::move(result);
    };
    build_lookup_keys();

    /// Deduplicate inside the block. The map holds the winning source row per key; keys reference the
    /// single serialization buffer, so no per-row allocation happens here.
    using CandidateMap = HashMapWithSavedHash<std::string_view, UInt32, StringViewHash>;
    CandidateMap candidates;
    candidates.reserve(input_rows);
    /// A row that ties on version and tie-break loses to the row already chosen. That is a silent
    /// resolution of an ambiguity in the data, so it is counted for the user to notice.
    size_t equal_version_ties = 0;
    for (size_t row = 0; row < input_rows; ++row)
    {
        CandidateMap::LookupResult it = nullptr;
        bool inserted = false;
        candidates.emplace(primary_keys.at(row), it, inserted, primary_keys.hashes[row]);
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(row);
            continue;
        }

        const int comparison = compareWinner(block, row, it->getMapped());
        if (comparison > 0)
            it->getMapped() = static_cast<UInt32>(row);
        else if (comparison == 0)
            ++equal_version_ties;
    }

    struct Mutation
    {
        std::string_view key;
        size_t key_hash = 0;
        UInt32 source_row = 0;
        EntryId entry_id = 0;
        std::unique_ptr<RowData> row;
        std::optional<RowData> previous;
        bool is_new = false;
        /// A key whose entry is still published but currently tombstoned by `DELETE`. It needs a new
        /// version like any replacement, but no primary-index slot and no posting: it already has both.
        bool is_resurrected = false;
        bool primary_inserted = false;
        bool version_installed = false;
    };

    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_after_lookup_catalog_snapshot);
    std::unique_lock writer_lock(writer_mutex);
    if (lookup_positions_snapshot != lookup_index_positions)
    {
        lookup_positions_snapshot = lookup_index_positions;
        build_lookup_keys();
    }
    const size_t index_count = lookup_positions_snapshot.size();

    std::vector<Mutation> mutations;
    mutations.reserve(candidates.size());

    const UInt64 snapshot_generation = published_generation.load(std::memory_order_acquire);
    for (const auto & candidate : candidates)
    {
        Mutation mutation;
        mutation.source_row = candidate.getMapped();
        mutation.key = primary_keys.at(mutation.source_row);
        mutation.key_hash = primary_keys.hashes[mutation.source_row];

        const auto entry = findEntry(mutation.key, mutation.key_hash);
        if (!entry)
        {
            mutation.is_new = true;
            mutations.push_back(std::move(mutation));
            continue;
        }

        const auto current = resolveEntry(*entry, snapshot_generation);
        if (!current)
        {
            /// The key was deleted. Nothing is left to compare against, so the row wins outright and is
            /// resurrected in place of the tombstone.
            mutation.entry_id = *entry;
            mutation.is_resurrected = true;
            mutations.push_back(std::move(mutation));
            continue;
        }

        const int comparison = compareWinner(block, mutation.source_row, *current);
        if (comparison <= 0)
        {
            if (comparison == 0)
                ++equal_version_ties;
            continue;
        }

        mutation.entry_id = *entry;
        mutation.previous = current;
        mutations.push_back(std::move(mutation));
    }

    if (equal_version_ties)
        ProfileEvents::increment(ProfileEvents::OverwriteCacheEqualVersionTies, equal_version_ties);

    if (mutations.empty())
        return;

    const size_t block_mutation_count = mutations.size();

    auto selector = ColumnUInt64::create();
    selector->reserve(block_mutation_count);
    for (const auto & mutation : mutations)
        selector->insertValue(mutation.source_row);

    auto segment = std::make_shared<RowSegment>();
    segment->columns.reserve(block.columns());
    for (size_t position = 0; position < block.columns(); ++position)
    {
        auto selected = block.getByPosition(position).column->index(*selector, 0);
        if (!keep_uncompressed[position])
            selected = selected->compress(/*force_compression=*/true);
        segment->allocated_bytes += selected->allocatedBytes();
        segment->columns.push_back(std::move(selected));
    }
    segment->live_rows.store(block_mutation_count, std::memory_order_relaxed);
    segment->entry_ids.resize(block_mutation_count);
    if (persistence->isEnabled())
        segment->persistent_id = replay_segment_id ? replay_segment_id : persistence->allocateSegmentId();

    UInt64 prospective_bytes = total_size_bytes.load(std::memory_order_relaxed);
    if (prospective_bytes > std::numeric_limits<UInt64>::max() - segment->allocated_bytes)
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
    prospective_bytes += segment->allocated_bytes;
    for (size_t row = 0; row < block_mutation_count; ++row)
    {
        auto compact_row = std::make_unique<RowData>();
        compact_row->segment = segment;
        compact_row->segment_row = static_cast<UInt32>(row);
        mutations[row].row = std::move(compact_row);
    }

    /// Every segment this publication creates, so that it can be recorded once the publication succeeds.
    std::vector<std::shared_ptr<RowSegment>> added_segments;
    added_segments.push_back(segment);

    struct SegmentCompaction
    {
        std::shared_ptr<RowSegment> source;
        UInt64 replaced_rows = 0;
        UInt64 projected_live_rows = 0;
        bool selected = false;
        std::vector<std::pair<EntryId, RowData>> live_entries;
    };

    std::vector<SegmentCompaction> segment_compactions;
    std::unordered_map<RowSegment *, size_t> segment_compaction_positions;
    std::vector<EntryId> mutated_entry_ids;
    mutated_entry_ids.reserve(block_mutation_count);
    for (const auto & mutation : mutations)
    {
        if (mutation.is_new || !mutation.previous || !mutation.previous->segment)
            continue;
        mutated_entry_ids.push_back(mutation.entry_id);
        auto [it, inserted] = segment_compaction_positions.emplace(
            mutation.previous->segment.get(), segment_compactions.size());
        if (inserted)
        {
            segment_compactions.emplace_back();
            segment_compactions.back().source = mutation.previous->segment;
        }
        ++segment_compactions[it->second].replaced_rows;
    }
    std::ranges::sort(mutated_entry_ids);

    for (auto & compaction : segment_compactions)
    {
        const UInt64 live_rows = compaction.source->live_rows.load(std::memory_order_acquire);
        if (compaction.replaced_rows > live_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment live-row count");
        compaction.projected_live_rows = live_rows - compaction.replaced_rows;
        const UInt64 total_rows = compaction.source->entry_ids.size();
        if (compaction.projected_live_rows > total_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment size");
        const UInt64 projected_dead_rows = total_rows - compaction.projected_live_rows;
        if (compaction.projected_live_rows == 0 || projected_dead_rows < (total_rows + 1) / 2)
            continue;
        /// Replay must not rewrite a segment: the log is not being written yet, so the rewritten segment
        /// would have no file while the segment it replaced still has one.
        if (loading)
            continue;
        compaction.selected = true;
        compaction.live_entries.reserve(compaction.projected_live_rows);

        /// Segments carry a back-reference per row, so collecting survivors costs one pass over the
        /// segment instead of a scan of every entry in the table.
        for (size_t row = 0; row < compaction.source->entry_ids.size(); ++row)
        {
            if ((row & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while compacting `OverwriteCache` row segments");
            const EntryId entry_id = compaction.source->entry_ids[row];
            if (std::ranges::binary_search(mutated_entry_ids, entry_id))
                continue;
            const auto & entry = entries.at(entry_id);
            std::lock_guard row_lock(row_mutexes[rowLockIndex(entry_id)]);
            if (!entry.head || entry.head->row.segment.get() != compaction.source.get()
                || entry.head->row.segment_row != row)
                continue;
            compaction.live_entries.emplace_back(entry_id, entry.head->row);
        }
    }

    for (auto & compaction : segment_compactions)
    {
        if (!compaction.selected)
            continue;
        if (compaction.live_entries.size() != compaction.projected_live_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment references");

        auto compaction_selector = ColumnUInt64::create();
        compaction_selector->reserve(compaction.live_entries.size());
        for (const auto & [entry_id, row] : compaction.live_entries)
        {
            static_cast<void>(entry_id);
            compaction_selector->insertValue(row.segment_row);
        }

        auto compacted_segment = std::make_shared<RowSegment>();
        compacted_segment->columns.reserve(compaction.source->columns.size());
        for (size_t position = 0; position < compaction.source->columns.size(); ++position)
        {
            auto selected = compaction.source->columns[position]->decompress()->index(*compaction_selector, 0);
            if (!keep_uncompressed[position])
                selected = selected->compress(/*force_compression=*/true);
            compacted_segment->allocated_bytes += selected->allocatedBytes();
            compacted_segment->columns.push_back(std::move(selected));
        }
        compacted_segment->live_rows.store(compaction.live_entries.size(), std::memory_order_relaxed);
        compacted_segment->entry_ids.reserve(compaction.live_entries.size());
        if (persistence->isEnabled())
            compacted_segment->persistent_id = persistence->allocateSegmentId();
        if (prospective_bytes > std::numeric_limits<UInt64>::max() - compacted_segment->allocated_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
        prospective_bytes += compacted_segment->allocated_bytes;
        added_segments.push_back(compacted_segment);

        for (size_t row = 0; row < compaction.live_entries.size(); ++row)
        {
            const auto & [entry_id, previous] = compaction.live_entries[row];
            auto compacted_row = std::make_unique<RowData>();
            compacted_row->segment = compacted_segment;
            compacted_row->segment_row = static_cast<UInt32>(row);
            compacted_segment->entry_ids.push_back(entry_id);
            Mutation compacted_mutation;
            compacted_mutation.entry_id = entry_id;
            compacted_mutation.row = std::move(compacted_row);
            compacted_mutation.previous = previous;
            mutations.push_back(std::move(compacted_mutation));
        }
    }

    if (prospective_bytes > settings.max_memory_bytes)
        throw Exception(
            ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "`OverwriteCache` insert requires {} bytes, exceeding `max_memory_bytes` = {}",
            prospective_bytes,
            settings.max_memory_bytes);

    const auto new_entries = static_cast<EntryId>(std::ranges::count_if(mutations, [](const auto & mutation) { return mutation.is_new; }));
    if (new_entries > std::numeric_limits<EntryId>::max() - next_entry_id)
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` entry identifier space is exhausted");

    EntryId staged_next_entry_id = next_entry_id;
    for (auto & mutation : mutations)
    {
        if (mutation.is_new)
            mutation.entry_id = staged_next_entry_id++;
    }
    for (size_t row = 0; row < block_mutation_count; ++row)
        segment->entry_ids[row] = mutations[row].entry_id;

    /// One prepared posting per distinct lookup key, plus a slot per (mutation, index) pair so that
    /// publication never re-hashes or re-looks-up a posting.
    struct PreparedPosting
    {
        size_t index = 0;
        size_t shard_index = 0;
        std::string_view key;
        size_t key_hash = 0;
        UInt32 posting_position = 0;
        UInt32 additional_rows = 0;
        UInt32 entry_id_offset = 0;
        size_t old_size = 0;
        UInt64 bytes_delta = 0;
        bool prepared = false;
    };

    std::vector<PreparedPosting> prepared_postings;
    std::vector<UInt32> posting_slots;
    {
        using PostingLookup = HashMapWithSavedHash<std::string_view, UInt32, StringViewHash>;
        std::vector<PostingLookup> posting_lookup(index_count);
        posting_slots.assign(block_mutation_count * index_count, 0);
        for (size_t position = 0; position < block_mutation_count; ++position)
        {
            if (!mutations[position].is_new)
                continue;
            for (size_t index = 0; index < index_count; ++index)
            {
                const std::string_view key = lookup_keys[index].at(mutations[position].source_row);
                const size_t hash = lookup_keys[index].hashes[mutations[position].source_row];
                PostingLookup::LookupResult it = nullptr;
                bool inserted = false;
                posting_lookup[index].emplace(key, it, inserted, hash);
                if (inserted)
                {
                    it->getMapped() = static_cast<UInt32>(prepared_postings.size());
                    prepared_postings.push_back({index, shardIndex(hash), key, hash, 0, 0, 0, 0, 0, false});
                }
                const UInt32 slot = it->getMapped();
                ++prepared_postings[slot].additional_rows;
                posting_slots[position * index_count + index] = slot;
            }
        }
    }

    /// Flatten the entry identifiers of every prepared posting into one array.
    std::vector<EntryId> posting_entry_ids;
    {
        UInt32 offset = 0;
        for (auto & prepared : prepared_postings)
        {
            prepared.entry_id_offset = offset;
            offset += prepared.additional_rows;
        }
        posting_entry_ids.resize(offset);
        std::vector<UInt32> filled(prepared_postings.size(), 0);
        for (size_t position = 0; position < block_mutation_count; ++position)
        {
            if (!mutations[position].is_new)
                continue;
            for (size_t index = 0; index < index_count; ++index)
            {
                const UInt32 slot = posting_slots[position * index_count + index];
                posting_entry_ids[prepared_postings[slot].entry_id_offset + filled[slot]++] = mutations[position].entry_id;
            }
        }
    }

    /// Group new keys by primary shard with a counting sort so each shard is locked once.
    std::vector<UInt32> primary_shard_offsets(primary_shard_count + 1, 0);
    std::vector<UInt32> primary_order(new_entries);
    for (const auto & mutation : mutations)
    {
        if (mutation.is_new)
            ++primary_shard_offsets[shardIndex(mutation.key_hash) + 1];
    }
    for (size_t shard_index = 0; shard_index < primary_shard_count; ++shard_index)
        primary_shard_offsets[shard_index + 1] += primary_shard_offsets[shard_index];
    {
        std::vector<UInt32> cursor(primary_shard_offsets.begin(), primary_shard_offsets.end() - 1);
        for (UInt32 position = 0; position < mutations.size(); ++position)
        {
            if (mutations[position].is_new)
                primary_order[cursor[shardIndex(mutations[position].key_hash)]++] = position;
        }
    }

    std::vector<std::shared_ptr<RowSegment>> retired_segments;
    retired_segments.reserve(mutations.size());

    const UInt64 current_generation = published_generation.load(std::memory_order_acquire);
    if (current_generation == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`OverwriteCache` publication generation space is exhausted");
    const UInt64 new_generation = current_generation + 1;

    const size_t old_entry_count = entries.size();
    if (next_entry_id != old_entry_count + 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` entry identifier sequence");
    const UInt64 entries_bytes_before = entries.allocatedBytes();
    entries.grow(old_entry_count + new_entries);
    const UInt64 entries_bytes_delta = entries.allocatedBytes() - entries_bytes_before;
    if (prospective_bytes > std::numeric_limits<UInt64>::max() - entries_bytes_delta)
    {
        total_size_bytes.fetch_add(entries_bytes_delta, std::memory_order_relaxed);
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
    }
    prospective_bytes += entries_bytes_delta;

    std::vector<UInt64> lookup_bytes_delta(lookup_indexes.size());
    std::vector<UInt64> primary_shard_bytes_delta(primary_shard_count);

    try
    {
        for (size_t shard_index = 0; shard_index < primary_shard_count; ++shard_index)
        {
            const UInt32 begin = primary_shard_offsets[shard_index];
            const UInt32 end = primary_shard_offsets[shard_index + 1];
            if (begin == end)
                continue;
            auto & shard = primary_shards[shard_index];
            std::unique_lock lock(shard.mutex);
            const UInt64 bytes_before = shard.entries.getBufferSizeInBytes() + shard.arena->allocatedBytes();
            shard.entries.reserve(shard.entries.size() + (end - begin));
            /// Move the key bytes into the shard arena up front, so publication itself cannot allocate.
            for (UInt32 position = begin; position < end; ++position)
            {
                auto & mutation = mutations[primary_order[position]];
                mutation.key = std::string_view(shard.arena->insert(mutation.key.data(), mutation.key.size()), mutation.key.size());
            }
            primary_shard_bytes_delta[shard_index]
                = shard.entries.getBufferSizeInBytes() + shard.arena->allocatedBytes() - bytes_before;
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - primary_shard_bytes_delta[shard_index])
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` primary-index memory accounting overflow");
            prospective_bytes += primary_shard_bytes_delta[shard_index];
        }

        for (auto & prepared : prepared_postings)
        {
            auto & shard = lookup_indexes[prepared.index]->shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            const UInt64 bytes_before = shard.index.getBufferSizeInBytes() + shard.arena->allocatedBytes();

            PostingShard::PostingMap::LookupResult it = nullptr;
            bool inserted = false;
            shard.index.emplace(ArenaKeyHolder{prepared.key, *shard.arena}, it, inserted, prepared.key_hash);
            if (inserted)
            {
                it->getMapped() = static_cast<UInt32>(shard.postings.size());
                shard.postings.emplace_back();
            }
            prepared.posting_position = it->getMapped();
            prepared.prepared = true;

            auto & posting = shard.postings[prepared.posting_position];
            prepared.old_size = posting.size();
            const UInt64 posting_bytes_before = posting.allocatedBytes();
            posting.reserve(posting.size() + prepared.additional_rows, staged_next_entry_id - 1);

            prepared.bytes_delta = shard.index.getBufferSizeInBytes() + shard.arena->allocatedBytes() - bytes_before
                + posting.allocatedBytes() - posting_bytes_before + (inserted ? sizeof(PostingShard::Posting) : 0);
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - prepared.bytes_delta)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
            prospective_bytes += prepared.bytes_delta;
            lookup_bytes_delta[prepared.index] += prepared.bytes_delta;
        }

        if (prospective_bytes > settings.max_memory_bytes)
            throw Exception(
                ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                "`OverwriteCache` insert requires {} bytes, exceeding `max_memory_bytes` = {}",
                prospective_bytes,
                settings.max_memory_bytes);

        /// Push the new versions before the keys become reachable. They carry the not-yet-published
        /// generation, so no reader can see them until `published_generation` is bumped.
        for (auto & mutation : mutations)
        {
            auto & entry = entries.at(mutation.entry_id);
            auto version = takeVersion();
            version->row = std::move(*mutation.row);
            version->generation = new_generation;
            std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
            version->older = std::move(entry.head);
            entry.head = std::move(version);
            mutation.version_installed = true;
        }

        for (size_t shard_index = 0; shard_index < primary_shard_count; ++shard_index)
        {
            const UInt32 begin = primary_shard_offsets[shard_index];
            const UInt32 end = primary_shard_offsets[shard_index + 1];
            if (begin == end)
                continue;
            auto & shard = primary_shards[shard_index];
            std::unique_lock lock(shard.mutex);
            for (UInt32 position = begin; position < end; ++position)
            {
                auto & mutation = mutations[primary_order[position]];
                PrimaryMap::LookupResult it = nullptr;
                bool inserted = false;
                shard.entries.emplace(mutation.key, it, inserted, mutation.key_hash);
                if (!inserted)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate `OverwriteCache` key during publication");
                it->getMapped() = mutation.entry_id;
                mutation.primary_inserted = true;
            }
        }

        for (const auto & prepared : prepared_postings)
        {
            auto & shard = lookup_indexes[prepared.index]->shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            auto & posting = shard.postings[prepared.posting_position];
            for (UInt32 position = 0; position < prepared.additional_rows; ++position)
                posting.push_back(posting_entry_ids[prepared.entry_id_offset + position]);
        }

        fiu_do_on(FailPoints::overwrite_cache_throw_during_publish, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure during `OverwriteCache` publication");
        });

        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_commit);
    }
    catch (...)
    {
        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_rollback);
        /// Hash-table buffers and arena pages cannot shrink, so whatever was reserved stays accounted.
        UInt64 retained_bytes = entries_bytes_delta;
        for (const UInt64 bucket_delta : primary_shard_bytes_delta)
            retained_bytes += bucket_delta;
        for (auto & mutation : mutations)
        {
            if (!mutation.version_installed)
                continue;
            auto & entry = entries.at(mutation.entry_id);
            std::unique_ptr<EntryVersion> discarded;
            {
                std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
                discarded = std::move(entry.head);
                entry.head = std::move(discarded->older);
            }
            discarded->older.reset();
            recycleVersions(std::move(discarded));
        }
        for (const auto & prepared : prepared_postings)
        {
            if (!prepared.prepared)
                continue;
            auto & shard = lookup_indexes[prepared.index]->shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            /// An empty posting left behind stays correct: it is reachable but contributes no rows.
            shard.postings[prepared.posting_position].resize(prepared.old_size);
            retained_bytes += prepared.bytes_delta;
        }
        for (const auto & mutation : mutations)
        {
            if (!mutation.primary_inserted)
                continue;
            auto & shard = primary_shards[shardIndex(mutation.key_hash)];
            std::unique_lock lock(shard.mutex);
            shard.entries.erase(mutation.key, mutation.key_hash);
        }

        total_size_bytes.fetch_add(retained_bytes, std::memory_order_relaxed);
        for (size_t index = 0; index < lookup_bytes_delta.size(); ++index)
            lookup_indexes[index]->accounted_bytes.fetch_add(lookup_bytes_delta[index], std::memory_order_relaxed);

        /// The slots staged for this block keep their identifiers: a reader may have copied one from a
        /// posting before rollback removed it, and an empty slot resolves to no row. Reusing them would
        /// need a wait for readers, which is exactly what publication must never do.
        next_entry_id = staged_next_entry_id;
        throw;
    }

    const auto resurrected_entries
        = static_cast<UInt64>(std::ranges::count_if(mutations, [](const auto & mutation) { return mutation.is_resurrected; }));

    published_generation.store(new_generation, std::memory_order_release);
    next_entry_id = staged_next_entry_id;
    total_size_bytes.store(prospective_bytes, std::memory_order_relaxed);
    total_size_rows.fetch_add(new_entries + resurrected_entries, std::memory_order_relaxed);
    for (size_t index = 0; index < lookup_indexes.size(); ++index)
        lookup_indexes[index]->accounted_bytes.fetch_add(lookup_bytes_delta[index], std::memory_order_relaxed);

    UInt64 reclaim_bytes = 0;
    for (const auto & mutation : mutations)
    {
        if (!mutation.previous)
            continue;
        if (mutation.previous->segment && mutation.previous->segment->live_rows.fetch_sub(1, std::memory_order_acq_rel) == 1)
            retired_segments.push_back(mutation.previous->segment);
    }

    for (const auto & retired_segment : retired_segments)
        reclaim_bytes += retired_segment->allocated_bytes;
    total_size_bytes.fetch_sub(reclaim_bytes, std::memory_order_relaxed);

    /// Drop the versions no live snapshot can reach any more. A reader holding an older snapshot keeps
    /// its versions instead of forcing this writer to wait for it.
    const UInt64 watermark = oldestLiveGeneration();
    for (const auto & mutation : mutations)
    {
        auto & entry = entries.at(mutation.entry_id);
        std::unique_ptr<EntryVersion> obsolete;
        {
            std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
            for (auto * version = entry.head.get(); version; version = version->older.get())
            {
                if (version->generation <= watermark)
                {
                    obsolete = std::move(version->older);
                    break;
                }
            }
        }
        recycleVersions(std::move(obsolete));
    }

    if (!persistence->isEnabled())
        return;

    if (loading)
    {
        for (const auto & retired_segment : retired_segments)
        {
            if (retired_segment->persistent_id)
                retired_during_load.push_back(retired_segment->persistent_id);
        }
        return;
    }

    OverwriteCachePersistence::Commit commit;
    commit.generation = new_generation;
    commit.added.reserve(added_segments.size());
    for (const auto & added_segment : added_segments)
        commit.added.push_back({added_segment->persistent_id, added_segment->columns, added_segment->entry_ids.size()});
    commit.removed.reserve(retired_segments.size());
    for (const auto & retired_segment : retired_segments)
    {
        if (retired_segment->persistent_id)
            commit.removed.push_back(retired_segment->persistent_id);
    }

    const UInt64 sequence = persistence->enqueue(std::move(commit));
    /// The wait releases the writer lock first, so a durable write never holds up another publication.
    /// The log is written in order, so waiting for this sequence covers every publication before it.
    writer_lock.unlock();
    if (settings.persist_mode == OverwriteCachePersistMode::Sync)
        persistence->waitDurable(sequence);
}

void StorageOverwriteCache::deleteBlock(const Block & block)
{
    const size_t rows = block.rows();
    if (rows == 0)
        return;

    /// The bytes must match what an insert produced for the same key, so serialize through the
    /// storage's own serializations rather than through whatever the mutation pipeline hands over.
    Columns key_column_data;
    key_column_data.reserve(key_columns.size());
    for (const auto & name : key_columns)
        key_column_data.push_back(recursiveRemoveSparse(block.getByName(name).column->convertToFullColumnIfConst()));

    std::vector<String> serialized_keys;
    serialized_keys.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        WriteBufferFromOwnString out;
        for (size_t index = 0; index < key_column_data.size(); ++index)
            serializations[key_positions[index]]->serializeBinary(*key_column_data[index], row, out, format_settings);
        serialized_keys.push_back(out.str());
    }

    deleteKeys(serialized_keys);
}

size_t StorageOverwriteCache::deleteKeys(const std::vector<String> & serialized_keys)
{
    if (serialized_keys.empty())
        return 0;

    struct Deletion
    {
        EntryId entry_id = 0;
        /// Absent for a tombstone, set for a row relocated into a compacted segment.
        std::unique_ptr<RowData> row;
        std::optional<RowData> previous;
        bool version_installed = false;
    };

    std::unique_lock writer_lock(writer_mutex);

    const UInt64 snapshot_generation = published_generation.load(std::memory_order_acquire);

    std::vector<Deletion> deletions;
    deletions.reserve(serialized_keys.size());
    std::unordered_set<EntryId> seen;
    seen.reserve(serialized_keys.size());
    for (const auto & key : serialized_keys)
    {
        const auto entry = findEntry(key, StringViewHash{}(key));
        if (!entry || !seen.emplace(*entry).second)
            continue;
        auto current = resolveEntry(*entry, snapshot_generation);
        /// No row means the key is already deleted, so there is nothing left to publish for it.
        if (!current)
            continue;
        Deletion deletion;
        deletion.entry_id = *entry;
        deletion.previous = std::move(current);
        deletions.push_back(std::move(deletion));
    }

    if (deletions.empty())
        return 0;

    const size_t deleted_rows = deletions.size();

    std::vector<EntryId> deleted_entry_ids;
    deleted_entry_ids.reserve(deleted_rows);
    for (const auto & deletion : deletions)
        deleted_entry_ids.push_back(deletion.entry_id);
    std::ranges::sort(deleted_entry_ids);

    /// A deletion leaves the same partially dead segment a replacement leaves, so the same rule applies:
    /// rewrite a segment once at least half of it is dead.
    struct SegmentCompaction
    {
        std::shared_ptr<RowSegment> source;
        UInt64 deleted_rows = 0;
        UInt64 projected_live_rows = 0;
        bool selected = false;
        std::vector<std::pair<EntryId, RowData>> live_entries;
    };

    std::vector<SegmentCompaction> segment_compactions;
    std::unordered_map<RowSegment *, size_t> segment_compaction_positions;
    for (const auto & deletion : deletions)
    {
        auto [it, inserted]
            = segment_compaction_positions.emplace(deletion.previous->segment.get(), segment_compactions.size());
        if (inserted)
        {
            segment_compactions.emplace_back();
            segment_compactions.back().source = deletion.previous->segment;
        }
        ++segment_compactions[it->second].deleted_rows;
    }

    const UInt64 bytes_before = total_size_bytes.load(std::memory_order_relaxed);
    UInt64 compaction_budget = 0;
    for (auto & compaction : segment_compactions)
    {
        const UInt64 live_rows = compaction.source->live_rows.load(std::memory_order_acquire);
        if (compaction.deleted_rows > live_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment live-row count");
        compaction.projected_live_rows = live_rows - compaction.deleted_rows;
        const UInt64 total_rows = compaction.source->entry_ids.size();
        if (compaction.projected_live_rows > total_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment size");
        const UInt64 projected_dead_rows = total_rows - compaction.projected_live_rows;
        if (compaction.projected_live_rows == 0 || projected_dead_rows < (total_rows + 1) / 2)
            continue;
        /// Replay must not rewrite a segment, for the same reason an insert must not: the log is not being
        /// written yet, so a rewritten segment would have no file of its own.
        if (loading)
            continue;

        /// A rewritten segment coexists with its source until the readers of the previous epoch drain, so
        /// it needs room beside it. Freeing memory must never fail for want of memory, so a segment whose
        /// source-sized upper bound does not fit is simply left alone instead of failing the `DELETE`.
        const UInt64 headroom
            = settings.max_memory_bytes - std::min(bytes_before + compaction_budget, settings.max_memory_bytes);
        if (compaction.source->allocated_bytes > headroom)
            continue;
        compaction_budget += compaction.source->allocated_bytes;

        compaction.selected = true;
        compaction.live_entries.reserve(compaction.projected_live_rows);
        for (size_t row = 0; row < compaction.source->entry_ids.size(); ++row)
        {
            if ((row & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while compacting `OverwriteCache` row segments");
            const EntryId entry_id = compaction.source->entry_ids[row];
            if (std::ranges::binary_search(deleted_entry_ids, entry_id))
                continue;
            const auto & entry = entries.at(entry_id);
            std::lock_guard row_lock(row_mutexes[rowLockIndex(entry_id)]);
            if (!entry.head || entry.head->row.segment.get() != compaction.source.get() || entry.head->row.segment_row != row)
                continue;
            compaction.live_entries.emplace_back(entry_id, entry.head->row);
        }
    }

    UInt64 prospective_bytes = bytes_before;
    std::vector<std::shared_ptr<RowSegment>> added_segments;
    for (auto & compaction : segment_compactions)
    {
        if (!compaction.selected)
            continue;
        if (compaction.live_entries.size() != compaction.projected_live_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment references");

        auto compaction_selector = ColumnUInt64::create();
        compaction_selector->reserve(compaction.live_entries.size());
        for (const auto & [entry_id, row] : compaction.live_entries)
        {
            static_cast<void>(entry_id);
            compaction_selector->insertValue(row.segment_row);
        }

        auto compacted_segment = std::make_shared<RowSegment>();
        compacted_segment->columns.reserve(compaction.source->columns.size());
        for (size_t position = 0; position < compaction.source->columns.size(); ++position)
        {
            auto selected = compaction.source->columns[position]->decompress()->index(*compaction_selector, 0);
            if (!keep_uncompressed[position])
                selected = selected->compress(/*force_compression=*/true);
            compacted_segment->allocated_bytes += selected->allocatedBytes();
            compacted_segment->columns.push_back(std::move(selected));
        }
        compacted_segment->live_rows.store(compaction.live_entries.size(), std::memory_order_relaxed);
        compacted_segment->entry_ids.reserve(compaction.live_entries.size());
        if (persistence->isEnabled())
            compacted_segment->persistent_id = persistence->allocateSegmentId();
        if (prospective_bytes > std::numeric_limits<UInt64>::max() - compacted_segment->allocated_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
        prospective_bytes += compacted_segment->allocated_bytes;
        added_segments.push_back(compacted_segment);

        for (size_t row = 0; row < compaction.live_entries.size(); ++row)
        {
            const auto & [entry_id, previous] = compaction.live_entries[row];
            auto compacted_row = std::make_unique<RowData>();
            compacted_row->segment = compacted_segment;
            compacted_row->segment_row = static_cast<UInt32>(row);
            compacted_segment->entry_ids.push_back(entry_id);
            Deletion relocation;
            relocation.entry_id = entry_id;
            relocation.row = std::move(compacted_row);
            relocation.previous = previous;
            deletions.push_back(std::move(relocation));
        }
    }

    const UInt64 current_generation = published_generation.load(std::memory_order_acquire);
    if (current_generation == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`OverwriteCache` publication generation space is exhausted");
    const UInt64 new_generation = current_generation + 1;

    try
    {
        /// A tombstone is a version without a segment. It is published exactly like a replacement, so a
        /// reader that captured an earlier generation keeps resolving the row it already found.
        for (auto & deletion : deletions)
        {
            auto & entry = entries.at(deletion.entry_id);
            auto version = takeVersion();
            version->row = deletion.row ? std::move(*deletion.row) : RowData{};
            version->generation = new_generation;
            std::lock_guard lock(row_mutexes[rowLockIndex(deletion.entry_id)]);
            version->older = std::move(entry.head);
            entry.head = std::move(version);
            deletion.version_installed = true;
        }

        fiu_do_on(FailPoints::overwrite_cache_throw_during_publish, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure during `OverwriteCache` publication");
        });

        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_commit);
    }
    catch (...)
    {
        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_rollback);
        for (auto & deletion : deletions)
        {
            if (!deletion.version_installed)
                continue;
            auto & entry = entries.at(deletion.entry_id);
            std::unique_ptr<EntryVersion> discarded;
            {
                std::lock_guard lock(row_mutexes[rowLockIndex(deletion.entry_id)]);
                discarded = std::move(entry.head);
                entry.head = std::move(discarded->older);
            }
            discarded->older.reset();
            recycleVersions(std::move(discarded));
        }
        /// Nothing was published, so the compacted segments are dropped here and were never accounted.
        throw;
    }

    published_generation.store(new_generation, std::memory_order_release);
    total_size_bytes.store(prospective_bytes, std::memory_order_relaxed);
    total_size_rows.fetch_sub(deleted_rows, std::memory_order_relaxed);

    UInt64 reclaim_bytes = 0;
    std::vector<std::shared_ptr<RowSegment>> retired_segments;
    retired_segments.reserve(segment_compactions.size());
    for (const auto & deletion : deletions)
    {
        if (deletion.previous->segment->live_rows.fetch_sub(1, std::memory_order_acq_rel) == 1)
            retired_segments.push_back(deletion.previous->segment);
    }
    for (const auto & retired_segment : retired_segments)
        reclaim_bytes += retired_segment->allocated_bytes;
    total_size_bytes.fetch_sub(reclaim_bytes, std::memory_order_relaxed);

    const UInt64 watermark = oldestLiveGeneration();
    for (const auto & deletion : deletions)
    {
        auto & entry = entries.at(deletion.entry_id);
        std::unique_ptr<EntryVersion> obsolete;
        {
            std::lock_guard lock(row_mutexes[rowLockIndex(deletion.entry_id)]);
            for (auto * version = entry.head.get(); version; version = version->older.get())
            {
                if (version->generation <= watermark)
                {
                    obsolete = std::move(version->older);
                    break;
                }
            }
        }
        recycleVersions(std::move(obsolete));
    }

    if (!persistence->isEnabled())
        return deleted_rows;

    if (loading)
    {
        for (const auto & retired_segment : retired_segments)
        {
            if (retired_segment->persistent_id)
                retired_during_load.push_back(retired_segment->persistent_id);
        }
        return deleted_rows;
    }

    /// A deletion has to be recorded even though it creates no segment: the segment it emptied a row of
    /// is not superseded by anything, so replaying that segment alone would bring the key back.
    OverwriteCachePersistence::Commit commit;
    commit.generation = new_generation;
    commit.deleted_keys = serialized_keys;
    commit.added.reserve(added_segments.size());
    for (const auto & added_segment : added_segments)
        commit.added.push_back({added_segment->persistent_id, added_segment->columns, added_segment->entry_ids.size()});
    commit.removed.reserve(retired_segments.size());
    for (const auto & retired_segment : retired_segments)
    {
        if (retired_segment->persistent_id)
            commit.removed.push_back(retired_segment->persistent_id);
    }

    const UInt64 sequence = persistence->enqueue(std::move(commit));
    writer_lock.unlock();
    if (settings.persist_mode == OverwriteCachePersistMode::Sync)
        persistence->waitDurable(sequence);

    return deleted_rows;
}

void StorageOverwriteCache::checkMutationIsPossible(const MutationCommands & commands, const Settings &) const
{
    if (commands.empty())
        return;
    if (commands.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Storage `OverwriteCache` supports only one mutation command per query");
    if (commands.front().type != MutationCommand::Type::DELETE)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Storage `OverwriteCache` supports only `DELETE` mutations; a stored row is replaced by inserting a greater version");
}

void StorageOverwriteCache::mutate(const MutationCommands & commands, ContextPtr context)
{
    if (commands.empty())
        return;
    checkMutationIsPossible(commands, context->getSettingsRef());

    const auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    const auto storage_ptr = DatabaseCatalog::instance().getTable(getStorageID(), context);

    MutationsInterpreter::Settings mutation_settings(true);
    mutation_settings.return_all_columns = true;
    mutation_settings.return_mutated_rows = true;

    /// The rows to delete are produced by the read path, so a `DELETE` accepts exactly the predicates a
    /// `SELECT` accepts: a complete `KEYS` tuple, or one or more declared lookup indexes.
    MutationsInterpreter interpreter(
        storage_ptr,
        metadata_snapshot,
        commands,
        metadata_snapshot->getColumns().getNamesOfPhysical(),
        context,
        mutation_settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter.execute());
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
        deleteBlock(block);
}

StorageOverwriteCache::ReadResult StorageOverwriteCache::getRowsForPrimaryKeys(const std::vector<String> & serialized_keys) const
{
    ReadResult result;
    result.guard = std::make_shared<ReadGuard>(*this);
    result.rows.reserve(serialized_keys.size());
    std::unordered_set<EntryId> seen;
    for (const auto & key : serialized_keys)
    {
        const auto entry = findEntry(key, StringViewHash{}(key));
        if (!entry || !seen.emplace(*entry).second)
            continue;
        if (auto row = resolveEntry(*entry, result.guard->generation()))
            result.rows.push_back(std::move(*row));
    }
    return result;
}

UInt64 StorageOverwriteCache::getPostingCardinality(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const
{
    UInt64 result = 0;
    std::unordered_set<String> seen_keys;
    seen_keys.reserve(serialized_keys.size());
    for (const auto & key : serialized_keys)
    {
        if (!seen_keys.emplace(key).second)
            continue;
        const size_t hash = StringViewHash{}(key);
        const auto & shard = index->shards[shardIndex(hash)];
        std::shared_lock lock(shard.mutex);
        if (const auto * posting = shard.find(key, hash))
            result += posting->size();
    }
    return result;
}

std::vector<StorageOverwriteCache::EntryId>
StorageOverwriteCache::getPostingIds(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const
{
    std::vector<EntryId> result;
    result.reserve(getPostingCardinality(index, serialized_keys));
    std::unordered_set<String> seen_keys;
    seen_keys.reserve(serialized_keys.size());
    for (const auto & key : serialized_keys)
    {
        if (!seen_keys.emplace(key).second)
            continue;
        const size_t hash = StringViewHash{}(key);
        const auto & shard = index->shards[shardIndex(hash)];
        std::shared_lock lock(shard.mutex);
        if (const auto * posting = shard.find(key, hash))
        {
            posting->forEach([&](EntryId entry_id)
            {
                if ((result.size() & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while reading an `OverwriteCache` posting");
                result.push_back(entry_id);
            });
        }
    }
    std::ranges::sort(result);
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

void StorageOverwriteCache::intersectPostingIds(
    std::vector<EntryId> & entry_ids, const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const
{
    if (entry_ids.empty())
        return;

    std::vector<UInt8> matched(entry_ids.size(), 0);
    std::unordered_set<String> seen_keys;
    seen_keys.reserve(serialized_keys.size());
    for (const auto & key : serialized_keys)
    {
        if (!seen_keys.emplace(key).second)
            continue;
        const size_t hash = StringViewHash{}(key);
        const auto & shard = index->shards[shardIndex(hash)];
        std::shared_lock lock(shard.mutex);
        const auto * posting = shard.find(key, hash);
        if (!posting)
            continue;
        for (size_t position = 0; position < entry_ids.size(); ++position)
        {
            if ((position & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while intersecting `OverwriteCache` postings");
            if (!matched[position] && posting->contains(entry_ids[position]))
                matched[position] = 1;
        }
    }

    size_t output = 0;
    for (size_t position = 0; position < entry_ids.size(); ++position)
    {
        if (matched[position])
            entry_ids[output++] = entry_ids[position];
    }
    entry_ids.resize(output);
}

StorageOverwriteCache::ReadResult StorageOverwriteCache::getRowsForLookupRequests(const std::vector<LookupRequest> & requests) const
{
    if (requests.empty())
        return {};
    ReadResult result;
    result.guard = requests.front().guard;
    if (!result.guard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `OverwriteCache` lookup snapshot guard");
    for (const auto & request : requests)
    {
        if (request.guard != result.guard)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatched `OverwriteCache` lookup snapshot guards");
    }
    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_during_lookup);
    size_t driver = 0;
    UInt64 driver_cardinality = getPostingCardinality(requests[0].index, requests[0].serialized_keys);
    for (size_t index = 1; index < requests.size(); ++index)
    {
        const UInt64 cardinality = getPostingCardinality(requests[index].index, requests[index].serialized_keys);
        if (cardinality < driver_cardinality)
        {
            driver = index;
            driver_cardinality = cardinality;
        }
    }

    auto entry_ids = getPostingIds(requests[driver].index, requests[driver].serialized_keys);
    for (size_t index = 0; index < requests.size() && !entry_ids.empty(); ++index)
    {
        if (index != driver)
            intersectPostingIds(entry_ids, requests[index].index, requests[index].serialized_keys);
    }

    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_after_lookup_ids);

    result.rows.reserve(entry_ids.size());
    for (size_t position = 0; position < entry_ids.size(); ++position)
    {
        if ((position & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while resolving `OverwriteCache` postings");
        if (auto row = resolveEntry(entry_ids[position], result.guard->generation()))
            result.rows.push_back(std::move(*row));
    }
    return result;
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
            key_column_types[key_index]->getDefaultSerialization()->serializeBinary(
                *keys[key_index].column, row, out, format_settings);
        serialized_keys.push_back(out.str());
    }

    ReadGuard read_guard(*this);
    std::vector<RowDataPtr> resolved_rows(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (const auto entry = findEntry(serialized_keys[row], StringViewHash{}(serialized_keys[row])))
            resolved_rows[row] = resolveEntry(*entry, read_guard.generation());
    }

    SegmentColumnCache segment_columns;
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
            insertValueIntoColumn(*resolved_rows[row], result_positions[column], *result_columns[column], segment_columns);
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
    for (auto & index : lookup_indexes)
    {
        for (auto & shard : index->shards)
        {
            std::unique_lock lock(shard.mutex);
            shard.index = PostingShard::PostingMap{};
            std::deque<PostingShard::Posting>().swap(shard.postings);
            shard.arena = std::make_unique<Arena>();
        }
        index->accounted_bytes.store(0, std::memory_order_relaxed);
    }
    for (auto & shard : primary_shards)
    {
        std::unique_lock lock(shard.mutex);
        shard.entries = PrimaryMap{};
        shard.arena = std::make_unique<Arena>();
    }
    /// Entries are reachable without a lock, so no reader that already resolved an identifier may
    /// still be inside the table when its storage is released. Unlike publication, releasing storage
    /// outright has no alternative to waiting - and `TRUNCATE` and `DROP` already hold the table
    /// exclusively, so nothing can be reading through this storage anyway.
    drainReaders();
    entries.clear();
    next_entry_id = 1;
    published_generation.store(0, std::memory_order_release);
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

void StorageOverwriteCache::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    if (commands.size() != 1
        || (commands.front().type != AlterCommand::ADD_LOOKUP_INDEX && commands.front().type != AlterCommand::DROP_LOOKUP_INDEX))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Storage `OverwriteCache` supports only one `ADD INDEX (...)` or `DROP INDEX (...)` command per ALTER");

    auto columns = extractIdentifierList(*commands.front().lookup_index, "lookup `INDEX` clause");
    std::unordered_map<String, size_t> key_order;
    for (size_t position = 0; position < key_columns.size(); ++position)
        key_order.emplace(key_columns[position], position);
    std::unordered_set<String> index_column_set;
    for (const auto & column : columns)
    {
        if (!key_order.contains(column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lookup-index column {} must be declared in `KEYS`", backQuote(column));
        if (!index_column_set.emplace(column).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate column {} in lookup `INDEX`", backQuote(column));
    }
    std::ranges::sort(columns, {}, [&](const String & column) { return key_order.at(column); });
    if (commands.front().type == AlterCommand::ADD_LOOKUP_INDEX && columns == key_columns)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lookup `INDEX` duplicates the complete `KEYS` tuple, which already has a primary index");

    std::shared_lock lock(lookup_catalog_mutex);
    const bool exists = std::ranges::find(lookup_index_columns, columns) != lookup_index_columns.end();
    if (commands.front().type == AlterCommand::ADD_LOOKUP_INDEX && exists && !commands.front().if_not_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate lookup `INDEX` declaration");
    if (commands.front().type == AlterCommand::DROP_LOOKUP_INDEX && !exists && !commands.front().if_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lookup `INDEX` does not exist");
}

void StorageOverwriteCache::alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder &)
{
    checkAlterIsPossible(commands, context);
    const auto & command = commands.front();

    Names columns = extractIdentifierList(*command.lookup_index, "lookup `INDEX` clause");
    std::unordered_map<String, size_t> key_order;
    for (size_t position = 0; position < key_columns.size(); ++position)
        key_order.emplace(key_columns[position], position);
    std::ranges::sort(columns, {}, [&](const String & column) { return key_order.at(column); });

    const bool drop_index = command.type == AlterCommand::DROP_LOOKUP_INDEX;
    {
        std::shared_lock lock(lookup_catalog_mutex);
        const bool exists = std::ranges::find(lookup_index_columns, columns) != lookup_index_columns.end();
        if ((!drop_index && exists) || (drop_index && !exists))
            return;
    }

    const auto table_id = getStorageID();
    const auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    ASTPtr metadata_indexes = new_metadata.lookup_indexes
        ? new_metadata.lookup_indexes->clone()
        : make_intrusive<ASTExpressionList>();
    auto & metadata_index_list = metadata_indexes->as<ASTExpressionList &>();

    if (drop_index)
    {
        const auto erase_begin = std::remove_if(metadata_index_list.children.begin(), metadata_index_list.children.end(), [&](const ASTPtr & index_ast)
        {
            auto current_columns = extractIdentifierList(*index_ast, "lookup `INDEX` clause");
            std::ranges::sort(current_columns, {}, [&](const String & column) { return key_order.at(column); });
            return current_columns == columns;
        });
        metadata_index_list.children.erase(erase_begin, metadata_index_list.children.end());
        new_metadata.lookup_indexes = metadata_index_list.children.empty() ? nullptr : metadata_indexes;
        auto prepared_metadata = std::make_shared<const StorageInMemoryMetadata>(new_metadata);

        std::unique_lock writer_lock(writer_mutex);
        const UInt64 current_generation = published_generation.load(std::memory_order_acquire);
        if (current_generation == std::numeric_limits<UInt64>::max())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "`OverwriteCache` publication generation space is exhausted");

        auto next_lookup_index_columns = lookup_index_columns;
        auto next_lookup_index_positions = lookup_index_positions;
        auto next_lookup_index_column_types = lookup_index_column_types;
        auto next_lookup_indexes = lookup_indexes;
        const auto next_it = std::ranges::find(next_lookup_index_columns, columns);
        if (next_it == next_lookup_index_columns.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Lookup-index catalog changed while preparing `DROP INDEX`");
        const size_t position = next_it - next_lookup_index_columns.begin();
        const UInt64 removed_bytes = next_lookup_indexes[position]->accounted_bytes.load(std::memory_order_relaxed);
        LookupIndexPtr removed_index = next_lookup_indexes[position];
        next_lookup_index_columns.erase(next_lookup_index_columns.begin() + position);
        next_lookup_index_positions.erase(next_lookup_index_positions.begin() + position);
        next_lookup_index_column_types.erase(next_lookup_index_column_types.begin() + position);
        next_lookup_indexes.erase(next_lookup_indexes.begin() + position);

        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(
            context, table_id, new_metadata, /*validate_new_create_query=*/true);

        {
            std::unique_lock lock(lookup_catalog_mutex);
            lookup_index_columns.swap(next_lookup_index_columns);
            lookup_index_positions.swap(next_lookup_index_positions);
            lookup_index_column_types.swap(next_lookup_index_column_types);
            lookup_indexes.swap(next_lookup_indexes);
        }
        next_lookup_indexes.clear();
        published_generation.store(current_generation + 1, std::memory_order_release);
        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_after_drop_index_publication);
        drainReaders();
        removed_index.reset();
        total_size_bytes.fetch_sub(removed_bytes, std::memory_order_relaxed);
        setInMemoryMetadata(std::move(prepared_metadata));
        return;
    }

    std::vector<size_t> positions;
    DataTypes types;
    positions.reserve(columns.size());
    types.reserve(columns.size());
    for (const auto & column : columns)
    {
        const auto position = getColumnPosition(column);
        positions.push_back(position);
        types.push_back(sample_block.getByPosition(position).type);
    }

    auto shadow = std::make_shared<LookupIndex>();
    UInt64 index_bytes = 0;
    size_t snapshot_entry_count = 0;
    SegmentColumnCache segment_columns;
    const auto add_to_shadow = [&](EntryId entry_id, const RowData & row)
    {
        const String key = serializeRowColumns(row, positions, segment_columns);
        auto & shard = shadow->shards[shardIndex(StringViewHash{}(key))];
        PostingShard::PostingMap::LookupResult it = nullptr;
        bool inserted = false;
        shard.index.emplace(ArenaKeyHolder{key, *shard.arena}, it, inserted, StringViewHash{}(key));
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(shard.postings.size());
            shard.postings.emplace_back();
        }
        shard.postings[it->getMapped()].push_back(entry_id);
    };

    std::unique_ptr<ReadGuard> snapshot_guard;
    {
        std::lock_guard writer_lock(writer_mutex);
        snapshot_entry_count = entries.size();
        snapshot_guard = std::make_unique<ReadGuard>(*this);
    }
    for (EntryId entry_id = 1; entry_id <= snapshot_entry_count; ++entry_id)
    {
        const auto row = resolveEntry(entry_id, snapshot_guard->generation());
        if (!row)
            continue;
        add_to_shadow(entry_id, *row);
    }
    snapshot_guard.reset();

    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_during_index_build);

    fiu_do_on(FailPoints::overwrite_cache_throw_during_index_build, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure during `OverwriteCache` lookup-index build");
    });

    std::unique_lock writer_lock(writer_mutex);
    const size_t catch_up_entry_count = entries.size();
    ReadGuard catch_up_guard(*this);
    for (EntryId entry_id = snapshot_entry_count + 1; entry_id <= catch_up_entry_count; ++entry_id)
    {
        const auto row = resolveEntry(entry_id, catch_up_guard.generation());
        if (!row)
            continue;
        add_to_shadow(entry_id, *row);
    }
    for (const auto & shard : shadow->shards)
    {
        UInt64 shard_bytes = shard.index.getBufferSizeInBytes() + shard.arena->allocatedBytes();
        for (const auto & posting : shard.postings)
            shard_bytes += sizeof(PostingShard::Posting) + posting.allocatedBytes();
        if (index_bytes > std::numeric_limits<UInt64>::max() - shard_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
        index_bytes += shard_bytes;
    }

    const UInt64 current_bytes = total_size_bytes.load(std::memory_order_relaxed);
    if (index_bytes > settings.max_memory_bytes - std::min(current_bytes, settings.max_memory_bytes))
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` cannot add lookup index within `max_memory_bytes`");

    auto next_lookup_index_columns = lookup_index_columns;
    auto next_lookup_index_positions = lookup_index_positions;
    auto next_lookup_index_column_types = lookup_index_column_types;
    auto next_lookup_indexes = lookup_indexes;
    next_lookup_index_columns.reserve(next_lookup_index_columns.size() + 1);
    next_lookup_index_positions.reserve(next_lookup_index_positions.size() + 1);
    next_lookup_index_column_types.reserve(next_lookup_index_column_types.size() + 1);
    next_lookup_indexes.reserve(next_lookup_indexes.size() + 1);
    next_lookup_index_columns.push_back(columns);
    next_lookup_index_positions.push_back(positions);
    next_lookup_index_column_types.push_back(types);
    next_lookup_indexes.push_back(shadow);

    metadata_index_list.children.push_back(makeLookupIndexAST(columns));
    new_metadata.lookup_indexes = metadata_indexes;
    auto prepared_metadata = std::make_shared<const StorageInMemoryMetadata>(new_metadata);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(
        context, table_id, new_metadata, /*validate_new_create_query=*/true);

    shadow->accounted_bytes.store(index_bytes, std::memory_order_relaxed);
    {
        std::unique_lock lock(lookup_catalog_mutex);
        lookup_index_columns.swap(next_lookup_index_columns);
        lookup_index_positions.swap(next_lookup_index_positions);
        lookup_index_column_types.swap(next_lookup_index_column_types);
        lookup_indexes.swap(next_lookup_indexes);
    }
    total_size_bytes.fetch_add(index_bytes, std::memory_order_relaxed);
    setInMemoryMetadata(std::move(prepared_metadata));
}

void StorageOverwriteCache::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    clearData();
    persistence->truncate();
}

void StorageOverwriteCache::drop()
{
    clearData();
    persistence->removeAllFiles();
}

void StorageOverwriteCache::shutdown(bool)
{
    /// Draining the queue makes a clean restart lose nothing even in `Async` mode.
    persistence->shutdown();
}

void StorageOverwriteCache::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    persistence->rename(new_path_to_table_data);
    renameInMemory(new_table_id);
}

void StorageOverwriteCache::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    if (!persistence->isEnabled())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Storage `OverwriteCache` cannot be backed up with `persist_mode = 'none'`, because it keeps no data on disk");

    /// The files are immutable, so a backup copies them instead of reading the table. Retirement is held
    /// back until every entry has been read, or a publication could delete a file the collector was told
    /// to copy - which is why the pin travels with the entries instead of being released here.
    auto pin = std::make_shared<OverwriteCachePersistence::BackupPin>(*persistence);
    const auto file_names = persistence->collectFilesForBackup();
    const auto data_path = persistence->getPath();
    const auto disk = persistence->getDisk();

    const std::filesystem::path data_path_in_backup_fs = data_path_in_backup;
    BackupEntries backup_entries;
    backup_entries.reserve(file_names.size());
    for (const auto & file_name : file_names)
    {
        const auto file_path = std::filesystem::path(data_path) / file_name;
        BackupEntryPtr entry = std::make_shared<BackupEntryFromImmutableFile>(
            disk, file_path, /*copy_encrypted=*/false, disk->getFileSize(file_path));
        backup_entries.emplace_back(data_path_in_backup_fs / file_name, wrapBackupEntryWith(std::move(entry), pin));
    }

    backup_entries_collector.addBackupEntries(std::move(backup_entries));
}

void StorageOverwriteCache::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.isNonEmptyTableAllowed() && total_size_rows.load(std::memory_order_relaxed))
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageOverwriteCache>(shared_from_this()), backup, data_path_in_backup]
        { storage->restoreDataImpl(backup, data_path_in_backup); });
}

void StorageOverwriteCache::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup)
{
    const auto file_names = backup->listFiles(data_path_in_backup, /*recursive=*/false);
    if (std::ranges::find(file_names, OverwriteCachePersistence::manifest_file_name) == file_names.end())
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "Backup of storage `OverwriteCache` at {} has no {} file",
            data_path_in_backup,
            OverwriteCachePersistence::manifest_file_name);

    /// Replacing the files means replacing the table, so the cache is dropped first and rebuilt from the
    /// restored log rather than merged with whatever it happened to hold.
    clearData();
    persistence->removeAllFiles();

    const std::filesystem::path data_path_in_backup_fs = data_path_in_backup;
    for (const auto & file_name : file_names)
    {
        auto in = backup->readFile(data_path_in_backup_fs / file_name);
        persistence->restoreFileFromBackup(file_name, *in);
    }

    loadPersistedData();
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
            std::vector<Names> lookup_index_columns;
            if (args.storage_def->lookup_indexes)
            {
                lookup_index_columns.reserve(args.storage_def->lookup_indexes->children.size());
                for (const auto & index_ast : args.storage_def->lookup_indexes->children)
                    lookup_index_columns.push_back(extractIdentifierList(*index_ast, "lookup `INDEX` clause"));
            }
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

            std::unordered_map<String, size_t> key_order;
            for (size_t position = 0; position < key_columns.size(); ++position)
                key_order.emplace(key_columns[position], position);

            std::unordered_set<String> index_signatures;
            for (auto & index_columns : lookup_index_columns)
            {
                std::unordered_set<String> index_column_set;
                for (const auto & column : index_columns)
                {
                    if (!key_set.contains(column))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "Lookup-index column {} must be declared in `KEYS`", backQuote(column));
                    if (!index_column_set.emplace(column).second)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate column {} in lookup `INDEX`", backQuote(column));
                }
                std::ranges::sort(index_columns, {}, [&](const String & column) { return key_order.at(column); });
                if (index_columns == key_columns)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Lookup `INDEX` duplicates the complete `KEYS` tuple, which already has a primary index");
                String signature;
                for (const auto & column : index_columns)
                {
                    signature += column;
                    signature.push_back('\0');
                }
                if (!index_signatures.emplace(signature).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate lookup `INDEX` declaration");
            }

            /// A temporary table cannot outlive the session that created it, so there is nothing for a log
            /// to restore. Asking for one anyway is a request that cannot be honoured rather than a
            /// default to quietly override.
            const bool persist_mode_given = args.storage_def->settings
                && std::ranges::any_of(
                       args.storage_def->settings->changes,
                       [](const SettingChange & change) { return change.name == "persist_mode"; });
            if (args.query.isTemporary())
            {
                if (persist_mode_given && settings.persist_mode != OverwriteCachePersistMode::None)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "A temporary table of storage `OverwriteCache` cannot use `persist_mode = '{}'`, because it does not "
                        "outlive its session",
                        toString(settings.persist_mode));
                settings.persist_mode = OverwriteCachePersistMode::None;
            }

            DiskPtr disk;
            if (settings.persist_mode != OverwriteCachePersistMode::None)
            {
                if (args.relative_data_path.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Storage `OverwriteCache` requires a data path to persist data. Use `persist_mode = 'none'` for a "
                        "table that is not meant to survive a restart");
                disk = args.getContext()->getDisk(settings.disk_name);
            }

            return std::make_shared<StorageOverwriteCache>(
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                version_column,
                std::move(key_columns),
                std::move(lookup_index_columns),
                args.storage_def->lookup_indexes ? args.storage_def->lookup_indexes->clone() : nullptr,
                std::move(settings),
                args.storage_def->settings ? args.storage_def->settings->clone() : nullptr,
                std::move(disk),
                args.relative_data_path);
        },
        {
            .supports_settings = true,
            .supports_keys = true,
            .supports_lookup_indexes = true,
            .has_builtin_setting_fn = isOverwriteCacheSetting,
        });
}

}
