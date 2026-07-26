#include <Storages/StorageOverwriteCache.h>

#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ThreadStatus.h>
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
    return name == "max_memory_bytes" || name == "equal_version_tiebreak_columns";
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
            std::shared_ptr<StorageOverwriteCache::RowSegment> current_segment;
            ColumnPtr source_column;
            for (size_t row = offset; row < offset + rows_to_emit; ++row)
            {
                if (current_segment != rows[row].segment)
                {
                    current_segment = rows[row].segment;
                    source_column = current_segment->columns[positions[output_position]]->decompress();
                }
                columns[output_position]->insertFrom(*source_column, rows[row].segment_row);
            }
        }

        offset += rows_to_emit;
        return Chunk(std::move(columns), rows_to_emit);
    }

private:
    /// Keep the guard before rows so rows release their segment references before the epoch is released.
    StorageOverwriteCache::ReadGuardPtr read_guard;
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

        auto indexes = storage.getLookupIndexSnapshot();
        for (auto & index : indexes)
        {
            auto [keys, requires_scan]
                = getFilterKeys(index.columns, index.types, filter_actions_dag.get(), context);
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
    ASTPtr settings_changes_)
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

String StorageOverwriteCache::serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
        serializations[position]->serializeBinary(*block.getByPosition(position).column, row, out, {});
    return out.str();
}

String StorageOverwriteCache::serializeRowColumns(const RowData & row, const std::vector<size_t> & positions) const
{
    WriteBufferFromOwnString out;
    for (const auto position : positions)
    {
        const auto column = row.segment->columns[position]->decompress();
        serializations[position]->serializeBinary(*column, row.segment_row, out, {});
    }
    return out.str();
}

void StorageOverwriteCache::insertValueIntoColumn(const RowData & row, size_t position, IColumn & column) const
{
    const auto source = row.segment->columns[position]->decompress();
    column.insertFrom(*source, row.segment_row);
}

Field StorageOverwriteCache::getRowField(const RowData & row, size_t position) const
{
    const auto column = row.segment->columns[position]->decompress();
    Field result;
    column->get(row.segment_row, result);
    return result;
}

Field StorageOverwriteCache::getCandidateField(const CandidateRow & row, size_t position) const
{
    if (position >= row.value_offsets.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` candidate row offsets");
    const size_t begin = position ? row.value_offsets[position - 1] : 0;
    const size_t end = row.value_offsets[position];
    auto column = sample_block.getByPosition(position).type->createColumn();
    ReadBufferFromMemory input(row.encoded_values.data() + begin, end - begin);
    serializations[position]->deserializeBinary(*column, input, {});
    Field result;
    column->get(0, result);
    return result;
}

int StorageOverwriteCache::compareWinner(const CandidateRow & lhs, const CandidateRow & rhs) const
{
    const auto compare_field = [](const Field & left, const Field & right)
    {
        if (left < right)
            return -1;
        if (right < left)
            return 1;
        return 0;
    };

    if (const int result = compare_field(getCandidateField(lhs, version_position), getCandidateField(rhs, version_position)))
        return result;

    for (const auto position : tiebreak_positions)
    {
        if (const int result = compare_field(getCandidateField(lhs, position), getCandidateField(rhs, position)))
            return result;
    }

    if (lhs.encoded_values != rhs.encoded_values || lhs.value_offsets != rhs.value_offsets)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Conflicting rows for the same `OverwriteCache` key have identical version and tie-break values");
    return 0;
}

int StorageOverwriteCache::compareWinner(const CandidateRow & lhs, const RowData & rhs) const
{
    const auto compare_field = [](const Field & left, const Field & right)
    {
        if (left < right)
            return -1;
        if (right < left)
            return 1;
        return 0;
    };

    if (const int result = compare_field(getCandidateField(lhs, version_position), getRowField(rhs, version_position)))
        return result;
    for (const auto position : tiebreak_positions)
    {
        if (const int result = compare_field(getCandidateField(lhs, position), getRowField(rhs, position)))
            return result;
    }

    if (!rowsEqual(lhs, rhs))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Conflicting rows for the same `OverwriteCache` key have identical version and tie-break values");
    return 0;
}

bool StorageOverwriteCache::rowsEqual(const CandidateRow & lhs, const RowData & rhs) const
{
    for (size_t position = 0; position < sample_block.columns(); ++position)
        if (getCandidateField(lhs, position) != getRowField(rhs, position))
            return false;
    return true;
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

std::optional<StorageOverwriteCache::EntryId> StorageOverwriteCache::findEntry(const String & key) const
{
    const auto & shard = primary_shards[primaryShardIndex(key)];
    std::shared_lock lock(shard.mutex);
    if (const auto it = shard.entries.find(key); it != shard.entries.end())
        return it->second;
    return {};
}

StorageOverwriteCache::RowDataPtr StorageOverwriteCache::resolveEntry(EntryId entry_id, UInt64 snapshot_generation) const
{
    std::shared_lock entries_lock(entries_by_id_mutex);
    if (entry_id == 0 || entry_id > entries_by_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` entry identifier");
    std::lock_guard row_lock(row_mutexes[rowLockIndex(entry_id)]);
    const auto & entry = entries_by_id[entry_id - 1];
    if (entry.pending_generation != 0 && entry.pending_generation <= snapshot_generation)
        return *entry.pending;
    return entry.committed;
}

void StorageOverwriteCache::insertBlock(const Block & block)
{
    std::vector<std::vector<size_t>> lookup_positions_snapshot;
    {
        std::shared_lock catalog_lock(lookup_catalog_mutex);
        lookup_positions_snapshot = lookup_index_positions;
    }
    std::unordered_map<String, std::shared_ptr<CandidateRow>> candidates;
    candidates.reserve(block.rows());

    for (size_t row = 0; row < block.rows(); ++row)
    {
        auto candidate = std::make_shared<CandidateRow>();
        candidate->source_row = static_cast<UInt32>(row);
        WriteBufferFromOwnString encoded;
        candidate->value_offsets.reserve(block.columns());
        for (size_t position = 0; position < block.columns(); ++position)
        {
            serializations[position]->serializeBinary(*block.getByPosition(position).column, row, encoded, {});
            if (encoded.count() > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "An `OverwriteCache` row exceeds the compact 4 GiB row limit");
            candidate->value_offsets.push_back(static_cast<UInt32>(encoded.count()));
        }
        candidate->encoded_values = encoded.str();
        candidate->primary_key = serializeColumns(block, row, key_positions);
        candidate->lookup_keys.reserve(lookup_positions_snapshot.size());
        for (const auto & positions : lookup_positions_snapshot)
            candidate->lookup_keys.push_back(serializeColumns(block, row, positions));
        auto [it, inserted] = candidates.emplace(candidate->primary_key, candidate);
        if (!inserted && compareWinner(*candidate, *it->second) > 0)
            it->second = std::move(candidate);
    }

    struct Mutation
    {
        String key;
        EntryId entry_id = 0;
        std::shared_ptr<CandidateRow> candidate;
        std::unique_ptr<RowData> row;
        std::optional<RowData> previous;
        std::vector<String> lookup_keys;
        bool is_new = false;
        bool primary_inserted = false;
        bool pending_installed = false;
    };

    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_after_lookup_catalog_snapshot);
    std::lock_guard writer_lock(writer_mutex);
    if (lookup_positions_snapshot != lookup_index_positions)
    {
        for (auto & [key, candidate] : candidates)
        {
            static_cast<void>(key);
            candidate->lookup_keys.clear();
            candidate->lookup_keys.reserve(lookup_index_positions.size());
            for (const auto & positions : lookup_index_positions)
                candidate->lookup_keys.push_back(serializeColumns(block, candidate->source_row, positions));
        }
    }
    std::vector<Mutation> mutations;
    mutations.reserve(candidates.size());

    for (auto & [key, candidate] : candidates)
    {
        auto entry = findEntry(key);
        if (!entry)
        {
            auto lookup_keys = candidate->lookup_keys;
            mutations.push_back({key, {}, std::move(candidate), {}, {}, std::move(lookup_keys), true});
            continue;
        }

        const auto current = resolveEntry(*entry, published_generation.load(std::memory_order_acquire));
        if (!current)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Corrupted `OverwriteCache` primary index");

        const int winner = compareWinner(*candidate, *current);
        if (winner <= 0)
            continue;

        auto lookup_keys = candidate->lookup_keys;
        mutations.push_back({key, *entry, std::move(candidate), {}, current, std::move(lookup_keys), false});
    }

    if (mutations.empty())
        return;

    auto selector = ColumnUInt64::create();
    selector->reserve(mutations.size());
    for (const auto & mutation : mutations)
        selector->insertValue(mutation.candidate->source_row);

    auto segment = std::make_shared<RowSegment>();
    segment->columns.reserve(block.columns());
    for (const auto & source : block)
    {
        auto selected = source.column->index(*selector, 0)->compress(/*force_compression=*/true);
        segment->allocated_bytes += selected->allocatedBytes();
        segment->columns.push_back(std::move(selected));
    }
    segment->live_rows.store(mutations.size(), std::memory_order_relaxed);

    UInt64 prospective_bytes = total_size_bytes.load(std::memory_order_relaxed);
    if (prospective_bytes > std::numeric_limits<UInt64>::max() - segment->allocated_bytes)
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
    prospective_bytes += segment->allocated_bytes;
    for (size_t row = 0; row < mutations.size(); ++row)
    {
        auto compact_row = std::make_unique<RowData>();
        compact_row->segment = segment;
        compact_row->segment_row = static_cast<UInt32>(row);
        UInt64 added_bytes = 0;
        if (mutations[row].is_new)
            added_bytes += sizeof(std::pair<const String, EntryId>) + mutations[row].key.size() + 64;
        if (prospective_bytes > std::numeric_limits<UInt64>::max() - added_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
        prospective_bytes += added_bytes;
        mutations[row].row = std::move(compact_row);
        mutations[row].candidate.reset();
    }

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
    std::unordered_set<EntryId> mutated_entry_ids;
    mutated_entry_ids.reserve(mutations.size());
    for (const auto & mutation : mutations)
    {
        if (mutation.is_new || !mutation.previous || !mutation.previous->segment)
            continue;
        mutated_entry_ids.insert(mutation.entry_id);
        auto [it, inserted] = segment_compaction_positions.emplace(
            mutation.previous->segment.get(), segment_compactions.size());
        if (inserted)
        {
            segment_compactions.emplace_back();
            segment_compactions.back().source = mutation.previous->segment;
        }
        ++segment_compactions[it->second].replaced_rows;
    }

    std::unordered_map<RowSegment *, size_t> selected_compaction_positions;
    for (size_t position = 0; position < segment_compactions.size(); ++position)
    {
        auto & compaction = segment_compactions[position];
        const UInt64 live_rows = compaction.source->live_rows.load(std::memory_order_acquire);
        if (compaction.replaced_rows > live_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment live-row count");
        compaction.projected_live_rows = live_rows - compaction.replaced_rows;
        const UInt64 total_rows = compaction.source->columns.empty() ? 0 : compaction.source->columns.front()->size();
        if (compaction.projected_live_rows > total_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` row-segment size");
        const UInt64 projected_dead_rows = total_rows - compaction.projected_live_rows;
        if (compaction.projected_live_rows == 0 || projected_dead_rows < (total_rows + 1) / 2)
            continue;
        compaction.selected = true;
        compaction.live_entries.reserve(compaction.projected_live_rows);
        selected_compaction_positions.emplace(compaction.source.get(), position);
    }

    if (!selected_compaction_positions.empty())
    {
        std::shared_lock entries_lock(entries_by_id_mutex);
        for (EntryId entry_id = 1; entry_id <= entries_by_id.size(); ++entry_id)
        {
            if ((entry_id & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while compacting `OverwriteCache` row segments");
            if (mutated_entry_ids.contains(entry_id))
                continue;
            const auto & entry = entries_by_id[entry_id - 1];
            std::lock_guard row_lock(row_mutexes[rowLockIndex(entry_id)]);
            if (!entry.committed)
                continue;
            const auto it = selected_compaction_positions.find(entry.committed->segment.get());
            if (it != selected_compaction_positions.end())
                segment_compactions[it->second].live_entries.emplace_back(entry_id, *entry.committed);
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
        for (const auto & source_column : compaction.source->columns)
        {
            auto selected = source_column->decompress()->index(*compaction_selector, 0)->compress(/*force_compression=*/true);
            compacted_segment->allocated_bytes += selected->allocatedBytes();
            compacted_segment->columns.push_back(std::move(selected));
        }
        compacted_segment->live_rows.store(compaction.live_entries.size(), std::memory_order_relaxed);
        if (prospective_bytes > std::numeric_limits<UInt64>::max() - compacted_segment->allocated_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
        prospective_bytes += compacted_segment->allocated_bytes;

        for (size_t row = 0; row < compaction.live_entries.size(); ++row)
        {
            const auto & [entry_id, previous] = compaction.live_entries[row];
            auto compacted_row = std::make_unique<RowData>();
            compacted_row->segment = compacted_segment;
            compacted_row->segment_row = static_cast<UInt32>(row);
            Mutation compacted_mutation;
            compacted_mutation.entry_id = entry_id;
            compacted_mutation.row = std::move(compacted_row);
            compacted_mutation.previous = previous;
            mutations.push_back(std::move(compacted_mutation));
        }
    }
    candidates.clear();

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
    std::vector<std::unordered_map<String, size_t>> lookup_additions(lookup_indexes.size());
    for (auto & additions : lookup_additions)
        additions.reserve(new_entries);
    for (auto & mutation : mutations)
    {
        if (mutation.is_new)
        {
            mutation.entry_id = staged_next_entry_id++;
            for (size_t index = 0; index < mutation.lookup_keys.size(); ++index)
                ++lookup_additions[index][mutation.lookup_keys[index]];
        }
    }

    struct PreparedPosting
    {
        size_t index = 0;
        size_t shard_index = 0;
        String key;
        size_t additional_rows = 0;
        size_t old_size = 0;
        UInt64 old_allocated_bytes = 0;
        UInt64 reserved_allocated_bytes = 0;
        UInt64 node_bytes = 0;
        UInt64 bucket_bytes_delta = 0;
        bool inserted = false;
        bool prepared = false;
    };

    std::vector<PreparedPosting> prepared_postings;
    for (size_t index = 0; index < lookup_additions.size(); ++index)
    {
        prepared_postings.reserve(prepared_postings.size() + lookup_additions[index].size());
        for (const auto & [key, count] : lookup_additions[index])
            prepared_postings.push_back({index, postingShardIndex(key), key, count});
    }

    std::vector<std::shared_ptr<RowSegment>> retired_segments;
    retired_segments.reserve(mutations.size());

    const UInt64 current_generation = published_generation.load(std::memory_order_acquire);
    if (current_generation == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`OverwriteCache` publication generation space is exhausted");

    size_t old_entries_by_id_size = 0;
    UInt64 entries_capacity_delta = 0;
    {
        std::unique_lock lock(entries_by_id_mutex);
        old_entries_by_id_size = entries_by_id.size();
        const size_t old_capacity = entries_by_id.capacity();
        entries_by_id.reserve(entries_by_id.size() + new_entries);
        entries_capacity_delta = static_cast<UInt64>(entries_by_id.capacity() - old_capacity) * sizeof(Entry);
    }
    if (prospective_bytes > std::numeric_limits<UInt64>::max() - entries_capacity_delta)
    {
        total_size_bytes.fetch_add(entries_capacity_delta, std::memory_order_relaxed);
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
    }
    prospective_bytes += entries_capacity_delta;
    const UInt64 new_generation = current_generation + 1;
    std::vector<UInt64> lookup_bytes_delta(lookup_indexes.size());
    std::vector<UInt64> retained_lookup_bytes(lookup_indexes.size());
    std::vector<size_t> primary_additions(primary_shards.size());
    std::vector<UInt64> primary_bucket_bytes_delta(primary_shards.size());
    for (const auto & mutation : mutations)
    {
        if (mutation.is_new)
            ++primary_additions[primaryShardIndex(mutation.key)];
    }

    try
    {
        for (size_t shard_index = 0; shard_index < primary_shards.size(); ++shard_index)
        {
            if (primary_additions[shard_index] == 0)
                continue;
            auto & shard = primary_shards[shard_index];
            std::unique_lock lock(shard.mutex);
            const size_t old_bucket_count = shard.entries.bucket_count();
            const size_t required_size = shard.entries.size() + primary_additions[shard_index];
            if (static_cast<double>(required_size)
                > static_cast<double>(old_bucket_count) * static_cast<double>(shard.entries.max_load_factor()))
                shard.entries.reserve(required_size);
            primary_bucket_bytes_delta[shard_index]
                = static_cast<UInt64>(shard.entries.bucket_count() - old_bucket_count) * sizeof(void *);
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - primary_bucket_bytes_delta[shard_index])
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` primary-index memory accounting overflow");
            prospective_bytes += primary_bucket_bytes_delta[shard_index];
        }

        for (auto & prepared : prepared_postings)
        {
            auto & shard = lookup_indexes[prepared.index]->shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            const size_t old_bucket_count = shard.postings.bucket_count();
            const size_t required_size = shard.postings.size() + 1;
            if (static_cast<double>(required_size)
                > static_cast<double>(old_bucket_count) * static_cast<double>(shard.postings.max_load_factor()))
                shard.postings.reserve(required_size);
            prepared.bucket_bytes_delta
                = static_cast<UInt64>(shard.postings.bucket_count() - old_bucket_count) * sizeof(void *);
            prepared.prepared = true;
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - prepared.bucket_bytes_delta)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
            prospective_bytes += prepared.bucket_bytes_delta;
            if (lookup_bytes_delta[prepared.index] > std::numeric_limits<UInt64>::max() - prepared.bucket_bytes_delta)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
            lookup_bytes_delta[prepared.index] += prepared.bucket_bytes_delta;

            auto [it, inserted] = shard.postings.try_emplace(prepared.key);
            prepared.old_size = it->second.size();
            prepared.old_allocated_bytes = it->second.allocatedBytes();
            prepared.inserted = inserted;
            prepared.node_bytes = inserted
                ? sizeof(std::pair<const String, PostingShard::Posting>) + prepared.key.size() + 64
                : 0;
            it->second.reserve(it->second.size() + prepared.additional_rows, staged_next_entry_id - 1);
            prepared.reserved_allocated_bytes = it->second.allocatedBytes();
            const UInt64 allocated_delta
                = prepared.reserved_allocated_bytes - prepared.old_allocated_bytes + prepared.node_bytes;
            if (prospective_bytes > std::numeric_limits<UInt64>::max() - allocated_delta)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` memory accounting overflow");
            prospective_bytes += allocated_delta;
            if (lookup_bytes_delta[prepared.index] > std::numeric_limits<UInt64>::max() - allocated_delta)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
            lookup_bytes_delta[prepared.index] += allocated_delta;
        }

        if (prospective_bytes > settings.max_memory_bytes)
            throw Exception(
                ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                "`OverwriteCache` insert requires {} bytes, exceeding `max_memory_bytes` = {}",
                prospective_bytes,
                settings.max_memory_bytes);

        {
            std::unique_lock lock(entries_by_id_mutex);
            for (auto & mutation : mutations)
            {
                if (!mutation.is_new)
                    continue;
                if (mutation.entry_id != entries_by_id.size() + 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted `OverwriteCache` entry identifier sequence");
                auto & entry = entries_by_id.emplace_back();
                entry.pending = std::move(mutation.row);
                entry.pending_generation = new_generation;
            }
        }

        for (auto & mutation : mutations)
        {
            if (mutation.is_new)
            {
                auto & primary_shard = primary_shards[primaryShardIndex(mutation.key)];
                {
                    std::unique_lock lock(primary_shard.mutex);
                    mutation.primary_inserted = primary_shard.entries.emplace(mutation.key, mutation.entry_id).second;
                }
                if (!mutation.primary_inserted)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate `OverwriteCache` key during publication");

                for (size_t index = 0; index < mutation.lookup_keys.size(); ++index)
                {
                    const auto & lookup_key = mutation.lookup_keys[index];
                    auto & shard = lookup_indexes[index]->shards[postingShardIndex(lookup_key)];
                    std::unique_lock lock(shard.mutex);
                    shard.postings.find(lookup_key)->second.push_back(mutation.entry_id);
                }
            }
            else
            {
                /// Every indexed column is part of the immutable composite key, so a replacement
                /// publishes only the new payload and retains the existing postings.
                auto & entry = entries_by_id[mutation.entry_id - 1];
                std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
                if (entry.pending_generation != 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Concurrent `OverwriteCache` publication for the same entry");
                entry.pending = std::move(mutation.row);
                entry.pending_generation = new_generation;
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
        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_rollback);
        UInt64 retained_bytes = entries_capacity_delta;
        for (const UInt64 bucket_delta : primary_bucket_bytes_delta)
            retained_bytes += bucket_delta;
        for (auto & mutation : mutations)
        {
            if (!mutation.pending_installed)
                continue;
            auto & entry = entries_by_id[mutation.entry_id - 1];
            std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
            entry.pending.reset();
            entry.pending_generation = 0;
        }
        for (const auto & prepared : prepared_postings)
        {
            if (!prepared.prepared)
                continue;
            auto & shard = lookup_indexes[prepared.index]->shards[prepared.shard_index];
            std::unique_lock lock(shard.mutex);
            retained_bytes += prepared.bucket_bytes_delta;
            retained_lookup_bytes[prepared.index] += prepared.bucket_bytes_delta;
            if (const auto it = shard.postings.find(prepared.key); it != shard.postings.end())
            {
                it->second.resize(prepared.old_size);
                if (prepared.inserted)
                    shard.postings.erase(it);
                else
                {
                    const UInt64 retained_delta = it->second.allocatedBytes() - prepared.old_allocated_bytes;
                    retained_bytes += retained_delta;
                    retained_lookup_bytes[prepared.index] += retained_delta;
                }
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

        total_size_bytes.fetch_add(retained_bytes, std::memory_order_relaxed);
        for (size_t index = 0; index < retained_lookup_bytes.size(); ++index)
            lookup_indexes[index]->accounted_bytes.fetch_add(retained_lookup_bytes[index], std::memory_order_relaxed);

        if (new_entries != 0)
        {
            /// A reader may have copied an identifier from a posting before rollback removed it.
            /// Keep the dense entry slots alive until every such reader has left its epoch.
            const UInt8 old_epoch = active_reader_epoch.fetch_xor(1, std::memory_order_acq_rel);
            UInt64 active = active_readers[old_epoch].load(std::memory_order_acquire);
            while (active != 0)
            {
                active_readers[old_epoch].wait(active, std::memory_order_relaxed);
                active = active_readers[old_epoch].load(std::memory_order_acquire);
            }
        }
        {
            std::unique_lock lock(entries_by_id_mutex);
            entries_by_id.resize(old_entries_by_id_size);
        }
        throw;
    }

    published_generation.store(new_generation, std::memory_order_release);
    next_entry_id = staged_next_entry_id;
    total_size_bytes.store(prospective_bytes, std::memory_order_relaxed);
    total_size_rows.fetch_add(new_entries, std::memory_order_relaxed);
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

    const UInt8 old_epoch = active_reader_epoch.fetch_xor(1, std::memory_order_acq_rel);
    UInt64 active = active_readers[old_epoch].load(std::memory_order_acquire);
    while (active != 0)
    {
        active_readers[old_epoch].wait(active, std::memory_order_relaxed);
        active = active_readers[old_epoch].load(std::memory_order_acquire);
    }

    for (const auto & retired_segment : retired_segments)
        reclaim_bytes += retired_segment->allocated_bytes;
    total_size_bytes.fetch_sub(reclaim_bytes, std::memory_order_relaxed);

    for (auto & mutation : mutations)
    {
        auto & entry = entries_by_id[mutation.entry_id - 1];
        std::lock_guard lock(row_mutexes[rowLockIndex(mutation.entry_id)]);
        entry.committed = std::move(*entry.pending);
        entry.pending.reset();
        entry.pending_generation = 0;
    }
}

StorageOverwriteCache::ReadResult StorageOverwriteCache::getRowsForPrimaryKeys(const std::vector<String> & serialized_keys) const
{
    ReadResult result;
    result.guard = std::make_shared<ReadGuard>(*this);
    result.rows.reserve(serialized_keys.size());
    std::unordered_set<EntryId> seen;
    for (const auto & key : serialized_keys)
    {
        const auto entry = findEntry(key);
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
        const auto & shard = index->shards[postingShardIndex(key)];
        std::shared_lock lock(shard.mutex);
        if (const auto it = shard.postings.find(key); it != shard.postings.end())
            result += it->second.size();
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
        const auto & shard = index->shards[postingShardIndex(key)];
        std::shared_lock lock(shard.mutex);
        if (const auto it = shard.postings.find(key); it != shard.postings.end())
        {
            it->second.forEach([&](EntryId entry_id)
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
        const auto & shard = index->shards[postingShardIndex(key)];
        std::shared_lock lock(shard.mutex);
        const auto it = shard.postings.find(key);
        if (it == shard.postings.end())
            continue;
        for (size_t position = 0; position < entry_ids.size(); ++position)
        {
            if ((position & 4095) == 0 && CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled while intersecting `OverwriteCache` postings");
            if (!matched[position] && it->second.contains(entry_ids[position]))
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
            key_column_types[key_index]->getDefaultSerialization()->serializeBinary(*keys[key_index].column, row, out, {});
        serialized_keys.push_back(out.str());
    }

    ReadGuard read_guard(*this);
    std::vector<RowDataPtr> resolved_rows(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (const auto entry = findEntry(serialized_keys[row]))
            resolved_rows[row] = resolveEntry(*entry, read_guard.generation());
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
            insertValueIntoColumn(*resolved_rows[row], result_positions[column], *result_columns[column]);
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
            decltype(shard.postings) empty;
            shard.postings.swap(empty);
        }
        index->accounted_bytes.store(0, std::memory_order_relaxed);
    }
    for (auto & shard : primary_shards)
    {
        std::unique_lock lock(shard.mutex);
        decltype(shard.entries) empty;
        shard.entries.swap(empty);
    }
    {
        std::unique_lock lock(entries_by_id_mutex);
        decltype(entries_by_id) empty;
        entries_by_id.swap(empty);
    }
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
        const UInt8 old_epoch = active_reader_epoch.fetch_xor(1, std::memory_order_acq_rel);
        FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_after_drop_index_publication);
        UInt64 active = active_readers[old_epoch].load(std::memory_order_acquire);
        while (active != 0)
        {
            active_readers[old_epoch].wait(active, std::memory_order_relaxed);
            active = active_readers[old_epoch].load(std::memory_order_acquire);
        }
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
    std::unique_ptr<ReadGuard> snapshot_guard;
    {
        std::lock_guard writer_lock(writer_mutex);
        {
            std::shared_lock lock(entries_by_id_mutex);
            snapshot_entry_count = entries_by_id.size();
        }
        snapshot_guard = std::make_unique<ReadGuard>(*this);
    }
    for (EntryId entry_id = 1; entry_id <= snapshot_entry_count; ++entry_id)
    {
        const auto row = resolveEntry(entry_id, snapshot_guard->generation());
        if (!row)
            continue;
        String key = serializeRowColumns(*row, positions);
        auto & posting = shadow->shards[postingShardIndex(key)].postings[key];
        posting.push_back(entry_id);
    }
    snapshot_guard.reset();

    FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_during_index_build);

    fiu_do_on(FailPoints::overwrite_cache_throw_during_index_build, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure during `OverwriteCache` lookup-index build");
    });

    std::unique_lock writer_lock(writer_mutex);
    size_t catch_up_entry_count = 0;
    {
        std::shared_lock lock(entries_by_id_mutex);
        catch_up_entry_count = entries_by_id.size();
    }
    ReadGuard catch_up_guard(*this);
    for (EntryId entry_id = snapshot_entry_count + 1; entry_id <= catch_up_entry_count; ++entry_id)
    {
        const auto row = resolveEntry(entry_id, catch_up_guard.generation());
        if (!row)
            continue;
        String key = serializeRowColumns(*row, positions);
        auto & posting = shadow->shards[postingShardIndex(key)].postings[key];
        posting.push_back(entry_id);
    }
    for (const auto & shard : shadow->shards)
    {
        const UInt64 bucket_bytes = static_cast<UInt64>(shard.postings.bucket_count()) * sizeof(void *);
        if (index_bytes > std::numeric_limits<UInt64>::max() - bucket_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
        index_bytes += bucket_bytes;
        for (const auto & [key, posting] : shard.postings)
        {
            const UInt64 posting_bytes
                = key.size() + sizeof(std::pair<const String, PostingShard::Posting>) + 64 + posting.allocatedBytes();
            if (index_bytes > std::numeric_limits<UInt64>::max() - posting_bytes)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "`OverwriteCache` lookup-index memory accounting overflow");
            index_bytes += posting_bytes;
        }
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
                args.storage_def->settings ? args.storage_def->settings->clone() : nullptr);
        },
        {
            .supports_settings = true,
            .supports_keys = true,
            .supports_lookup_indexes = true,
            .has_builtin_setting_fn = isOverwriteCacheSetting,
        });
}

}
