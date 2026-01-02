#include <Storages/MergeTree/VerticalInsertTask.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/TTLDescription.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Settings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <deque>

namespace ProfileEvents
{
    extern const Event VerticalInsertRows;
    extern const Event VerticalInsertBytes;
    extern const Event VerticalInsertMergingColumns;
    extern const Event VerticalInsertGatheringColumns;
    extern const Event VerticalInsertWritersCreated;
    extern const Event VerticalInsertWriterFlushes;
    extern const Event VerticalInsertTotalMilliseconds;
    extern const Event VerticalInsertHorizontalPhaseMilliseconds;
    extern const Event VerticalInsertVerticalPhaseMilliseconds;
    extern const Event VerticalInsertSkipped;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

namespace
{

class DisjointSet
{
public:
    explicit DisjointSet(size_t size) : parent(size), rank(size, 0)
    {
        for (size_t i = 0; i < size; ++i)
            parent[i] = i;
    }

    size_t find(size_t x)
    {
        if (parent[x] != x)
            parent[x] = find(parent[x]);
        return parent[x];
    }

    void unite(size_t a, size_t b)
    {
        a = find(a);
        b = find(b);
        if (a == b)
            return;
        if (rank[a] < rank[b])
            std::swap(a, b);
        parent[b] = a;
        if (rank[a] == rank[b])
            ++rank[a];
    }

private:
    std::vector<size_t> parent;
    std::vector<size_t> rank;
};

std::vector<String> getArraySizeStreams(
    const NameAndTypePair & column,
    const Block & block,
    const SerializationByName & serializations,
    const MergeTreeSettings & settings)
{
    auto it = serializations.find(column.name);
    if (it == serializations.end())
        return {};

    const auto & serialization = it->second;
    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Wide;

    auto data = ISerialization::SubstreamData(serialization).withType(column.type);
    if (const auto * col = block.findByName(column.name))
        data = data.withColumn(col->column);

    std::vector<String> streams;
    serialization->enumerateStreams(enumerate_settings,
        [&](const ISerialization::SubstreamPath & substream_path)
        {
            if (substream_path.empty())
                return;
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;
            if (substream_path.back().type != ISerialization::Substream::ArraySizes)
                return;

            streams.emplace_back(ISerialization::getFileNameForStream(
                column, substream_path, ISerialization::StreamFileNameSettings(settings)));
        },
        data);

    return streams;
}

String getColumnNameInStorage(const String & name, const NameSet & storage_columns_name_set)
{
    if (storage_columns_name_set.contains(name))
        return name;
    return String(Nested::getColumnFromSubcolumn(name, storage_columns_name_set));
}

void addColumnNameOrBase(const String & name, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    key_columns.insert(getColumnNameInStorage(name, storage_columns_name_set));
}

bool addColumnNameOrBaseIfExists(const String & name, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    if (storage_columns_name_set.contains(name))
    {
        key_columns.insert(name);
        return true;
    }

    auto split = Nested::splitName(name);
    if (split.second.empty())
        return false;

    key_columns.insert(String(Nested::getColumnFromSubcolumn(name, storage_columns_name_set)));
    return true;
}

void addColumnsFromTTLDescription(const TTLDescription & ttl, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & column : ttl.expression_columns)
        addColumnNameOrBase(column.name, storage_columns_name_set, key_columns);

    for (const auto & column : ttl.where_expression_columns)
        addColumnNameOrBase(column.name, storage_columns_name_set, key_columns);

    for (const auto & name : ttl.group_by_keys)
        addColumnNameOrBaseIfExists(name, storage_columns_name_set, key_columns);
}

void addColumnsFromTTLDescriptions(const TTLDescriptions & ttls, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & ttl : ttls)
        addColumnsFromTTLDescription(ttl, storage_columns_name_set, key_columns);
}

void addColumnsFromTTLColumnsDescription(const TTLColumnsDescription & ttls, const NameSet & storage_columns_name_set, NameSet & key_columns)
{
    for (const auto & [name, ttl] : ttls)
    {
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);
        addColumnsFromTTLDescription(ttl, storage_columns_name_set, key_columns);
    }
}

void addNestedKeyColumns(const String & name, const NamesAndTypesList & storage_columns, NameSet & key_columns)
{
    auto split = Nested::splitName(name);
    if (split.second.empty())
        return;

    const String prefix = split.first + ".";
    for (const auto & column : storage_columns)
    {
        if (startsWith(column.name, prefix))
            key_columns.insert(column.name);
    }
}

NameSet getHorizontalPhaseColumns(
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const MergeTreeIndices & horizontal_skip_indexes,
    const MergeTreeData::MergingParams & merging_params)
{
    NameSet key_columns;
    const auto & sorting_key_expr = metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();
    auto storage_columns_name_set = storage_columns.getNameSet();

    /// getRequiredColumns() is expected to resolve aliases, but we still map to storage columns.
    for (const auto & name : sort_key_columns_vec)
    {
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);
        addNestedKeyColumns(name, storage_columns, key_columns);
    }

    /// Force sign column for Collapsing mode and VersionedCollapsing mode
    if (!merging_params.sign_column.empty())
        addColumnNameOrBase(merging_params.sign_column, storage_columns_name_set, key_columns);

    /// Force is_deleted column for Replacing mode
    if (!merging_params.is_deleted_column.empty())
        addColumnNameOrBase(merging_params.is_deleted_column, storage_columns_name_set, key_columns);

    /// Force version column for Replacing mode and VersionedCollapsing mode
    if (!merging_params.version_column.empty())
        addColumnNameOrBase(merging_params.version_column, storage_columns_name_set, key_columns);

    /// Force all columns params of Graphite mode
    if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        addColumnNameOrBase(merging_params.graphite_params.path_column_name, storage_columns_name_set, key_columns);
        addColumnNameOrBase(merging_params.graphite_params.time_column_name, storage_columns_name_set, key_columns);
        addColumnNameOrBase(merging_params.graphite_params.value_column_name, storage_columns_name_set, key_columns);
        addColumnNameOrBase(merging_params.graphite_params.version_column_name, storage_columns_name_set, key_columns);
    }

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty() && !storage_columns.empty())
        key_columns.emplace(storage_columns.front().name);

    /// Include partition key columns to keep minmax index inputs in horizontal phase
    const auto minmax_columns = MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey());
    for (const auto & name : minmax_columns)
        addColumnNameOrBase(name, storage_columns_name_set, key_columns);

    /// Include TTL expression columns to keep TTL inputs in horizontal phase
    addColumnsFromTTLDescription(metadata_snapshot->getRowsTTL(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRowsWhereTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getMoveTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getRecompressionTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLDescriptions(metadata_snapshot->getGroupByTTLs(), storage_columns_name_set, key_columns);
    addColumnsFromTTLColumnsDescription(metadata_snapshot->getColumnTTLs(), storage_columns_name_set, key_columns);

    /// Add columns from skip indexes that must be built in horizontal phase.
    for (const auto & index : horizontal_skip_indexes)
    {
        const auto & index_desc = index->index;
        auto index_columns = index_desc.expression->getRequiredColumns();
        for (const auto & col : index_columns)
            addColumnNameOrBase(col, storage_columns_name_set, key_columns);
    }

    return key_columns;
}

void addSubcolumnsFromExpression(const ExpressionActionsPtr & expr, Block & block)
{
    for (const auto & required_column : expr->getRequiredColumns())
    {
        if (!block.has(required_column))
            block.insert(block.getSubcolumnByName(required_column));
    }
}

struct IndexColumnsPlan
{
    Names columns;
    ExpressionActionsPtr expression;
};

struct HorizontalPhasePlan
{
    NameSet horizontal_columns;
    MergeTreeIndices horizontal_skip_indexes;
    std::unordered_map<String, MergeTreeIndices> skip_indexes_by_column;
};

IndexColumnsPlan buildIndexColumnsPlan(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & horizontal_skip_indexes,
    MergeTreeData & data,
    const Block & block)
{
    /// Build index columns list and compute key expressions only if needed.
    /// Mirrors non-vertical insert, which materializes key/index expressions into the block before writing.
    IndexColumnsPlan plan;
    /// Primary key is a prefix of sorting key.
    plan.columns = metadata_snapshot->getPrimaryKeyColumns();
    if (plan.columns.empty())
        plan.columns = metadata_snapshot->getSortingKeyColumns();

    NameSet index_column_set(plan.columns.begin(), plan.columns.end());
    for (const auto & index : horizontal_skip_indexes)
    {
        for (const auto & name : index->index.column_names)
        {
            if (index_column_set.emplace(name).second)
                plan.columns.push_back(name);
        }
    }

    bool need_index_expr = false;
    for (const auto & name : plan.columns)
    {
        if (!block.has(name))
        {
            need_index_expr = true;
            break;
        }
    }

    if (need_index_expr)
    {
        if (metadata_snapshot->hasPrimaryKey())
            plan.expression = data.getPrimaryKeyAndSkipIndicesExpression(metadata_snapshot, horizontal_skip_indexes);
        else
            plan.expression = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, horizontal_skip_indexes);
    }

    return plan;
}

HorizontalPhasePlan buildHorizontalPhasePlan(
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const MergeTreeIndices & skip_indexes,
    const MergeTreeData::MergingParams & merging_params)
{
    HorizontalPhasePlan plan;
    const auto storage_columns_name_set = storage_columns.getNameSet();

    /// Classify skip indexes: single-column ones go to vertical phase,
    /// multi-column ones go to horizontal phase.
    for (const auto & index : skip_indexes)
    {
        const auto & index_desc = index->index;
        auto index_columns = index_desc.expression->getRequiredColumns();

        if (index_columns.size() == 1)
        {
            const auto & column_name = index_columns.front();
            String actual_column = getColumnNameInStorage(column_name, storage_columns_name_set);
            plan.skip_indexes_by_column[actual_column].push_back(index);
        }
        else
        {
            plan.horizontal_skip_indexes.push_back(index);
        }
    }

    plan.horizontal_columns = getHorizontalPhaseColumns(
        metadata_snapshot,
        storage_columns,
        plan.horizontal_skip_indexes,
        merging_params);

    return plan;
}

}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 enable_vertical_insert_algorithm;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_rows_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_columns_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_min_bytes_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_insert_algorithm_columns_batch_size;
    extern const MergeTreeSettingsUInt64 min_bytes_for_wide_part;
    extern const MergeTreeSettingsUInt64 min_rows_for_wide_part;
    extern const MergeTreeSettingsBool fsync_after_insert;
}

namespace Setting
{
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
}

VerticalInsertTask::VerticalInsertTask(
    MergeTreeData & data_,
    MergeTreeSettingsPtr data_settings_,
    const StorageMetadataPtr & metadata_snapshot_,
    MergeTreeMutableDataPartPtr new_data_part_,
    Block block_,
    const IColumn::Permutation * permutation_,
    MergeTreeIndices skip_indexes_,
    ColumnsStatistics statistics_,
    CompressionCodecPtr codec_,
    MergeTreeIndexGranularityPtr index_granularity_,
    const Settings & settings_,
    ContextPtr context_)
    : data(data_)
    , data_settings(std::move(data_settings_))
    , metadata_snapshot(metadata_snapshot_)
    , new_data_part(std::move(new_data_part_))
    , block(std::move(block_))
    , block_rows(0)
    , block_bytes(0)
    , permutation(permutation_)
    , has_permutation(false)
    , skip_indexes(std::move(skip_indexes_))
    , statistics(std::move(statistics_))
    , codec(std::move(codec_))
    , index_granularity(std::move(index_granularity_))
    , settings(settings_)
    , context(context_)
    , log(getLogger("VerticalInsertTask"))
{
    block_rows = block.rows();
    block_bytes = block.bytes();
    if (permutation_ && !permutation_->empty())
    {
        bool permutation_is_identity = true;
        for (size_t i = 0, size = permutation_->size(); i < size; ++i)
        {
            if ((*permutation_)[i] != i)
            {
                permutation_is_identity = false;
                break;
            }
        }
        has_permutation = !permutation_is_identity;
    }

    all_column_names = new_data_part->getColumns().getNames();
    classifyColumns();

    LOG_DEBUG(log, "Vertical insert for part {}: {} merging columns, {} gathering columns, batch_size={}, has_permutation={}, block_rows={}, block_bytes={}",
        new_data_part->name,
        merging_columns.size(),
        gathering_columns.size(),
        settings.columns_batch_size,
        has_permutation,
        block_rows,
        block_bytes);
}

void VerticalInsertTask::classifyColumns()
{
    /// Decide which physical columns stay in horizontal phase vs. vertical phase.
    /// Key/index expression columns are materialized later, right before writing.
    auto storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    storage_column_names = storage_columns.getNameSet();
    buildOffsetGroups(storage_columns);
    auto horizontal_plan = buildHorizontalPhasePlan(
        metadata_snapshot,
        storage_columns,
        skip_indexes,
        data.merging_params);
    NameSet horizontal_columns = std::move(horizontal_plan.horizontal_columns);
    horizontal_skip_indexes = std::move(horizontal_plan.horizontal_skip_indexes);
    skip_indexes_by_column = std::move(horizontal_plan.skip_indexes_by_column);

    /// Ensure array-size groups stay in the same phase.
    NameSet grouped_horizontal = horizontal_columns;
    for (const auto & group : offset_groups)
    {
        bool any_horizontal = false;
        for (const auto & column : group)
        {
            if (horizontal_columns.contains(column.name))
            {
                any_horizontal = true;
                break;
            }
        }
        if (!any_horizontal)
            continue;
        for (const auto & column : group)
            grouped_horizontal.insert(column.name);
    }
    horizontal_columns = std::move(grouped_horizontal);

    /// Classify statistics by column
    for (const auto & stat : statistics)
    {
        const String & stat_column = stat->getColumnName();
        statistics_by_column[stat_column].push_back(stat);
    }

    /// Partition columns into merging and gathering
    for (const auto & column : storage_columns)
    {
        if (!block.has(column.name))
            continue;  /// Skip columns not in this block

        if (horizontal_columns.contains(column.name))
        {
            merging_columns.emplace_back(column);

            /// If column is in horizontal stage, move its indexes there too
            auto it = skip_indexes_by_column.find(column.name);
            if (it != skip_indexes_by_column.end())
            {
                for (auto & idx : it->second)
                    horizontal_skip_indexes.push_back(std::move(idx));
                skip_indexes_by_column.erase(it);
            }

            auto stat_it = statistics_by_column.find(column.name);
            if (stat_it != statistics_by_column.end())
            {
                for (auto & stat : stat_it->second)
                    merging_statistics.push_back(std::move(stat));
                statistics_by_column.erase(stat_it);
            }
        }
        else
        {
            gathering_columns.emplace_back(column);
        }
    }

    /// Track only expression columns needed for vertical-phase skip indexes.
    index_expression_refcount.clear();
    for (const auto & [column_name, indexes] : skip_indexes_by_column)
    {
        for (const auto & index : indexes)
        {
            for (const auto & name : index->index.column_names)
            {
                if (!storage_column_names.contains(name))
                    ++index_expression_refcount[name];
            }
        }
    }
}

void VerticalInsertTask::buildOffsetGroups(const NamesAndTypesList & storage_columns)
{
    offset_groups.clear();
    offset_group_by_column.clear();

    std::vector<NameAndTypePair> block_columns;
    std::vector<std::vector<String>> array_size_streams;
    block_columns.reserve(storage_columns.size());
    array_size_streams.reserve(storage_columns.size());

    const auto & serializations = new_data_part->getSerializations();
    for (const auto & column : storage_columns)
    {
        if (!block.has(column.name))
            continue;
        block_columns.emplace_back(column);
        array_size_streams.emplace_back(getArraySizeStreams(column, block, serializations, *data_settings));
    }

    DisjointSet groups(block_columns.size());
    std::unordered_map<String, size_t> stream_owner;
    for (size_t i = 0; i < block_columns.size(); ++i)
    {
        for (const auto & stream_name : array_size_streams[i])
        {
            auto [it, inserted] = stream_owner.emplace(stream_name, i);
            if (!inserted)
                groups.unite(i, it->second);
        }
    }

    std::unordered_map<size_t, size_t> group_index_by_root;
    for (size_t i = 0; i < block_columns.size(); ++i)
    {
        size_t root = groups.find(i);
        auto [it, inserted] = group_index_by_root.emplace(root, offset_groups.size());
        if (inserted)
            offset_groups.emplace_back();
        size_t group_index = it->second;
        offset_groups[group_index].emplace_back(block_columns[i]);
        offset_group_by_column.emplace(block_columns[i].name, group_index);
    }
}

void VerticalInsertTask::cancel() noexcept
{
    try
    {
        if (horizontal_output)
            horizontal_output->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Vertical insert: failed to cancel horizontal output");
    }

    try
    {
        if (active_column_writer)
            active_column_writer->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Vertical insert: failed to cancel active column writer");
    }
    active_column_writer = nullptr;

    for (auto & entry : delayed_streams)
    {
        try
        {
            if (entry.stream)
                entry.stream->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Vertical insert: failed to cancel delayed column writer");
        }
    }
    delayed_streams.clear();
    vertical_substreams_by_column.clear();
}

void VerticalInsertTask::execute()
{
    if (has_permutation && permutation && permutation->size() != block_rows)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Vertical insert: permutation size {} does not match block rows {}",
            permutation->size(),
            block_rows);
    }

    ProfileEventTimeIncrement<Time::Milliseconds> total_watch(ProfileEvents::VerticalInsertTotalMilliseconds);
    bool completed = false;
    SCOPE_EXIT({
        if (!completed)
            cancel();
    });

    /// Phase 1: write key/special columns and build indices.
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertHorizontalPhaseMilliseconds);
        executeHorizontalPhase();
    }

    /// Phase 2: write remaining columns in batches, releasing memory early.
    {
        ProfileEventTimeIncrement<Time::Milliseconds> phase_watch(ProfileEvents::VerticalInsertVerticalPhaseMilliseconds);
        executeVerticalPhase();
    }

    ProfileEvents::increment(ProfileEvents::VerticalInsertRows, block_rows);
    ProfileEvents::increment(ProfileEvents::VerticalInsertBytes, block_bytes);
    ProfileEvents::increment(ProfileEvents::VerticalInsertMergingColumns, merging_columns.size());
    ProfileEvents::increment(ProfileEvents::VerticalInsertGatheringColumns, gathering_columns.size());

    completed = true;
}

void VerticalInsertTask::executeHorizontalPhase()
{
    /// Prepare a minimal block for key/skip-index writes and release those columns.
    if (!index_granularity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not set for vertical insert");

    /// Materialize sorting key and horizontal skip index expressions into the block.
    if (metadata_snapshot->hasSortingKey() || !horizontal_skip_indexes.empty())
    {
        auto expr = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, horizontal_skip_indexes);
        addSubcolumnsFromExpression(expr, block);
        expr->execute(block);
    }

    /// Ensure primary key and skip index expression columns are present for index calculation.
    auto index_plan = buildIndexColumnsPlan(metadata_snapshot, horizontal_skip_indexes, data, block);
    if (index_plan.expression)
    {
        addSubcolumnsFromExpression(index_plan.expression, block);
        index_plan.expression->execute(block);
    }

    /// Extract only merging columns from block
    Block merging_block;
    for (const auto & column : merging_columns)
    {
        if (auto * col = block.findByName(column.name))
            merging_block.insert({col->column, col->type, col->name});
    }

    /// Add primary key/skip index expression columns if they were computed in the block
    for (const auto & name : index_plan.columns)
    {
        if (merging_block.has(name))
            continue;
        if (auto * col = block.findByName(name))
            merging_block.insert({col->column, col->type, col->name});
    }

    /// Create the horizontal output stream for merging columns
    horizontal_output = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        data_settings,
        metadata_snapshot,
        merging_columns,
        horizontal_skip_indexes,
        merging_statistics,
        codec,
        index_granularity,
        context->getCurrentTransaction() ? context->getCurrentTransaction()->tid : Tx::PrehistoricTID,
        merging_block.bytes(),
        /*reset_columns=*/ false,
        /*blocks_are_granules_size=*/ false,
        context->getWriteSettings());

    if (!horizontal_output->getIndexGranularity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not initialized for horizontal output in vertical insert");

    /// Write merging columns with permutation.
    /// Use the full block so adaptive granularity reflects total row size.
    if (has_permutation)
        horizontal_output->writeWithPermutation(block, permutation);
    else
        horizontal_output->write(block);

    /// Release merging columns from original block to free memory
    for (const auto & column : merging_columns)
    {
        if (auto * col = block.findByName(column.name))
            col->column.reset();
    }

    /// Release primary key/skip index expression columns from original block to free memory
    for (const auto & name : index_plan.columns)
    {
        if (index_expression_refcount.contains(name))
            continue;
        if (auto * col = block.findByName(name))
            col->column.reset();
    }

    /// Release temporary expression columns (sorting key / horizontal skip indexes) not needed in vertical phase.
    for (auto & column : block)
    {
        if (storage_column_names.contains(column.name))
            continue;
        if (index_expression_refcount.contains(column.name))
            continue;
        if (column.column)
            column.column.reset();
    }
}

void VerticalInsertTask::addIndexExpressionColumns(
    const MergeTreeIndexPtr & index,
    Block & batch_block,
    std::unordered_map<String, size_t> & batch_expression_usage)
{
    bool need_expression = false;
    for (const auto & name : index->index.column_names)
    {
        if (storage_column_names.contains(name))
            continue;
        const auto * col = block.findByName(name);
        if (!col || !col->column)
        {
            need_expression = true;
            break;
        }
    }

    if (need_expression)
    {
        if (!index->index.expression)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert: missing expression for skip index {}",
                index->index.name);
        }
        addSubcolumnsFromExpression(index->index.expression, block);
        index->index.expression->execute(block);
    }

    for (const auto & name : index->index.column_names)
    {
        if (storage_column_names.contains(name))
            continue;

        auto * col = block.findByName(name);
        if (!col || !col->column)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert: missing index expression column {} in block",
                name);
        }

        if (!batch_block.has(name))
            batch_block.insert({col->column, col->type, col->name});

        ++batch_expression_usage[name];
    }
}

void VerticalInsertTask::releaseIndexExpressionColumns(const std::unordered_map<String, size_t> & batch_expression_usage)
{
    for (const auto & [name, count] : batch_expression_usage)
    {
        auto it = index_expression_refcount.find(name);
        if (it == index_expression_refcount.end())
            continue;

        if (it->second <= count)
        {
            if (auto * col = block.findByName(name))
                col->column.reset();
            index_expression_refcount.erase(it);
        }
        else
        {
            it->second -= count;
        }
    }
}

struct VerticalInsertTask::VerticalPhaseState
{
    VerticalInsertTask & task;
    const size_t batch_column_limit;
    const size_t batch_bytes_limit;
    size_t batch_column_count = 0;
    size_t batch_bytes_total = 0;
    const bool need_sync;
    MergeTreeIndices batch_skip_indexes;
    ColumnsStatistics batch_statistics;
    size_t max_delayed_streams = 0;
    size_t delayed_open_streams = 0;
    NamesAndTypesList batch_column_list;
    Block batch_block;
    std::unordered_map<String, size_t> batch_expression_usage;

    explicit VerticalPhaseState(
        VerticalInsertTask & task_,
        size_t batch_column_limit_,
        size_t batch_bytes_limit_,
        bool need_sync_)
        : task(task_)
        , batch_column_limit(batch_column_limit_)
        , batch_bytes_limit(batch_bytes_limit_)
        , need_sync(need_sync_)
    {
    }

    void init()
    {
        /// Reset reusable batch state and read delayed-stream policy once.
        resetBatch();
        task.vertical_substreams_by_column.clear();
        task.active_column_writer = nullptr;
        task.vertical_streams_created = 0;
        task.delayed_streams.clear();

        const auto & query_settings = task.context->getSettingsRef();
        if (query_settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
            max_delayed_streams = query_settings[Setting::max_insert_delayed_streams_for_parallel_write];
    }

    void resetBatch()
    {
        /// Reuse buffers between batches.
        batch_block.clear();
        batch_column_list.clear();
        batch_skip_indexes.clear();
        batch_statistics.clear();
        batch_expression_usage.clear();
        batch_column_count = 0;
        batch_bytes_total = 0;
    }

    void enqueueDelayed(DelayedColumnStream && delayed)
    {
        /// Either finalize immediately or cap concurrent open streams.
        if (max_delayed_streams == 0)
        {
            task.finalizeDelayedStream(delayed, need_sync);
            return;
        }

        delayed_open_streams += delayed.open_streams;
        task.delayed_streams.emplace_back(std::move(delayed));
        while (delayed_open_streams > max_delayed_streams)
        {
            auto & front = task.delayed_streams.front();
            delayed_open_streams -= front.open_streams;
            task.finalizeDelayedStream(front, need_sync);
            task.delayed_streams.pop_front();
        }
    }

    void flushBatch()
    {
        /// Write current batch and move stream to delayed/finalized state.
        if (batch_column_list.empty())
            return;

        if (batch_block.rows() != task.block_rows)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert batch validation: block has {} rows, batch has {} rows",
                task.block_rows,
                batch_block.rows());
        }

        const size_t batch_uncompressed_bytes = batch_block.bytes();
        auto batch_out = std::make_unique<MergedColumnOnlyOutputStream>(
            task.new_data_part,
            task.data_settings,
            task.metadata_snapshot,
            batch_column_list,
            batch_skip_indexes,
            batch_statistics,
            task.codec,
            task.horizontal_output->getIndexGranularity(),
            batch_uncompressed_bytes);

        ProfileEvents::increment(ProfileEvents::VerticalInsertWritersCreated);
        ProfileEvents::increment(ProfileEvents::VerticalInsertWriterFlushes);

        task.active_column_writer = batch_out.get();
        SCOPE_EXIT({
            task.active_column_writer = nullptr;
        });
        try
        {
            if (task.has_permutation)
                batch_out->writeWithPermutation(batch_block, task.permutation);
            else
                batch_out->write(batch_block);
        }
        catch (...)
        {
            tryLogCurrentException(task.log, "Vertical insert: failed to write column batch");
            throw;
        }

        task.releaseIndexExpressionColumns(batch_expression_usage);
        resetBatch();

        DelayedColumnStream delayed;
        delayed.stream = std::move(batch_out);
        auto changed_checksums = delayed.stream->fillChecksums(task.new_data_part, task.vertical_checksums);
        delayed.checksums.add(std::move(changed_checksums));
        delayed.substreams = delayed.stream->getColumnsSubstreams();
        delayed.open_streams = delayed.stream->getNumberOfOpenStreams();
        task.vertical_streams_created += delayed.open_streams;

        enqueueDelayed(std::move(delayed));
    }

    void addColumn(const NameAndTypePair & column)
    {
        /// Move column into batch; caller enforces offset-group boundaries.
        auto * col_data = task.block.findByName(column.name);
        if (!col_data || !col_data->column)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert: missing column {} in batch block",
                column.name);
        }

        const size_t column_rows = col_data->column ? col_data->column->size() : 0;
        if (column_rows != task.block_rows)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert column validation: column {} has {} rows, expected {}",
                col_data->name,
                column_rows,
                task.block_rows);
        }

        ColumnPtr column_data = col_data->column;
        const size_t column_bytes = column_data ? column_data->byteSize() : 0;

        batch_block.insert({column_data, col_data->type, col_data->name});
        batch_column_list.push_back(column);
        batch_bytes_total += column_bytes;

        auto it_idx = task.skip_indexes_by_column.find(column.name);
        if (it_idx != task.skip_indexes_by_column.end())
        {
            for (auto & idx : it_idx->second)
            {
                batch_skip_indexes.push_back(std::move(idx));
                task.addIndexExpressionColumns(batch_skip_indexes.back(), batch_block, batch_expression_usage);
            }
            task.skip_indexes_by_column.erase(it_idx);
        }

        auto it_stat = task.statistics_by_column.find(column.name);
        if (it_stat != task.statistics_by_column.end())
        {
            for (auto & stat : it_stat->second)
                batch_statistics.push_back(std::move(stat));
            task.statistics_by_column.erase(it_stat);
        }

        col_data->column.reset();
        ++batch_column_count;
    }

    void addColumnGroup(const NamesAndTypesList & group)
    {
        if (group.empty())
            return;

        size_t group_columns = 0;
        size_t group_bytes = 0;
        for (const auto & column : group)
        {
            const auto * col_data = task.block.findByName(column.name);
            if (!col_data || !col_data->column)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Vertical insert: missing column {} in group",
                    column.name);
            }
            group_bytes += col_data->column->byteSize();
            ++group_columns;
        }

        bool should_flush = false;
        if (!batch_column_list.empty() && batch_column_limit > 0 && batch_column_count + group_columns > batch_column_limit)
            should_flush = true;
        if (!batch_column_list.empty() && batch_bytes_limit > 0 && batch_bytes_total + group_bytes > batch_bytes_limit)
            should_flush = true;

        if (should_flush)
            flushBatch();

        for (const auto & column : group)
            addColumn(column);
    }

    void finish()
    {
        /// Flush remaining batch and finalize delayed streams/substreams.
        flushBatch();

        if (!task.skip_indexes_by_column.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Vertical insert: unprocessed skip index columns remain: {}", task.skip_indexes_by_column.size());
        if (!task.statistics_by_column.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Vertical insert: unprocessed statistics columns remain: {}", task.statistics_by_column.size());

        for (const auto & column : task.block)
        {
            if (column.column)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Vertical insert: column {} still holds data after vertical phase", column.name);
        }

        task.block.clear();
        task.skip_indexes_by_column.clear();
        task.statistics_by_column.clear();

        for (auto & stream : task.delayed_streams)
            task.finalizeDelayedStream(stream, need_sync);
        task.delayed_streams.clear();

        if (!task.vertical_substreams_by_column.empty())
        {
            ColumnsSubstreams merged;
            for (const auto & column_name : task.all_column_names)
            {
                auto it = task.vertical_substreams_by_column.find(column_name);
                if (it == task.vertical_substreams_by_column.end())
                    continue;
                merged.addColumn(column_name);
                merged.addSubstreamsToLastColumn(it->second);
            }
            task.vertical_columns_substreams = std::move(merged);
        }
        task.vertical_substreams_by_column.clear();
    }
};

void VerticalInsertTask::executeVerticalPhase()
{
    /// Stream non-key columns in batches with optional delayed finalization.
    const size_t batch_column_limit = std::max<size_t>(1, settings.columns_batch_size);
    const size_t batch_bytes_limit = settings.columns_batch_bytes;
    const bool need_sync = (*data_settings)[MergeTreeSetting::fsync_after_insert];

    if (!horizontal_output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Horizontal output stream is not initialized for vertical insert");
    if (!horizontal_output->getIndexGranularity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not initialized for vertical insert column writer");

    chassert(delayed_streams.empty());

    VerticalPhaseState state(*this, batch_column_limit, batch_bytes_limit, need_sync);
    state.init();

    /// Keep all columns that share array-size streams in the same batch.
    std::vector<size_t> group_order;
    group_order.reserve(offset_groups.size());
    std::vector<bool> group_seen(offset_groups.size(), false);
    for (const auto & column : gathering_columns)
    {
        auto it = offset_group_by_column.find(column.name);
        if (it == offset_group_by_column.end() || it->second >= offset_groups.size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Vertical insert: missing offset group for column {}",
                column.name);
        }
        const size_t group_index = it->second;
        if (!group_seen[group_index])
        {
            group_seen[group_index] = true;
            group_order.push_back(group_index);
        }
    }

    for (const auto group_index : group_order)
        state.addColumnGroup(offset_groups[group_index]);

    state.finish();
}

void VerticalInsertTask::finalizeDelayedStream(DelayedColumnStream & delayed, bool need_sync)
{
    delayed.stream->finish(need_sync);
    vertical_checksums.add(std::move(delayed.checksums));
    if (!delayed.substreams.empty())
    {
        for (const auto & [column_name, substreams] : delayed.substreams.getColumnsSubstreams())
        {
            auto [it, inserted] = vertical_substreams_by_column.emplace(column_name, substreams);
            if (!inserted)
            {
                auto & existing = it->second;
                for (const auto & substream : substreams)
                {
                    if (std::find(existing.begin(), existing.end(), substream) == existing.end())
                        existing.emplace_back(substream);
                }
            }
        }
    }
}

bool shouldUseVerticalInsert(
    const Block & block,
    const NamesAndTypesList & storage_columns,
    const MergeTreeData::MergingParams & merging_params,
    const MergeTreeIndices & skip_indexes,
    const MergeTreeSettingsPtr & settings,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPart::Type part_type)
{
    /// Check master switch
    if (!(*settings)[MergeTreeSetting::enable_vertical_insert_algorithm])
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Only wide parts support vertical insert
    if (part_type != MergeTreeData::DataPart::Type::Wide)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Respect wide-part thresholds to avoid surprising behavior
    const size_t rows = block.rows();
    const size_t bytes = block.bytes();
    const size_t min_rows_for_wide_part = (*settings)[MergeTreeSetting::min_rows_for_wide_part];
    const size_t min_bytes_for_wide_part = (*settings)[MergeTreeSetting::min_bytes_for_wide_part];
    if (rows < min_rows_for_wide_part && bytes < min_bytes_for_wide_part)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Check row threshold
    const size_t min_rows = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_rows_to_activate];
    if (rows < min_rows)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Check byte threshold (if set)
    const size_t min_bytes = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_bytes_to_activate];
    if (min_bytes > 0 && bytes < min_bytes)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    /// Count gathering columns
    size_t gathering_count = countGatheringColumns(storage_columns, merging_params, metadata_snapshot, skip_indexes);

    const size_t min_columns = (*settings)[MergeTreeSetting::vertical_insert_algorithm_min_columns_to_activate];
    if (gathering_count < min_columns)
    {
        ProfileEvents::increment(ProfileEvents::VerticalInsertSkipped);
        return false;
    }

    return true;
}

size_t countGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const MergeTreeData::MergingParams & merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indexes)
{
    auto horizontal_plan = buildHorizontalPhasePlan(
        metadata_snapshot,
        storage_columns,
        skip_indexes,
        merging_params);
    const NameSet & horizontal_columns = horizontal_plan.horizontal_columns;

    const auto storage_columns_name_set = storage_columns.getNameSet();
    for (const auto & name : horizontal_columns)
        chassert(storage_columns_name_set.contains(name));

    return storage_columns.size() - horizontal_columns.size();
}

}
