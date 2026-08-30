#include <memory>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <Disks/createVolume.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/RowOrderOptimizer.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyDenseIndexOps.h>
#include <Common/ColumnsHashing.h>
#include <Common/DateLUTImpl.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/intExp.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>

#include <Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/String.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/CollapsingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/VersionedCollapsingAlgorithm.h>
#include <Processors/Merges/Algorithms/GraphiteRollupSortedAlgorithm.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>

#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event MergeTreeDataWriterBlocks;
    extern const Event MergeTreeDataWriterBlocksAlreadySorted;
    extern const Event MergeTreeDataWriterRows;
    extern const Event MergeTreeDataWriterUncompressedBytes;
    extern const Event MergeTreeDataWriterCompressedBytes;
    extern const Event MergeTreeDataWriterSortingBlocksMicroseconds;
    extern const Event MergeTreeDataWriterMergingBlocksMicroseconds;
    extern const Event MergeTreeDataWriterProjectionsCalculationMicroseconds;
    extern const Event MergeTreeDataProjectionWriterBlocks;
    extern const Event MergeTreeDataProjectionWriterBlocksAlreadySorted;
    extern const Event MergeTreeDataProjectionWriterRows;
    extern const Event MergeTreeDataProjectionWriterUncompressedBytes;
    extern const Event MergeTreeDataProjectionWriterCompressedBytes;
    extern const Event MergeTreeDataProjectionWriterSortingBlocksMicroseconds;
    extern const Event MergeTreeDataProjectionWriterMergingBlocksMicroseconds;
    extern const Event MergeTreeDataWriterStatisticsCalculationMicroseconds;
    extern const Event RejectedInserts;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool finalize_projection_parts_synchronously;
    extern const SettingsBool materialize_skip_indexes_on_insert;
    extern const SettingsString exclude_materialize_skip_indexes_on_insert;
    extern const SettingsBool materialize_statistics_on_insert;
    extern const SettingsUInt64 materialize_statistics_on_insert_max_table_size;
    extern const SettingsBool optimize_on_insert;
    extern const SettingsBool throw_on_max_partitions_per_insert_block;
    extern const SettingsUInt64 min_free_disk_bytes_to_perform_insert;
    extern const SettingsFloat min_free_disk_ratio_to_perform_insert;
    extern const SettingsUInt64 unique_key_max_encoded_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_json_shared_data_paths_repromotion;
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsBool fsync_after_insert;
    extern const MergeTreeSettingsBool fsync_part_directory;
    extern const MergeTreeSettingsBool materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsString exclude_materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsUInt64 min_free_disk_bytes_to_perform_insert;
    extern const MergeTreeSettingsFloat min_free_disk_ratio_to_perform_insert;
    extern const MergeTreeSettingsBool optimize_row_order;
    extern const MergeTreeSettingsBool compute_exact_num_defaults_for_sparse_columns;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version_for_zero_level_parts;
    extern const MergeTreeSettingsBool materialize_projections_on_insert;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PARTS;
    extern const int NOT_ENOUGH_SPACE;
}

void buildScatterSelector(
        const ColumnRawPtrs & columns,
        PODArray<size_t> & partition_num_to_first_row,
        IColumn::Selector & selector,
        size_t max_parts,
        ContextPtr context)
{
    /// Use generic hashed variant since partitioning is unlikely to be a bottleneck.
    using Data = HashMap<UInt128, size_t, UInt128TrivialHash>;
    Data partitions_map;

    size_t num_rows = columns[0]->size();
    size_t partitions_count = 0;
    size_t throw_on_limit = context->getSettingsRef()[Setting::throw_on_max_partitions_per_insert_block];

    for (size_t i = 0; i < num_rows; ++i)
    {
        Data::key_type key = ColumnsHashing::hash128(i, columns.size(), columns);
        typename Data::LookupResult it = nullptr;
        bool inserted = false;
        partitions_map.emplace(key, it, inserted);

        if (inserted)
        {
            if (max_parts && partitions_count >= max_parts && throw_on_limit)
            {
                ProfileEvents::increment(ProfileEvents::RejectedInserts);
                throw Exception(ErrorCodes::TOO_MANY_PARTS,
                                "Too many partitions for single INSERT block (more than {}). "
                                "The limit is controlled by 'max_partitions_per_insert_block' setting. "
                                "Large number of partitions is a common misconception. "
                                "It will lead to severe negative performance impact, including slow server startup, "
                                "slow INSERT queries and slow SELECT queries. Recommended total number of partitions "
                                "for a table is under 1000..10000. Please note, that partitioning is not intended "
                                "to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). "
                                "Partitions are intended for data manipulation (DROP PARTITION, etc).", max_parts);
            }

            partition_num_to_first_row.push_back(i);
            it->getMapped() = partitions_count;

            ++partitions_count;

            /// Optimization for common case when there is only one partition - defer selector initialization.
            if (partitions_count == 2)
            {
                selector = IColumn::Selector(num_rows);
                std::fill(selector.begin(), selector.begin() + i, 0);
            }
        }

        if (partitions_count > 1)
            selector[i] = it->getMapped();
    }
    // Checking partitions per insert block again here outside the loop above
    // so we can log the total number of partitions that would have parts created
    if (max_parts && partitions_count >= max_parts && !throw_on_limit)
    {
        const auto & client_info = context->getClientInfo();
        LoggerPtr log = getLogger("MergeTreeDataWriter");

        LOG_WARNING(log, "INSERT query from initial_user {} (query ID: {}) inserted a block "
                         "that created parts in {} partitions. This is being logged "
                         "rather than throwing an exception as throw_on_max_partitions_per_insert_block=false.",
                         client_info.initial_user, client_info.initial_query_id, partitions_count);
    }
}

namespace
{

template <bool with_where, typename TTLType, typename WhereColumnType, typename ValueExtractor>
void updateTTLInfo(
    MergeTreeDataPartTTLInfo & ttl_info,
    const PaddedPODArray<TTLType> & ttl_data,
    const WhereColumnType * where_column,
    ValueExtractor && value_extractor)
{
    for (size_t i = 0; i < ttl_data.size(); ++i)
    {
        if constexpr (with_where)
        {
            /// Update ttl info only if row passes the filter.
            /// Rows that don't pass the filter should not affect TTL.
            if (where_column->getBool(i))
            {
                auto value = value_extractor(ttl_data[i]);
                ttl_info.update(value);
            }
        }
        else
        {
            auto value = value_extractor(ttl_data[i]);
            ttl_info.update(value);
        }
    }
}

template <bool with_where, typename WhereColumnType>
void updateTTLInfo(MergeTreeDataPartTTLInfo & ttl_info, const IColumn & ttl_column, const WhereColumnType * where_column)
{
    const auto & date_lut = DateLUT::serverTimezoneInstance();

    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(&ttl_column))
    {
        updateTTLInfo<with_where>(ttl_info, column_date->getData(), where_column, [&date_lut](UInt16 val)
        {
            return date_lut.fromDayNum(DayNum(val));
        });
    }
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(&ttl_column))
    {
        updateTTLInfo<with_where>(ttl_info, column_date_time->getData(), where_column, [](UInt32 val)
        {
            return val;
        });
    }
    else if (const ColumnInt32 * column_date_32 = typeid_cast<const ColumnInt32 *>(&ttl_column))
    {
        updateTTLInfo<with_where>(ttl_info, column_date_32->getData(), where_column, [&date_lut](Int32 val)
        {
            return date_lut.fromDayNum(ExtendedDayNum(val));
        });
    }
    else if (const ColumnDateTime64 * column_date_time_64 = typeid_cast<const ColumnDateTime64 *>(&ttl_column))
    {
        updateTTLInfo<with_where>(ttl_info, column_date_time_64->getData(), where_column, [scale = column_date_time_64->getScale()](DateTime64 val)
        {
            return val / intExp10OfSize<Int64>(scale);
        });
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type ({}) of result TTL column", ttl_column.getName());
    }
}

void updateTTLInfo(MergeTreeDataPartTTLInfo & ttl_info, const IColumn & ttl_column, const IColumn * where_column)
{
    if (where_column)
    {
        /// Add specialization for UInt8 because it's the most common type for filter
        if (const auto * where_column_uint8 = typeid_cast<const ColumnUInt8 *>(where_column))
            updateTTLInfo<true>(ttl_info, ttl_column, where_column_uint8);
        else
            updateTTLInfo<true>(ttl_info, ttl_column, where_column);
    }
    else
    {
        updateTTLInfo<false>(ttl_info, ttl_column, where_column);
    }
}

template <typename ColumnType>
bool hasRowsInFilter(const ColumnType & where_column)
{
    for (size_t i = 0; i < where_column.size(); ++i)
    {
        if (where_column.getBool(i))
            return true;
    }
    return false;
}

bool hasRowsInFilter(const IColumn & where_column)
{
    /// Add specialization for UInt8 because it's the most common type for filter
    if (const auto * where_column_uint8 = typeid_cast<const ColumnUInt8 *>(&where_column))
        return hasRowsInFilter(*where_column_uint8);
    else
        return hasRowsInFilter(where_column);
}

void updateTTLInfoConst(MergeTreeDataPartTTLInfo & ttl_info, const ColumnConst & ttl_column, const IColumn * where_column)
{
    if (where_column && !hasRowsInFilter(*where_column))
        return;

    if (typeid_cast<const ColumnUInt16 *>(&ttl_column.getDataColumn()))
    {
        const auto & date_lut = DateLUT::serverTimezoneInstance();
        ttl_info.update(date_lut.fromDayNum(DayNum(ttl_column.getValue<UInt16>())));
    }
    else if (typeid_cast<const ColumnUInt32 *>(&ttl_column.getDataColumn()))
    {
        ttl_info.update(ttl_column.getValue<UInt32>());
    }
    else if (typeid_cast<const ColumnInt32 *>(&ttl_column.getDataColumn()))
    {
        const auto & date_lut = DateLUT::serverTimezoneInstance();
        ttl_info.update(date_lut.fromDayNum(ExtendedDayNum(ttl_column.getValue<Int32>())));
    }
    else if (const ColumnDateTime64 * column_date_time_64 = typeid_cast<const ColumnDateTime64 *>(&ttl_column.getDataColumn()))
    {
        ttl_info.update(ttl_column.getValue<DateTime64>() / intExp10OfSize<Int64>(column_date_time_64->getScale()));
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type ({}) of result TTL column", ttl_column.getName());
    }

}

/// Computes ttls and updates ttl infos
void updateTTL(
    const ContextPtr context,
    const TTLDescription & ttl_entry,
    IMergeTreeDataPart::TTLInfos & ttl_infos,
    MergeTreeDataPartTTLInfo & ttl_info,
    const Block & block,
    bool update_part_min_max_ttls)
{
    auto expr_and_set = ttl_entry.buildExpression(context);
    for (auto & subquery : expr_and_set.sets->getSubqueries())
        subquery->buildSetInplace(context);

    auto ttl_column = ITTLAlgorithm::executeExpressionAndGetColumn(expr_and_set.expression, block, ttl_entry.result_column);
    /// In some cases block can contain Sparse columns (for example, during direct deserialization into Sparse in input formats).
    ttl_column = ttl_column->convertToFullColumnIfSparse();
    ColumnPtr where_column;

    if (ttl_entry.where_expression_ast)
    {
        auto where_expr_and_set = ttl_entry.buildWhereExpression(context);
        for (auto & subquery : where_expr_and_set.sets->getSubqueries())
            subquery->buildSetInplace(context);

        where_column = ITTLAlgorithm::executeExpressionAndGetColumn(where_expr_and_set.expression, block, ttl_entry.where_result_column);
        if (where_column)
            where_column = where_column->convertToFullColumnIfConst();
    }

    if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(ttl_column.get()))
    {
        updateTTLInfoConst(ttl_info, *column_const, where_column.get());
    }
    else
    {
        updateTTLInfo(ttl_info, *ttl_column, where_column.get());
    }

    if (update_part_min_max_ttls)
        ttl_infos.updatePartMinMaxTTL(ttl_info);
}

void addSubcolumnsFromSortingKeyAndSkipIndicesExpression(const ExpressionActionsPtr & expr, Block & block)
{
    /// Iterate over required columns in the expression and check if block doesn't have this column.
    /// It can happen only if required column is a actually a subcolumn of some column in the block.
    /// In this case we should add this subcolumn to the block as a separate column so it can be
    /// processed as a separate input in the expression actions.
    for (const auto & required_column : expr->getRequiredColumns())
    {
        if (!block.has(required_column))
            block.insert(block.getSubcolumnByName(required_column));
    }
}

MergeTreeIndices collectSkipIndicesToMaterialize(
    const StorageMetadataPtr & metadata_snapshot,
    bool materialize_skip_indexes,
    const String & exclude_indexes_string,
    const Settings & settings,
    const MergeTreeSettings & merge_tree_settings)
{
    MergeTreeIndices indices;
    if (!materialize_skip_indexes)
        return indices;

    std::unordered_set<String> exclude_index_names;
    if (!exclude_indexes_string.empty())
        exclude_index_names = parseIdentifiersOrStringLiteralsToSet(exclude_indexes_string, settings);

    /// Indices looking into virtual columns can't be correctly built at this stage.
    auto is_virtual_column_index = [&metadata_snapshot](const IndexDescription & index)
    {
        for (const auto & required_column : index.column_names)
            if (metadata_snapshot->isVirtualColumn(required_column))
                return true;

        return false;
    };

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        if (exclude_index_names.contains(index.name))
            continue;

        if (is_virtual_column_index(index))
            continue;

        auto index_ptr = MergeTreeIndexFactory::instance().get(metadata_snapshot, index, merge_tree_settings);

        /// Inert indices (a removed index type kept only for attach compatibility) hold no data and
        /// cannot be materialized. Skip them so inserts into an attached legacy table do not throw.
        if (index_ptr->isInert())
            continue;

        indices.emplace_back(std::move(index_ptr));
    }

    return indices;
}


}

void MergeTreeTemporaryPart::cancel()
{
    for (auto & stream : streams)
    {
        stream.stream->cancel();
        stream.finalizer.cancel();
    }

    /// The part is trying to delete leftovers here
    part.reset();
}

void MergeTreeTemporaryPart::finalize()
{
    for (auto & stream : streams)
        stream.finalizer.finish();

    /// Optimize files layout in the storage for better caching
    auto file_order_hint = part->getPreferredFileOrder();
    part->getDataPartStorage().setPreferredFileOrder(file_order_hint);

    part->getDataPartStorage().precommitTransaction();
    for (const auto & [_, projection] : part->getProjectionParts())
    {
        projection->getDataPartStorage().setPreferredFileOrder(file_order_hint);
        projection->getDataPartStorage().precommitTransaction();
    }

    /// If any minmax column is a virtual, the writer aggregated placeholder values for it. Drop the
    /// in-memory index so `getMinMaxIndex()` reloads from disk and applies the 0-level correction.
    const auto metadata_snapshot = part->getMetadataSnapshot();
    const auto data_settings = part->storage.getSettings();
    for (const auto & [minmax_column, _] : MergeTreeData::getMinMaxColumns(metadata_snapshot->getPartitionKey(), data_settings))
        if (metadata_snapshot->isVirtualColumn(minmax_column))
            part->setMinMaxIndex(nullptr);
}

/// This method must be called after rename and commit of part
/// because a correct path is required for the keys of caches.
void MergeTreeTemporaryPart::prewarmCaches()
{
    auto prewarm_caches = part->storage.getCachesToPrewarm(part->getBytesUncompressedOnDisk());

    if (prewarm_caches.mark_cache)
    {
        for (const auto & stream : streams)
        {
            auto marks = stream.stream->releaseCachedMarks();
            addMarksToCache(*part, marks, prewarm_caches.mark_cache.get());
        }
    }

    if (prewarm_caches.index_mark_cache)
    {
        for (const auto & stream : streams)
        {
            auto index_marks = stream.stream->releaseCachedIndexMarks();
            addMarksToCache(*part, index_marks, prewarm_caches.index_mark_cache.get());
        }
    }

    if (prewarm_caches.primary_index_cache)
    {
        /// Index was already set during writing. Now move it to cache.
        part->moveIndexToCache(*prewarm_caches.primary_index_cache);
    }
}

BlocksWithPartition MergeTreeDataWriter::splitBlockIntoParts(
    Block && block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, IColumn::Selector * out_selector)
{
    /// out_selector is left empty when the block is not split (a single resulting partition);
    /// the caller then knows every row belongs to the only partition.
    if (out_selector)
        out_selector->clear();

    BlocksWithPartition result;
    if (block.empty() || !block.rows())
    {
        return result;
    }

    metadata_snapshot->check(block, true);

    if (!metadata_snapshot->hasPartitionKey()) /// Table is not partitioned.
    {
        result.emplace_back(std::make_shared<Block>(std::move(block)), Row{});
        result[0].partition_id = result[0].partition.getID(metadata_snapshot->getPartitionKey().sample_block);
        return result;
    }

    Block block_copy = block;
    /// After expression execution partition key columns will be added to block_copy with names regarding partition function.
    auto partition_key_names_and_types = MergeTreePartition::executePartitionByExpression(metadata_snapshot, block_copy, context);

    Columns partition_columns;
    ColumnRawPtrs partition_columns_raw_ptrs;
    partition_columns.reserve(partition_key_names_and_types.size());
    partition_columns_raw_ptrs.reserve(partition_key_names_and_types.size());
    bool all_partition_columns_are_equal = true;
    for (const auto & element : partition_key_names_and_types)
    {
        partition_columns.emplace_back(block_copy.getColumnOrSubcolumnByName(element.name).column);
        partition_columns_raw_ptrs.emplace_back(partition_columns.back().get());
        if (!partition_columns.back()->hasEqualValues())
            all_partition_columns_are_equal = false;
    }
    auto get_partition = [&](size_t row_num)
    {
        Row partition(partition_columns.size());
        for (size_t i = 0; i < partition_columns.size(); ++i)
            partition[i] = (*partition_columns[i])[row_num];
        return partition;
    };

    if (all_partition_columns_are_equal)
    {
        /// A typical case is when there is one partition (you do not need to split anything).
        /// NOTE: returning a copy of the original block so that calculated partition key columns
        /// do not interfere with possible calculated primary key columns of the same name.
        result.emplace_back(std::make_shared<Block>(std::move(block)), get_partition(0));
        result[0].partition_id = result[0].partition.getID(metadata_snapshot->getPartitionKey().sample_block);
        return result;
    }

    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    buildScatterSelector(partition_columns_raw_ptrs, partition_num_to_first_row, selector, max_parts, context);

    size_t partitions_count = partition_num_to_first_row.size();
    result.reserve(partitions_count);

    for (size_t i = 0; i < partitions_count; ++i)
        result.emplace_back(std::make_shared<Block>(block.cloneEmpty()), get_partition(partition_num_to_first_row[i]));

    for (size_t col = 0; col < block.columns(); ++col)
    {
        auto scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
        for (size_t i = 0; i < partitions_count; ++i)
            result[i].block->getByPosition(col).column = std::move(scattered[i]);
    }

    for (auto & item : result)
        item.partition_id = item.partition.getID(metadata_snapshot->getPartitionKey().sample_block);

    /// Hand the row -> partition-index mapping to the caller (deduplication uses it).
    if (out_selector)
        *out_selector = std::move(selector);

    return result;
}

Block MergeTreeDataWriter::mergeBlock(
    Block && block,
    const StorageMetadataPtr & metadata_snapshot,
    SortDescription sort_description,
    IColumn::Permutation *& permutation,
    const MergeTreeData::MergingParams & merging_params)
{
    SharedHeader header = std::make_shared<const Block>(std::move(block));
    OpenTelemetry::SpanHolder span("MergeTreeDataWriter::mergeBlock");

    size_t block_size = header->rows();
    span.addAttribute("clickhouse.rows", block_size);
    span.addAttribute("clickhouse.columns", header->columns());

    auto get_merging_algorithm = [&]() -> std::shared_ptr<IMergingAlgorithm>
    {
        switch (merging_params.mode)
        {
            /// There is nothing to merge in single block in ordinary MergeTree
            case MergeTreeData::MergingParams::Ordinary:
                return nullptr;
            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedAlgorithm>(
                    header, 1, sort_description, merging_params.is_deleted_column, merging_params.version_column, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt);
            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedAlgorithm>(
                    header, 1, sort_description, merging_params.sign_column,
                    false, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt, getLogger("MergeTreeDataWriter"), /*out_row_sources_buf_=*/ nullptr,
                    /*use_average_block_sizes=*/ false, /*throw_if_invalid_sign=*/ true);
            case MergeTreeData::MergingParams::Summing: {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<SummingSortedAlgorithm>(
                    header,
                    1,
                    sort_description,
                    merging_params.columns_to_sum,
                    required_columns,
                    block_size + 1,
                    /*block_size_bytes=*/0,
                    /*max_dynamic_subcolumns=*/std::nullopt,
                    "sumWithOverflow",
                    "sumMapWithOverflow",
                    true,
                    false,
                    merging_params.allow_tuple_element_aggregation);
            }
            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedAlgorithm>(
                    header,
                    1,
                    sort_description,
                    block_size + 1,
                    /*block_size_bytes=*/0,
                    /*max_dynamic_subcolumns=*/std::nullopt,
                    merging_params.allow_tuple_element_aggregation);
            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingAlgorithm>(
                    header, 1, sort_description, merging_params.sign_column, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt);
            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedAlgorithm>(
                    header, 1, sort_description, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt, merging_params.graphite_params, time(nullptr));
            case MergeTreeData::MergingParams::Coalescing:
            {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<SummingSortedAlgorithm>(
                    header, 1, sort_description, merging_params.columns_to_sum,
                    required_columns, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt, "last_value", "last_value", false, true, merging_params.allow_tuple_element_aggregation);
            }
        }
    };

    auto merging_algorithm = get_merging_algorithm();
    if (!merging_algorithm)
        return *header;

    span.addAttribute("clickhouse.merging_algorithm", merging_algorithm->getName());

    Chunk chunk(header->getColumns(), block_size);

    IMergingAlgorithm::Input input;
    input.set(std::move(chunk));
    input.permutation = permutation;

    IMergingAlgorithm::Inputs inputs;
    inputs.push_back(std::move(input));
    merging_algorithm->initialize(std::move(inputs));

    IMergingAlgorithm::Status status = merging_algorithm->merge();

    /// Check that after first merge merging_algorithm is waiting for data from input 0.
    if (status.required_source != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Required source after the first merge is not 0. Chunk rows: {}, is_finished: {}, required_source: {}, algorithm: {}", status.chunk.getNumRows(), status.is_finished, status.required_source, merging_algorithm->getName());

    status = merging_algorithm->merge();

    /// Check that merge is finished.
    if (!status.is_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge is not finished after the second merge.");

    /// Merged Block is sorted and we don't need to use permutation anymore
    permutation = nullptr;

    return header->cloneWithColumns(status.chunk.getColumns());
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPart(
    BlockWithPartition & block,
    StorageMetadataPtr metadata_snapshot,
    ContextPtr context,
    bool may_have_leftover)
{
    auto partition_id = block.partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    return writeTempPartImpl(block, std::move(metadata_snapshot), std::move(partition_id), /*patch_part_index=*/ {}, std::move(context), data.insert_increment.get(), may_have_leftover);
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPatchPart(
    BlockWithPartition & block,
    StorageMetadataPtr metadata_snapshot,
    String partition_id,
    PatchPartIndex patch_part_index,
    ContextPtr context,
    bool may_have_leftover)
{
    return writeTempPartImpl(block, std::move(metadata_snapshot), std::move(partition_id), std::move(patch_part_index), std::move(context), data.insert_increment.get(), may_have_leftover);
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPartImpl(
    BlockWithPartition & block_with_partition,
    StorageMetadataPtr metadata_snapshot,
    String partition_id,
    std::optional<PatchPartIndex> patch_part_index,
    ContextPtr context,
    UInt64 block_number,
    bool may_have_leftover)
{
    auto temp_part = std::make_unique<MergeTreeTemporaryPart>();
    Block & block = *block_with_partition.block;
    MergeTreePartition & partition = block_with_partition.partition;

    const auto & data_settings = data.getSettings();
    const auto & global_settings = context->getSettingsRef();

    auto columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    /// Do not write _block_number and _block_offset for 0-level parts: block number is not known on this step.
    const auto minmax_columns = MergeTreeData::getMinMaxColumns(metadata_snapshot->getPartitionKey(), data_settings, MergeTreePartMinMaxIndexColumns::PARTITION_KEY_ONLY);
    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, minmax_columns);

    const bool optimize_on_insert = !isPatchPartitionId(partition_id)
        && global_settings[Setting::optimize_on_insert]
        && data.merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    UInt32 new_part_level = optimize_on_insert ? 1 : 0;
    MergeTreePartInfo new_part_info(std::move(partition_id), block_number, block_number, new_part_level);

    if (patch_part_index && !patch_part_index->empty())
        new_part_info.mutation = patch_part_index->getMaxDataVersion();

    String part_name;
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date(static_cast<DayNum::UnderlyingType>(minmax_idx->hyperrectangle[data.minmax_idx_date_column_pos].left.safeGet<UInt64>()));
        DayNum max_date(static_cast<DayNum::UnderlyingType>(minmax_idx->hyperrectangle[data.minmax_idx_date_column_pos].right.safeGet<UInt64>()));

        const auto & date_lut = DateLUT::serverTimezoneInstance();

        auto min_month = date_lut.toNumYYYYMM(min_date);
        auto max_month = date_lut.toNumYYYYMM(max_date);

        if (min_month != max_month)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part spans more than one month.");

        part_name = new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        part_name = new_part_info.getPartNameV1();

    std::string temp_prefix = "tmp_insert_";
    const auto & temp_postfix = data.getPostfixForTempInsertName();
    if (!temp_postfix.empty())
        temp_prefix += temp_postfix + "_";

    std::string part_dir = temp_prefix + part_name;

    auto indices = collectSkipIndicesToMaterialize(
        metadata_snapshot,
        global_settings[Setting::materialize_skip_indexes_on_insert],
        global_settings[Setting::exclude_materialize_skip_indexes_on_insert].toString(),
        global_settings,
        *data_settings);

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
    {
        auto expr = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices);
        addSubcolumnsFromSortingKeyAndSkipIndicesExpression(expr, block);
        expr->execute(block);
    }

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    std::vector<bool> reverse_flags = metadata_snapshot->getSortingKeyReverseFlags();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        if (!reverse_flags.empty() && reverse_flags[i])
            sort_description.emplace_back(sort_columns[i], -1, 1);
        else
            sort_description.emplace_back(sort_columns[i], 1, 1);
    }

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocks);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterSortingBlocksMicroseconds);

        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted);
    }

    if ((*data_settings)[MergeTreeSetting::optimize_row_order]
        && data.merging_params.mode
            == MergeTreeData::MergingParams::Mode::Ordinary) /// Nobody knows if this optimization messes up specialized MergeTree engines.
    {
        RowOrderOptimizer::optimize(block, sort_description, perm);
        perm_ptr = &perm;
    }

    if (optimize_on_insert)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterMergingBlocksMicroseconds);
        block = mergeBlock(std::move(block), metadata_snapshot, sort_description, perm_ptr, data.merging_params);
    }

    ColumnsStatistics statistics;
    if (context->getSettingsRef()[Setting::materialize_statistics_on_insert])
    {
        const UInt64 max_table_size = context->getSettingsRef()[Setting::materialize_statistics_on_insert_max_table_size];
        /// Skip building statistics on INSERT for large tables (e.g. fact tables): they materialize
        /// statistics during merges instead, avoiding per-insert overhead.
        /// Setting value = 0 disables the limit.
        if (max_table_size == 0 || data.getTotalActiveSizeInBytes() + block.bytes() <= max_table_size)
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterStatisticsCalculationMicroseconds);
            const auto & all_columns = metadata_snapshot->getColumns();
            statistics = ColumnsStatistics(all_columns);
            /// A non-physical column is never present in a written block, so `build` below would
            /// reject it. Every other absence stays an error.
            std::erase_if(statistics, [&](const auto & entry) { return !all_columns.hasPhysical(entry.first); });
            statistics.build(block);
        }
    }

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    /// If optimize_on_insert is true, block may become empty after merge. There
    /// is no need to create empty part. Since expected_size could be zero when
    /// part only contains empty tuples. As a result, check rows instead.
    if (block.rows() == 0)
        return temp_part;

    DB::IMergeTreeDataPart::TTLInfos move_ttl_infos;
    const auto & move_ttl_entries = metadata_snapshot->getMoveTTLs();
    for (const auto & ttl_entry : move_ttl_entries)
        updateTTL(context, ttl_entry, move_ttl_infos, move_ttl_infos.moves_ttl[ttl_entry.result_column], block, false);

    const UInt64 & min_bytes_to_perform_insert =
            (*data_settings)[MergeTreeSetting::min_free_disk_bytes_to_perform_insert].changed
            ? (*data_settings)[MergeTreeSetting::min_free_disk_bytes_to_perform_insert]
            : global_settings[Setting::min_free_disk_bytes_to_perform_insert];

    const Float32 & min_ratio_to_perform_insert =
        (*data_settings)[MergeTreeSetting::min_free_disk_ratio_to_perform_insert].changed
        ? (*data_settings)[MergeTreeSetting::min_free_disk_ratio_to_perform_insert]
        : global_settings[Setting::min_free_disk_ratio_to_perform_insert];

    const bool is_system_database = (data.getStorageID().getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);

    VolumePtr volume = data.getStoragePolicy()->getVolume(0);
    ReservationPtr reservation;

    if (!is_system_database && (min_bytes_to_perform_insert > 0 || min_ratio_to_perform_insert > 0.0f))
    {
        ReservationConstraints constraints(min_bytes_to_perform_insert, min_ratio_to_perform_insert);

        /// Try to reserve space on volume with constraints
        reservation = volume->reserve(expected_size, constraints);

        if (!reservation)
        {
            throw Exception(
                ErrorCodes::NOT_ENOUGH_SPACE,
                "Could not perform insert. "
                "None of the disks in volume '{}' have enough free space to meet the configured threshold. "
                "The threshold can be configured either in MergeTree settings or User settings, "
                "using the following settings: "
                "(1) `min_free_disk_bytes_to_perform_insert` = {} "
                "(2) `min_free_disk_ratio_to_perform_insert` = {}",
                volume->getName(),
                min_bytes_to_perform_insert,
                min_ratio_to_perform_insert);
        }
    }
    else
    {
        /// No free space check needed, use default reservation
        reservation = data.reserveSpacePreferringTTLRules(metadata_snapshot, expected_size, move_ttl_infos, time(nullptr), 0, true);
    }

    /// The `SingleDiskVolume` is stored on the part and lives for its whole lifetime, so build it in
    /// the dedicated arena (`build()` below self-scopes; the tail metadata is re-homed further down).
    VolumePtr data_part_volume;
    {
        ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
        data_part_volume = createVolumeFromReservation(reservation, volume);
    }

    /// The part directory name can be non-unique because of leftovers of previous runs.
    temp_part->temporary_directory_lock = data.claimTemporaryPartDirectory(data_part_volume->getDisk(), part_dir, may_have_leftover);

    auto part_format = data.choosePartFormat(expected_size, block.rows(), new_part_level, /*projection =*/nullptr);
    /// UNIQUE KEY parts must use Full part storage: the dense-index sidecar
    /// (`unique_key_index.sst`) is opened directly by filesystem path via RocksDB
    /// `SstFileReader`, which cannot read a file packed inside an archive. Packed
    /// storage would leave the sidecar existsFile-visible but unopenable, failing
    /// every subsequent load of the part.
    if (metadata_snapshot->hasUniqueKey())
        part_format.storage_type = MergeTreeDataPartStorageType::Full;

    auto new_data_part = data.getDataPartBuilder(part_name, data_part_volume, part_dir, getReadSettings(), PartDirIntent::CreateFresh)
        .withPartFormat(part_format)
        .withPartInfo(new_part_info)
        .build();

    auto data_part_storage = new_data_part->getDataPartStoragePtr();
    data_part_storage->beginTransaction();

    if ((*data.storage_settings.get())[MergeTreeSetting::assign_part_uuids])
        new_data_part->uuid = UUIDHelpers::generateV4();

    SerializationInfo::Settings settings
    {
        static_cast<double>((*data_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization]),
        true,
        (*data_settings)[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
        (*data_settings)[MergeTreeSetting::serialization_info_version],
        (*data_settings)[MergeTreeSetting::string_serialization_version],
        (*data_settings)[MergeTreeSetting::nullable_serialization_version],
        (*data_settings)[MergeTreeSetting::map_serialization_version_for_zero_level_parts],
        (*data_settings)[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types],
    };
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    for (const auto & [column_name, _] : columns)
    {
        auto & column = block.getByName(column_name);
        if (!ISerialization::hasKind(infos.getKindStack(column_name), ISerialization::Kind::SPARSE))
            column.column = recursiveRemoveSparse(column.column);
    }

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());

    if (patch_part_index)
        new_data_part->setPatchPartIndex(std::move(*patch_part_index));

    new_data_part->rows_count = block.rows();
    new_data_part->existing_rows_count = block.rows();
    new_data_part->partition = std::move(partition);
    new_data_part->setMinMaxIndex(std::move(minmax_idx));
    new_data_part->is_temp = true;
    /// In case of replicated merge tree with zero copy replication
    /// Here ClickHouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    SyncGuardPtr sync_guard;

    data_part_storage->createDirectories();

    if ((*data_settings)[MergeTreeSetting::fsync_part_directory])
    {
        const auto disk = data_part_volume->getDisk();
        sync_guard = disk->getDirectorySyncGuard(data_part_storage->getFullPath());
    }

    if (metadata_snapshot->hasRowsTTL())
        updateTTL(context, metadata_snapshot->getRowsTTL(), new_data_part->ttl_infos, new_data_part->ttl_infos.table_ttl, block, true);

    for (const auto & ttl_entry : metadata_snapshot->getGroupByTTLs())
        updateTTL(context, ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.group_by_ttl[ttl_entry.result_column], block, true);

    for (const auto & ttl_entry : metadata_snapshot->getRowsWhereTTLs())
        updateTTL(context, ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.rows_where_ttl[ttl_entry.result_column], block, true);

    for (const auto & [name, ttl_entry] : metadata_snapshot->getColumnTTLs())
        updateTTL(context, ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.columns_ttl[name], block, true);

    const auto & recompression_ttl_entries = metadata_snapshot->getRecompressionTTLs();
    for (const auto & ttl_entry : recompression_ttl_entries)
        updateTTL(context, ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.recompression_ttl[ttl_entry.result_column], block, false);

    new_data_part->ttl_infos.update(move_ttl_infos);

    /// partition / ttl_infos / minmax (and patch source parts) are built above outside any
    /// dedicated-arena scope, so they were allocated in the default arenas. Re-home them into the
    /// dedicated arena, matching the merge / mutation / load paths.
    new_data_part->moveMetadataToDedicatedArena();

    /// Pass empty TTL infos so that `RECOMPRESS` codecs are not selected at insert time;
    /// recompression should happen during merges, not on the initial write path.
    auto compression_codec = data.getCompressionCodecForPart(metadata_snapshot, 0, {}, time(nullptr)).codec;

    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        block,
        *data_settings,
        new_data_part->index_granularity_info,
        /*blocks_are_granules=*/ false);

    IMergedBlockOutputStream::GatheredData gathered_data;
    gathered_data.statistics = std::move(statistics);

    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        data_settings,
        metadata_snapshot,
        columns,
        indices,
        compression_codec,
        std::move(index_granularity_ptr),
        (data.supportsTransactions() && context->getCurrentTransaction()) ? context->getCurrentTransaction()->tid : Tx::NonTransactionalTID,
        block.bytes(),
        /*reset_columns=*/false,
        /*blocks_are_granules_size=*/false,
        context->getWriteSettings(),
        static_cast<WrittenOffsetSubstreams *>(nullptr),
        /*try_adaptive_codec=*/ false);

    Block permuted_columns_cache;
    out->writeWithPermutation(block, perm_ptr, &permuted_columns_cache);

    if (metadata_snapshot->hasUniqueKey())
        UniqueKeyDenseIndexOps::writeDenseIndexOnInsert(
            *data_part_storage,
            metadata_snapshot,
            block,
            perm_ptr,
            context->getSettingsRef()[Setting::unique_key_max_encoded_size],
            context);

    if ((*data.getSettings())[MergeTreeSetting::materialize_projections_on_insert])
    {
        for (const auto & projection : metadata_snapshot->getProjections())
        {
            /// Commit-order projections use `_block_number` which is only finalized at commit time.
            /// Skip during insert; they will be built correctly during the first merge.
            if (projection.with_block_number)
                continue;

            Block projection_block;
            {
                ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterProjectionsCalculationMicroseconds);
                projection_block = projection.calculate(block, 0, context, perm_ptr);
                LOG_DEBUG(
                    log, "Spent {} ms calculating projection {} for the part {}", watch.elapsed() / 1000, projection.name, new_data_part->name);
            }

            if (projection_block.rows())
            {
                auto proj_temp_part
                    = writeProjectionPart(data, projection_block, projection, new_data_part.get(), /*merge_is_needed=*/false, context);
                new_data_part->addProjectionPart(projection.name, std::move(proj_temp_part->part));

                if (global_settings[Setting::finalize_projection_parts_synchronously])
                {
                    /// Finish each projection stream's finalizer immediately to release
                    /// output stream memory (writer buffers, S3 write buffers, etc.).
                    /// We cannot call proj_temp_part->finalize() here because the part
                    /// has already been moved to new_data_part above. The projection
                    /// part's precommitTransaction() will be handled later by the main
                    /// temp_part->finalize() which iterates part->getProjectionParts().
                    for (auto & stream : proj_temp_part->streams)
                        stream.finalizer.finish();
                }
                else
                {
                    for (auto & stream : proj_temp_part->streams)
                        temp_part->streams.emplace_back(std::move(stream));
                }
            }
        }
    }

    out->finalizeIndexGranularity();
    auto finalizer = out->finalizePartAsync(
        new_data_part,
        gathered_data,
        (*data_settings)[MergeTreeSetting::fsync_after_insert]);

    temp_part->part = new_data_part;
    temp_part->streams.emplace_back(MergeTreeTemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return temp_part;
}

namespace MutationHelpers
{
/// Defined in MutateTask.cpp; reused here so a projection materialized from a lightweight-update
/// patch keeps the same provenance protection APPLY PATCHES already gives the main part.
DataTypePtr mergeJSONSharedDataPathRulesFromPatchParts(
    DataTypePtr type, const String & column_name, const PatchPartsForReader & patch_parts, bool & inputs_saturated);
}

/// projection.metadata's columns are keyed by the projection's analyzed output name, which for a
/// non-trivial SELECT-list expression (e.g. `assumeNotNull(j)`, `materialize(j)`) is the rendered
/// expression text, not any single source column's own name -- see ProjectionsDescription.cpp's
/// choice of QueryProcessingStage: without a WHERE clause or aggregation, the header (and thus a
/// materialized part with no prior projection sub-part to read from) is instead built straight
/// from the source columns verbatim, so the *same* SELECT item can end up named either way
/// depending on the projection. Collect every identifier referenced anywhere in each SELECT item
/// (covers both outcomes, plus multi-column expressions like `if(cond, j, k)`) so the fallback
/// below can still find the right physical column either way.
/// Like IAST::collectIdentifierNames, but skips if()/multiIf() conditions and masks lambda formal
/// parameters (matching RequiredSourceColumnsVisitor), so a bound name can't shadow a real column.
static void collectValueCarryingIdentifierNames(const IAST & node, IdentifierNameSet & names, std::unordered_set<String> & masked_names);

/// Convenience overload for callers that don't need masking carried in from an outer scope.
static IdentifierNameSet collectValueCarryingIdentifierNames(const IAST & node)
{
    IdentifierNameSet names;
    std::unordered_set<String> masked_names;
    collectValueCarryingIdentifierNames(node, names, masked_names);
    return names;
}

/// A lambda formal is used by the body when it appears bare (`x`) or through a member (`x.doc`):
/// the member access still reads the formal, so the matching higher-order argument donates provenance.
static bool lambdaParamIsUsedInBody(const String & param_name, const IdentifierNameSet & body_names)
{
    if (body_names.contains(param_name))
        return true;
    const String prefix = param_name + ".";
    auto it = body_names.lower_bound(prefix);
    return it != body_names.end() && it->starts_with(prefix);
}

/// If args[0] is `lambda((p1, ..., pn) -> body)` with n == args.size() - 1 params (the shape shared
/// by arrayFold and the array-mapping higher-order functions), returns the params tuple and body.
static std::pair<const ASTFunction *, const IAST *> extractLambdaParamsAndBody(const ASTs & args)
{
    const auto * lambda_arg = args[0]->as<ASTFunction>();
    if (!lambda_arg || lambda_arg->name != "lambda" || !lambda_arg->arguments || lambda_arg->arguments->children.size() != 2)
        return {nullptr, nullptr};
    const auto * params_tuple = lambda_arg->arguments->children[0]->as<ASTFunction>();
    if (!params_tuple || params_tuple->name != "tuple" || !params_tuple->arguments
        || params_tuple->arguments->children.size() != args.size() - 1)
        return {nullptr, nullptr};
    return {params_tuple, lambda_arg->arguments->children[1].get()};
}

/// Member step for a constant subscript: `arr[i]` and `m[k]` are the same function, and only the
/// source type tells them apart, so name the step here and resolve it during the type descent.
static constexpr std::string_view SUBSCRIPT_MEMBER = "[]";

static const IAST * unwrapTransparentProjectionExpression(const IAST & node);

/// "t.doc" / "t.1" for tupleElement(t, 'doc' | 1) chains, "m.keys"/"m.values" for mapKeys/mapValues(m),
/// "x.[]" for a constant subscript, over a plain column base; nullopt when the base is not a plain
/// (possibly nested) column reference. Transparent wrappers are peeled at every step, so
/// `tupleElement(assumeNotNull(t), 'doc')` still names `t.doc` instead of falling back to `t`.
static std::optional<String> tryGetMemberQualifiedName(const IAST & wrapped_node)
{
    const IAST & node = *unwrapTransparentProjectionExpression(wrapped_node);
    if (const auto * identifier = node.as<ASTIdentifier>())
        return identifier->name();

    const auto * function = node.as<ASTFunction>();
    if (!function || !function->arguments)
        return std::nullopt;
    const auto & args = function->arguments->children;

    if (function->name == "tupleElement" && args.size() == 2)
    {
        const auto * literal = args[1]->as<ASTLiteral>();
        if (!literal)
            return std::nullopt;
        String member;
        if (literal->value.getType() == Field::Types::String)
            member = literal->value.safeGet<String>();
        else if (literal->value.getType() == Field::Types::UInt64)
            member = toString(literal->value.safeGet<UInt64>());
        else
            return std::nullopt;
        if (auto base = tryGetMemberQualifiedName(*args[0]))
            return *base + "." + member;
        return std::nullopt;
    }

    if ((function->name == "mapKeys" || function->name == "mapValues") && args.size() == 1)
    {
        if (auto base = tryGetMemberQualifiedName(*args[0]))
            return *base + "." + (function->name == "mapKeys" ? "keys" : "values");
        return std::nullopt;
    }

    if (function->name == "arrayElement" && args.size() == 2 && args[1]->as<ASTLiteral>())
    {
        if (auto base = tryGetMemberQualifiedName(*args[0]))
            return *base + "." + String(SUBSCRIPT_MEMBER);
        return std::nullopt;
    }

    return std::nullopt;
}

/// `x -> tupleElement(x, 'doc')` bound to `arr` reads one member of the array's elements, so the
/// donor is `arr.doc` (the member descent looks through the Array) rather than the whole `arr`.
/// False when the body also reads the formal bare, or the bound source has no qualified name:
/// then the caller's whole-source fallback is the only safe answer.
static bool tryAddMemberQualifiedLambdaSource(
    const String & param_name,
    const IdentifierNameSet & body_names,
    const IAST & source,
    IdentifierNameSet & names,
    const std::unordered_set<String> & masked_names)
{
    if (body_names.contains(param_name))
        return false;
    auto source_name = tryGetMemberQualifiedName(source);
    if (!source_name)
        return false;
    if (masked_names.contains(source_name->substr(0, source_name->find('.'))))
        return false;

    const String prefix = param_name + ".";
    bool added = false;
    for (auto it = body_names.lower_bound(prefix); it != body_names.end() && it->starts_with(prefix); ++it)
    {
        names.insert(*source_name + it->substr(param_name.size()));
        added = true;
    }
    return added;
}

static void collectValueCarryingIdentifierNames(const IAST & node, IdentifierNameSet & names, std::unordered_set<String> & masked_names)
{
    if (const auto * identifier = node.as<ASTIdentifier>())
    {
        if (!masked_names.contains(identifier->name()))
            names.insert(identifier->name());
        return;
    }

    if (const auto * function = node.as<ASTFunction>(); function && function->arguments)
    {
        const auto & args = function->arguments->children;
        if (function->name == "lambda" && args.size() == 2)
        {
            const auto * lambda_args_tuple = args[0]->as<ASTFunction>();
            if (lambda_args_tuple && lambda_args_tuple->name == "tuple" && lambda_args_tuple->arguments)
            {
                std::vector<String> newly_masked;
                for (const auto & param : lambda_args_tuple->arguments->children)
                    if (const auto * param_identifier = param->as<ASTIdentifier>(); param_identifier && masked_names.insert(param_identifier->name()).second)
                        newly_masked.push_back(param_identifier->name());

                collectValueCarryingIdentifierNames(*args[1], names, masked_names);

                for (const auto & name : newly_masked)
                    masked_names.erase(name);
                return;
            }
        }
        /// Member access reads one side of its source; qualify the candidate with that member so the
        /// resolver descends into it instead of donating the source's sibling policies.
        if (function->name == "tupleElement" || function->name == "mapKeys" || function->name == "mapValues"
            || function->name == "arrayElement")
        {
            if (auto qualified = tryGetMemberQualifiedName(node))
            {
                const String root = qualified->substr(0, qualified->find('.'));
                if (!masked_names.contains(root) && !masked_names.contains(*qualified))
                    names.insert(*qualified);
                return;
            }
        }
        if (Poco::toLower(function->name) == "if" && args.size() == 3)
        {
            collectValueCarryingIdentifierNames(*args[1], names, masked_names);
            collectValueCarryingIdentifierNames(*args[2], names, masked_names);
            return;
        }
        if (function->name == "multiIf" && args.size() >= 3)
        {
            for (size_t i = 0; i < args.size(); ++i)
                if (!(i % 2 == 0 && i != args.size() - 1))
                    collectValueCarryingIdentifierNames(*args[i], names, masked_names);
            return;
        }
        /// These (and mapFilter/mapSort/... Map adapters over the same array Impls) only select/order/
        /// count/split the *first* collection's own elements, so the lambda and the trailing
        /// condition/sort-key collections are not donors either (like if()'s condition).
        static const std::unordered_set<String> predicate_only_higher_order_functions = {
            "arrayFilter", "arrayExists", "arrayAll", "arrayCount",
            "arrayFirst", "arrayFirstOrNull", "arrayLast", "arrayLastOrNull",
            "arrayFirstIndex", "arrayLastIndex",
            "arrayFill", "arrayReverseFill",
            "arraySort", "arrayReverseSort", "arrayPartialSort", "arrayPartialReverseSort",
            "arraySplit", "arrayReverseSplit",
            "arrayCompact",
            "arrayTopK", "arrayBottomK",
            "mapFilter", "mapExists", "mapAll",
            "mapSort", "mapReverseSort", "mapPartialSort", "mapPartialReverseSort",
        };
        /// arrayPartialSort([f,] limit, arr, ...) / arrayTopK([f,] K, arr, ...) put a scalar count
        /// between the lambda and the collection whose elements they return.
        static const std::unordered_set<String> predicate_only_with_leading_count = {
            "arrayPartialSort", "arrayPartialReverseSort",
            "arrayTopK", "arrayBottomK",
            "mapPartialSort", "mapPartialReverseSort",
        };
        if (!args.empty() && predicate_only_higher_order_functions.contains(function->name))
        {
            const auto * lambda_arg = args[0]->as<ASTFunction>();
            if (lambda_arg && lambda_arg->name == "lambda")
            {
                const size_t source_index = predicate_only_with_leading_count.contains(function->name) ? 2 : 1;
                if (source_index < args.size())
                    collectValueCarryingIdentifierNames(*args[source_index], names, masked_names);
                return;
            }
        }
        if (function->name == "arrayFold" && args.size() >= 3)
        {
            /// arrayFold's params[0] is the accumulator, with no array of its own; seed can itself
            /// become the result (e.g. an empty array), so unlike the arrays it's always a donor.
            auto [lambda_params_tuple, lambda_body] = extractLambdaParamsAndBody(args);
            if (lambda_params_tuple)
            {
                IdentifierNameSet body_names = collectValueCarryingIdentifierNames(*lambda_body);

                const auto & params = lambda_params_tuple->arguments->children;
                for (size_t i = 1; i < params.size(); ++i)
                {
                    const auto * param_identifier = params[i]->as<ASTIdentifier>();
                    if (!param_identifier || !lambdaParamIsUsedInBody(param_identifier->name(), body_names))
                        continue;
                    /// Same member-qualified substitution the generic branch below makes: a body that only
                    /// reads `x.doc` donates `arr.doc`, not the whole `arr`, which would then be refused as
                    /// a tuple source and lose the element's own rules.
                    if (!tryAddMemberQualifiedLambdaSource(param_identifier->name(), body_names, *args[i], names, masked_names))
                        collectValueCarryingIdentifierNames(*args[i], names, masked_names);
                }
                collectValueCarryingIdentifierNames(*args.back(), names, masked_names);
                return;
            }
        }
        /// arrayMap((x, y) -> y, arr, meta_arr): only arrays whose bound parameter is actually
        /// referenced in the body feed the output, not every array argument unconditionally.
        if (!args.empty() && !predicate_only_higher_order_functions.contains(function->name))
        {
            auto [lambda_params_tuple, lambda_body] = extractLambdaParamsAndBody(args);
            if (lambda_params_tuple)
            {
                IdentifierNameSet body_names = collectValueCarryingIdentifierNames(*lambda_body);

                const auto & params = lambda_params_tuple->arguments->children;
                for (size_t i = 0; i != params.size(); ++i)
                {
                    const auto * param_identifier = params[i]->as<ASTIdentifier>();
                    if (!param_identifier || !lambdaParamIsUsedInBody(param_identifier->name(), body_names))
                        continue;
                    if (!tryAddMemberQualifiedLambdaSource(param_identifier->name(), body_names, *args[1 + i], names, masked_names))
                        collectValueCarryingIdentifierNames(*args[1 + i], names, masked_names);
                }
                return;
            }
        }
    }

    for (const auto & child : node.children)
        collectValueCarryingIdentifierNames(*child, names, masked_names);
}

/// tuple(j1, j2)/arrayZip(j1, j2)/map(j1, j2), and the lambda producers arrayMap(... -> tuple(...))
/// and mapApply, feed *different* structural slots from different identifiers; these let the caller
/// merge each slot's own candidates against only that slot's type.
struct ProjectionOutputProvenance
{
    std::vector<String> flat_candidates;
    std::vector<std::vector<String>> tuple_element_candidates;
    bool is_array_zip = false;
    std::vector<String> map_key_candidates;
    std::vector<String> map_value_candidates;
    bool is_map = false;
    /// `flat_candidates` are whole-output donors selected per row (the branches of a conditional),
    /// not different slots, so each one is aligned with the entire result and they merge together.
    bool aligned_alternatives = false;
};

/// materialize(x)/CAST(x, T) and the nullability wrappers don't change x's own AST structure; look
/// through them so a wrapped tuple(...)/map(...)/arrayZip(...) still gets per-slot handling below
/// (a reshaping CAST just fails the shape check at merge time and falls back safely).
static const IAST * unwrapTransparentProjectionExpression(const IAST & node)
{
    const IAST * current = &node;
    while (true)
    {
        const auto * function = current->as<ASTFunction>();
        if (!function || !function->arguments)
            break;

        const auto & args = function->arguments->children;
        if ((function->name == "materialize" || function->name == "toNullable" || function->name == "assumeNotNull")
            && args.size() == 1)
            current = args[0].get();
        else if ((function->name == "CAST" || function->name == "_CAST") && args.size() == 2)
            current = args[0].get();
        else
            break;
    }
    return current;
}

static std::unordered_map<String, ProjectionOutputProvenance> getProjectionOutputToSourceIdentifiers(const ProjectionDescription & projection)
{
    std::unordered_map<String, ProjectionOutputProvenance> output_to_sources;
    const auto * select_query = projection.query_ast->as<ASTSelectQuery>();
    if (!select_query || !select_query->select())
        return output_to_sources;

    for (const auto & child : select_query->select()->children)
    {
        ProjectionOutputProvenance provenance;

        IdentifierNameSet names = collectValueCarryingIdentifierNames(*child);
        provenance.flat_candidates.assign(names.begin(), names.end());

        if (const auto * function = unwrapTransparentProjectionExpression(*child)->as<ASTFunction>(); function && function->arguments)
        {
            const auto & args = function->arguments->children;
            if ((function->name == "tuple" || function->name == "arrayZip" || function->name == "arrayZipUnaligned")
                && args.size() >= 2)
            {
                /// arrayZipUnaligned has arrayZip's Array(Tuple(...)) contract, only with Nullable
                /// slots - which the policy merge already looks through.
                provenance.is_array_zip = (function->name != "tuple");
                for (const auto & arg : args)
                {
                    IdentifierNameSet element_names = collectValueCarryingIdentifierNames(*arg);
                    provenance.tuple_element_candidates.emplace_back(element_names.begin(), element_names.end());
                }
            }
            else if (function->name == "map" && args.size() >= 2 && args.size() % 2 == 0)
            {
                provenance.is_map = true;
                for (size_t i = 0; i < args.size(); ++i)
                {
                    IdentifierNameSet pair_names = collectValueCarryingIdentifierNames(*args[i]);
                    auto & target = (i % 2 == 0) ? provenance.map_key_candidates : provenance.map_value_candidates;
                    target.insert(target.end(), pair_names.begin(), pair_names.end());
                }
            }
            else if ((function->name == "if" && args.size() == 3)
                     || (function->name == "multiIf" && args.size() >= 3 && args.size() % 2 == 1))
            {
                /// A conditional picks one branch per row, so slot i of the result is fed by slot i of
                /// every branch. Union the branches slot-wise rather than leaving the flat candidate
                /// list to the self-only fallback, which drops every donor once the output holds more
                /// than one JSON node.
                ASTs branches;
                if (function->name == "if")
                {
                    branches.push_back(args[1]);
                    branches.push_back(args[2]);
                }
                else
                {
                    for (size_t i = 1; i + 1 < args.size(); i += 2)
                        branches.push_back(args[i]);
                    branches.push_back(args.back());
                }

                /// Slots only line up if every branch rebuilds the output the same structural way,
                /// so a mix of shapes - or any shape without slot-wise handling - falls through to
                /// the flat candidates.
                auto branch_kind = [](const ASTFunction & branch_function) -> std::string_view
                {
                    if (branch_function.name == "tuple")
                        return "tuple";
                    /// arrayZipUnaligned shares arrayZip's Array(Tuple(...)) contract, as above.
                    if (branch_function.name == "arrayZip" || branch_function.name == "arrayZipUnaligned")
                        return "arrayZip";
                    if (branch_function.name == "map")
                        return "map";
                    return {};
                };

                std::vector<const ASTFunction *> branch_functions;
                std::string_view kind;
                size_t arity = 0;
                for (const auto & branch : branches)
                {
                    const auto * branch_function = unwrapTransparentProjectionExpression(*branch)->as<ASTFunction>();
                    if (!branch_function || !branch_function->arguments)
                    {
                        branch_functions.clear();
                        break;
                    }
                    const std::string_view this_kind = branch_kind(*branch_function);
                    const size_t branch_arity = branch_function->arguments->children.size();
                    /// tuple/arrayZip are read slot by slot, so their arity has to agree across
                    /// branches; map unions its two halves, so only an even count matters.
                    const bool arity_ok = (this_kind == "map") ? (branch_arity % 2 == 0) : (!arity || branch_arity == arity);
                    if (this_kind.empty() || branch_arity < 2 || !arity_ok || (!kind.empty() && this_kind != kind))
                    {
                        branch_functions.clear();
                        break;
                    }
                    kind = this_kind;
                    arity = branch_arity;
                    branch_functions.push_back(branch_function);
                }

                if (!branch_functions.empty() && kind == "map")
                {
                    provenance.is_map = true;
                    for (const auto * branch_function : branch_functions)
                    {
                        const auto & branch_args = branch_function->arguments->children;
                        for (size_t i = 0; i != branch_args.size(); ++i)
                        {
                            IdentifierNameSet pair_names = collectValueCarryingIdentifierNames(*branch_args[i]);
                            auto & target = (i % 2 == 0) ? provenance.map_key_candidates : provenance.map_value_candidates;
                            target.insert(target.end(), pair_names.begin(), pair_names.end());
                        }
                    }
                }
                else if (!branch_functions.empty())
                {
                    provenance.is_array_zip = (kind == "arrayZip");
                    provenance.tuple_element_candidates.resize(arity);
                    for (const auto * branch_function : branch_functions)
                    {
                        for (size_t slot = 0; slot != arity; ++slot)
                        {
                            IdentifierNameSet slot_names
                                = collectValueCarryingIdentifierNames(*branch_function->arguments->children[slot]);
                            auto & target = provenance.tuple_element_candidates[slot];
                            target.insert(target.end(), slot_names.begin(), slot_names.end());
                        }
                    }
                }
                else
                {
                    /// No branch-local structure. The branches may still each be a whole-output donor
                    /// - the aligned shapes this file already preserves at top level, such as
                    /// `assumeNotNull(t)` or `arrayElement(arr, 1)`. Those are alternatives for the
                    /// same output rather than different slots, so each is aligned with all of it and
                    /// they can be merged together instead of falling into the multi-JSON drop below.
                    Names alternatives;
                    bool every_branch_is_one_donor = true;
                    for (const auto & branch : branches)
                    {
                        IdentifierNameSet branch_names = collectValueCarryingIdentifierNames(*branch);
                        if (branch_names.size() != 1)
                        {
                            every_branch_is_one_donor = false;
                            break;
                        }
                        alternatives.push_back(*branch_names.begin());
                    }

                    if (every_branch_is_one_donor)
                    {
                        /// Drops the condition's own identifiers, which donate nothing.
                        provenance.flat_candidates = std::move(alternatives);
                        provenance.aligned_alternatives = true;
                    }
                }
            }
            else if (function->name == "mapApply" && args.size() == 2)
            {
                /// mapApply((k, v) -> (key_expr, value_expr), m) rebuilds the map slot-wise. Its lambda
                /// takes two formals over a single map argument, so extractLambdaParamsAndBody's arity
                /// rule does not apply; a formal stands for one half of the source map, which the member
                /// descent can name as `m.keys` / `m.values`.
                const auto * lambda_arg = args[0]->as<ASTFunction>();
                const auto * params_tuple = lambda_arg && lambda_arg->name == "lambda" && lambda_arg->arguments
                        && lambda_arg->arguments->children.size() == 2
                    ? lambda_arg->arguments->children[0]->as<ASTFunction>()
                    : nullptr;
                /// The body may sit under a transparent wrapper: mapApply((k, v) -> materialize(tuple(k, v)), m).
                const auto * body_tuple = params_tuple
                    ? unwrapTransparentProjectionExpression(*lambda_arg->arguments->children[1])->as<ASTFunction>()
                    : nullptr;
                /// The map may be named through members too: mapApply((k, v) -> ..., tupleElement(t, 'm')).
                auto map_name = tryGetMemberQualifiedName(*args[1]);
                if (params_tuple && params_tuple->name == "tuple" && params_tuple->arguments
                    && params_tuple->arguments->children.size() == 2 && map_name
                    && body_tuple && body_tuple->name == "tuple" && body_tuple->arguments
                    && body_tuple->arguments->children.size() == 2)
                {
                    std::unordered_map<String, String> param_to_member;
                    for (size_t i = 0; i != 2; ++i)
                        if (const auto * param_identifier = params_tuple->arguments->children[i]->as<ASTIdentifier>())
                            param_to_member.emplace(
                                param_identifier->name(), *map_name + (i == 0 ? ".keys" : ".values"));

                    provenance.is_map = true;
                    for (size_t i = 0; i != 2; ++i)
                    {
                        auto & target = (i == 0) ? provenance.map_key_candidates : provenance.map_value_candidates;
                        for (const auto & name : collectValueCarryingIdentifierNames(*body_tuple->arguments->children[i]))
                        {
                            const size_t dot = name.find('.');
                            auto member_it = param_to_member.find(dot == String::npos ? name : name.substr(0, dot));
                            if (member_it == param_to_member.end())
                                target.push_back(name);
                            else
                                target.push_back(dot == String::npos ? member_it->second : member_it->second + name.substr(dot));
                        }
                    }
                }
            }
            else if (function->name == "arrayMap" && args.size() >= 2)
            {
                /// arrayMap((x, y) -> tuple(x, y), arr1, arr2) builds Array(Tuple(...)) element-wise:
                /// reconstruct per-slot candidates by substituting each lambda parameter with its
                /// bound array argument, so the whole-type merge cannot cross slots.
                auto [lambda_params_tuple, lambda_body] = extractLambdaParamsAndBody(args);
                /// The body may sit under a transparent wrapper: arrayMap((x, y) -> materialize(tuple(x, y)), ...).
                const auto * body_tuple = lambda_body ? unwrapTransparentProjectionExpression(*lambda_body)->as<ASTFunction>() : nullptr;
                if (lambda_params_tuple && body_tuple && body_tuple->name == "tuple" && body_tuple->arguments
                    && body_tuple->arguments->children.size() >= 2)
                {
                    std::unordered_map<String, const IAST *> param_to_source;
                    const auto & params = lambda_params_tuple->arguments->children;
                    for (size_t i = 0; i != params.size(); ++i)
                        if (const auto * param_identifier = params[i]->as<ASTIdentifier>())
                            param_to_source.emplace(param_identifier->name(), args[1 + i].get());

                    provenance.is_array_zip = true;
                    for (const auto & slot : body_tuple->arguments->children)
                    {
                        IdentifierNameSet slot_names = collectValueCarryingIdentifierNames(*slot);
                        std::vector<String> slot_candidates;
                        for (const auto & name : slot_names)
                        {
                            const size_t dot = name.find('.');
                            auto source_it = param_to_source.find(dot == String::npos ? name : name.substr(0, dot));
                            if (source_it == param_to_source.end())
                            {
                                slot_candidates.push_back(name);
                                continue;
                            }
                            if (dot == String::npos)
                            {
                                IdentifierNameSet source_names = collectValueCarryingIdentifierNames(*source_it->second);
                                slot_candidates.insert(slot_candidates.end(), source_names.begin(), source_names.end());
                            }
                            else if (auto source_name = tryGetMemberQualifiedName(*source_it->second))
                            {
                                /// `x.doc` with x bound to `arr` or to a member-qualified source such
                                /// as `tupleElement(t, 'arr')`: name it `arr.doc` / `t.arr.doc` and let
                                /// the member descent in resolveMemberQualifiedPolicySource do the rest.
                                slot_candidates.push_back(*source_name + name.substr(dot));
                            }
                        }
                        provenance.tuple_element_candidates.push_back(std::move(slot_candidates));
                    }
                }
            }
        }

        if (!provenance.flat_candidates.empty() || !provenance.tuple_element_candidates.empty() || provenance.is_map)
            output_to_sources.emplace(child->getAliasOrColumnName(), std::move(provenance));
    }
    return output_to_sources;
}

/// Number of JSON nodes anywhere in the type, walking the same containers as containsJSONObjectType.
static size_t countJSONObjectTypes(const IDataType & type)
{
    if (typeid_cast<const DataTypeObject *>(&type))
        return 1;
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return countJSONObjectTypes(*array->getNestedType());
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return countJSONObjectTypes(*nullable->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        size_t count = 0;
        for (const auto & element : tuple->getElements())
            count += countJSONObjectTypes(*element);
        return count;
    }
    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
        return countJSONObjectTypes(*map->getKeyType()) + countJSONObjectTypes(*map->getValueType());
    return 0;
}

/// One member step of a qualified candidate: Tuple elements by name or 1-based number, Map sides by
/// "keys"/"values", a constant subscript by "[]"; Array/Nullable wrappers are looked through (member
/// access maps over arrays).
static DataTypePtr descendJSONPolicySourceIntoMember(DataTypePtr type, std::string_view member)
{
    while (true)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            type = nullable->getNestedType();
        else if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
            type = array->getNestedType();
        else
            break;
    }

    /// A constant subscript: on a Map it selects the value side, on an Array it selects the element,
    /// which the wrapper peeling above has already reached.
    if (member == SUBSCRIPT_MEMBER)
    {
        if (const auto * subscripted_map = typeid_cast<const DataTypeMap *>(type.get()))
            return subscripted_map->getValueType();
        return type;
    }

    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        if (tuple->hasExplicitNames())
        {
            auto position = tuple->tryGetPositionByName(String(member));
            if (position)
                return tuple->getElements()[*position];
        }
        UInt64 index = 0;
        if (tryParse(index, member.data(), member.size()) && index >= 1 && index <= tuple->getElements().size())
            return tuple->getElements()[index - 1];
        return nullptr;
    }

    if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        if (member == "keys")
            return map->getKeyType();
        if (member == "values")
            return map->getValueType();
        return nullptr;
    }

    return nullptr;
}

/// A name containing the "[]" step is never a physical column or subcolumn name, but a JSON column
/// resolves any dotted tail as a path subcolumn, which would donate a policy-free Dynamic type.
static bool containsSubscriptMemberStep(std::string_view name)
{
    return name.contains(String(".") + String(SUBSCRIPT_MEMBER));
}

/// Resolves a member-qualified candidate ("t.doc", "m.values", "t.2") against `try_get_column`: takes
/// the longest prefix that is a physical column and descends the remaining members through its type,
/// so only the member the projection actually read donates its policy.
template <typename TryGetColumn>
static DataTypePtr resolveMemberQualifiedPolicySource(const String & candidate_name, TryGetColumn && try_get_column)
{
    for (size_t pos = candidate_name.rfind('.'); pos != String::npos;
         pos = (pos == 0 ? String::npos : candidate_name.rfind('.', pos - 1)))
    {
        auto prefix = candidate_name.substr(0, pos);
        if (containsSubscriptMemberStep(prefix))
            continue;

        auto base = try_get_column(prefix);
        if (!base)
            continue;

        DataTypePtr current = base->type;
        size_t member_begin = pos + 1;
        while (current && member_begin <= candidate_name.size())
        {
            size_t member_end = candidate_name.find('.', member_begin);
            if (member_end == String::npos)
                member_end = candidate_name.size();
            current = descendJSONPolicySourceIntoMember(
                current, std::string_view(candidate_name).substr(member_begin, member_end - member_begin));
            member_begin = member_end + 1;
        }
        return current;
    }
    return nullptr;
}

/// Mirrors applyJSONSharedDataPathPoliciesForMutation (MutateTask.cpp) for a projection's own declared
/// columns: prefer each source's own projection part, else fall back to that source's main columns.
/// `source_part_alter_conversions` is parallel to `source_parts`: a source part written before a
/// column rename still has the old physical name, so `tryGetColumn` on it (and on its own projection
/// sub-part, which is renamed in lockstep with the main part) needs the candidate name mapped back
/// through that part's own pending renames first, the same way computeJSONProvenanceType
/// (MutateTask.cpp) and MergeTask's own base-part provenance merge already do.
static DataTypePtr resolveJSONSharedDataPathPolicyForCandidates(
    DataTypePtr type,
    const Names & candidate_names,
    const ProjectionDescription & projection,
    const MergeTreeData::DataPartsVector & source_parts,
    const PatchPartsForReader & patch_parts,
    const std::vector<AlterConversionsPtr> & source_part_alter_conversions)
{
    for (size_t source_index = 0; source_index != source_parts.size(); ++source_index)
    {
        const auto & source_part = source_parts[source_index];
        const AlterConversionsPtr * alter_conversions = source_index < source_part_alter_conversions.size()
            ? &source_part_alter_conversions[source_index]
            : nullptr;

        /// A rebuild can add a column to the projection that an existing projection sub-part
        /// doesn't have yet (see MergeTask::prepareProjectionsToMergeAndRebuild); the fallback
        /// to the source's own main column must apply per-column, not only when no sub-part
        /// exists at all, or such a column silently skips provenance merging entirely.
        const auto & source_projection_parts = source_part->getProjectionParts();
        auto projection_part_it = source_projection_parts.find(projection.name);

        const auto try_get_source_column = [&](String name) -> std::optional<NameAndTypePair>
        {
            if (alter_conversions && *alter_conversions && (*alter_conversions)->isColumnRenamed(name))
                name = (*alter_conversions)->getColumnOldName(name);
            std::optional<NameAndTypePair> column;
            if (projection_part_it != source_projection_parts.end())
                column = projection_part_it->second->tryGetColumn(name);
            if (!column)
                column = source_part->tryGetColumn(name);
            return column;
        };

        for (const auto & candidate_name : candidate_names)
        {
            DataTypePtr policy_source_type;
            /// See containsSubscriptMemberStep(): "arr.[]" must not be taken as a JSON path subcolumn.
            if (containsSubscriptMemberStep(candidate_name))
            {
                policy_source_type = resolveMemberQualifiedPolicySource(candidate_name, try_get_source_column);
            }
            else if (auto source_column = try_get_source_column(candidate_name))
            {
                policy_source_type = source_column->type;
            }
            else if (candidate_name.contains('.'))
            {
                policy_source_type = resolveMemberQualifiedPolicySource(candidate_name, try_get_source_column);
            }

            if (policy_source_type)
                type = mergeJSONSharedDataPathRules(type, policy_source_type);
        }
    }

    bool inputs_saturated = false;
    for (const auto & candidate_name : candidate_names)
    {
        type = MutationHelpers::mergeJSONSharedDataPathRulesFromPatchParts(type, candidate_name, patch_parts, inputs_saturated);

        /// That lookup only knows physical column names. A member-qualified candidate needs the same
        /// base-column descent the source-part branch above does, or a patch part that is the only
        /// remaining record of the retired policy contributes nothing.
        if (!candidate_name.contains('.'))
            continue;

        for (const auto & patch : patch_parts)
        {
            const auto try_get_patch_column = [&](String name) -> std::optional<NameAndTypePair>
            {
                if (const auto & patch_conversions = patch.part->getAlterConversions();
                    patch_conversions && patch_conversions->isColumnRenamed(name))
                    name = patch_conversions->getColumnOldName(name);
                return patch.part->tryGetColumn(name);
            };

            if (auto member_type = resolveMemberQualifiedPolicySource(candidate_name, try_get_patch_column))
            {
                inputs_saturated = inputs_saturated || hasSaturatedJSONSharedDataPathPolicy(*member_type);
                type = mergeJSONSharedDataPathRules(type, member_type);
            }
        }
    }

    return type;
}

static void applyJSONSharedDataPathPoliciesForProjection(
    NamesAndTypesList & result_columns,
    const ProjectionDescription & projection,
    const MergeTreeData::DataPartsVector & source_parts,
    const PatchPartsForReader & patch_parts,
    const std::vector<AlterConversionsPtr> & source_part_alter_conversions)
{
    auto output_to_sources = getProjectionOutputToSourceIdentifiers(projection);

    for (auto & result_column : result_columns)
    {
        auto it = output_to_sources.find(result_column.name);
        const ProjectionOutputProvenance * provenance = it != output_to_sources.end() ? &it->second : nullptr;
        bool handled_structurally = false;

        if (provenance && !provenance->tuple_element_candidates.empty())
        {
            DataTypePtr element_container_type = result_column.type;
            const DataTypeArray * target_array = nullptr;
            if (provenance->is_array_zip)
            {
                target_array = typeid_cast<const DataTypeArray *>(element_container_type.get());
                if (target_array)
                    element_container_type = target_array->getNestedType();
            }

            if (const auto * target_tuple = typeid_cast<const DataTypeTuple *>(element_container_type.get());
                target_tuple && target_tuple->getElements().size() == provenance->tuple_element_candidates.size()
                && (!provenance->is_array_zip || target_array))
            {
                DataTypes elements = target_tuple->getElements();
                for (size_t i = 0; i != elements.size(); ++i)
                    elements[i] = resolveJSONSharedDataPathPolicyForCandidates(
                        elements[i], provenance->tuple_element_candidates[i], projection,
                        source_parts, patch_parts, source_part_alter_conversions);

                DataTypePtr new_tuple = target_tuple->hasExplicitNames()
                    ? std::make_shared<DataTypeTuple>(elements, target_tuple->getElementNames())
                    : std::make_shared<DataTypeTuple>(elements);
                result_column.type = provenance->is_array_zip
                    ? DataTypePtr(std::make_shared<DataTypeArray>(std::move(new_tuple)))
                    : new_tuple;
                handled_structurally = true;
            }
        }
        else if (provenance && provenance->is_map)
        {
            if (const auto * target_map = typeid_cast<const DataTypeMap *>(result_column.type.get()))
            {
                auto key_type = resolveJSONSharedDataPathPolicyForCandidates(
                    target_map->getKeyType(), provenance->map_key_candidates, projection,
                    source_parts, patch_parts, source_part_alter_conversions);
                auto value_type = resolveJSONSharedDataPathPolicyForCandidates(
                    target_map->getValueType(), provenance->map_value_candidates, projection,
                    source_parts, patch_parts, source_part_alter_conversions);
                result_column.type = std::make_shared<DataTypeMap>(std::move(key_type), std::move(value_type));
                handled_structurally = true;
            }
        }

        if (!handled_structurally)
        {
            Names candidate_names = {result_column.name};
            if (provenance)
            {
                Names non_self_candidates;
                for (const auto & name : provenance->flat_candidates)
                    if (name != result_column.name)
                        non_self_candidates.push_back(name);
                /// Several flat candidates over an output with several JSON nodes cannot be attributed
                /// without crossing slots; one aligned donor can - the merge still shape-checks it.
                /// Conditional alternatives are each aligned with the whole output, so several of
                /// them are attributable for the same reason a single donor is.
                if (countJSONObjectTypes(*result_column.type) <= 1 || non_self_candidates.size() == 1
                    || provenance->aligned_alternatives)
                    candidate_names.insert(candidate_names.end(), non_self_candidates.begin(), non_self_candidates.end());
            }

            result_column.type = resolveJSONSharedDataPathPolicyForCandidates(
                result_column.type, candidate_names, projection, source_parts, patch_parts, source_part_alter_conversions);
        }
    }
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeProjectionPartImpl(
    const String & part_name,
    bool is_temp,
    IMergeTreeDataPart * parent_part,
    const MergeTreeData & data,
    Block block,
    const ProjectionDescription & projection,
    MergeTreeIndices indices,
    bool merge_is_needed,
    bool try_adaptive_codec,
    const MergeTreeData::DataPartsVector & source_parts,
    const PatchPartsForReader & patch_parts,
    const std::vector<AlterConversionsPtr> & source_part_alter_conversions)
{
    auto temp_part = std::make_unique<MergeTreeTemporaryPart>();
    const auto & metadata_snapshot = projection.metadata;

    MergeTreeDataPartType part_type;
    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();
    // just check if there is enough space on parent volume
    MergeTreeData::reserveSpace(expected_size, parent_part->getDataPartStorage());
    part_type = data.choosePartFormat(expected_size, block.rows(), parent_part->info.level, &projection).part_type;

    auto new_data_part = parent_part->getProjectionPartBuilder(part_name, &projection, PartDirIntent::CreateFresh, is_temp).withPartType(part_type).build();
    auto projection_part_storage = new_data_part->getDataPartStoragePtr();
    auto data_settings = data.getSettings(&projection.settings_changes);

    if (is_temp)
        projection_part_storage->beginTransaction();

    new_data_part->is_temp = is_temp;

    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    /// A merge/mutation-driven rewrite must not silently re-promote paths a retired SHARED REGEXP
    /// rule once forced into shared data; a fresh insert (merge_is_needed=false) has no such history.
    if (merge_is_needed && !(*data_settings)[MergeTreeSetting::allow_json_shared_data_paths_repromotion])
        applyJSONSharedDataPathPoliciesForProjection(columns, projection, source_parts, patch_parts, source_part_alter_conversions);

    SerializationInfo::Settings settings
    {
        static_cast<double>((*data_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization]),
        true,
        (*data_settings)[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
        (*data_settings)[MergeTreeSetting::serialization_info_version],
        (*data_settings)[MergeTreeSetting::string_serialization_version],
        (*data_settings)[MergeTreeSetting::nullable_serialization_version],
        (*data_settings)[MergeTreeSetting::map_serialization_version_for_zero_level_parts],
        (*data_settings)[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types],
    };
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());

    projection_part_storage->createDirectories();

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
    {
        auto expr = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, {});
        addSubcolumnsFromSortingKeyAndSkipIndicesExpression(expr, block);
        expr->execute(block);
    }

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    std::vector<bool> reverse_flags = metadata_snapshot->getSortingKeyReverseFlags();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        if (!reverse_flags.empty() && reverse_flags[i])
            sort_description.emplace_back(sort_columns[i], -1, 1);
        else
            sort_description.emplace_back(sort_columns[i], 1, 1);
    }

    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterBlocks);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataProjectionWriterSortingBlocksMicroseconds);

        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterBlocksAlreadySorted);
    }

    if ((*data_settings)[MergeTreeSetting::optimize_row_order]
        && data.merging_params.mode
            == MergeTreeData::MergingParams::Mode::Ordinary) /// Nobody knows if this optimization messes up specialized MergeTree engines.
    {
        RowOrderOptimizer::optimize(block, sort_description, perm);
        perm_ptr = &perm;
    }

    if (projection.type == ProjectionDescription::Type::Aggregate && merge_is_needed)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataProjectionWriterMergingBlocksMicroseconds);

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;
        block = mergeBlock(std::move(block), metadata_snapshot, sort_description, perm_ptr, projection_merging_params);
    }

    auto compression_codec = data.getCompressionCodecForPart(metadata_snapshot, 0, {}, time(nullptr)).codec;

    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        block,
        *data_settings,
        new_data_part->index_granularity_info,
        /*blocks_are_granules=*/ false);

    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        data_settings,
        metadata_snapshot,
        columns,
        indices,
        compression_codec,
        std::move(index_granularity_ptr),
        Tx::NonTransactionalTID,
        block.bytes(),
        /*reset_columns=*/ false,
        /*blocks_are_granules_size=*/ false,
        data.getContext()->getWriteSettings(),
        static_cast<WrittenOffsetSubstreams *>(nullptr),
        try_adaptive_codec,
        /// A fresh insert's projection block has no historical placement to reconsider; only
        /// merge/mutation-driven rewrites (merge_is_needed) should read this table setting.
        /*reconsider_json_shared_data_placement=*/ merge_is_needed && (*data_settings)[MergeTreeSetting::allow_json_shared_data_paths_repromotion]);

    Block permuted_columns_cache;
    out->writeWithPermutation(block, perm_ptr, &permuted_columns_cache);
    out->finalizeIndexGranularity();
    auto finalizer = out->finalizePartAsync(new_data_part, IMergedBlockOutputStream::GatheredData{}, false);
    temp_part->part = new_data_part;
    temp_part->streams.emplace_back(MergeTreeTemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});

    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return temp_part;
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeProjectionPart(
    const MergeTreeData & data,
    Block block,
    const ProjectionDescription & projection,
    IMergeTreeDataPart * parent_part,
    bool merge_is_needed,
    ContextPtr context)
{
    const auto & query_settings = context->getSettingsRef();
    auto indices = collectSkipIndicesToMaterialize(
        projection.metadata,
        query_settings[Setting::materialize_skip_indexes_on_insert],
        query_settings[Setting::exclude_materialize_skip_indexes_on_insert].toString(),
        query_settings,
        *data.getSettings());

    return writeProjectionPartImpl(
        projection.name,
        false /* is_temp */,
        parent_part,
        data,
        std::move(block),
        projection,
        std::move(indices),
        merge_is_needed,
        /*try_adaptive_codec=*/ false,
        /// A fresh insert's projection block has no prior projection, main part, or patch to read provenance from.
        /*source_parts=*/ {},
        /*patch_parts=*/ {},
        /*source_part_alter_conversions=*/ {});
}

/// This is used for projection materialization process which may contain multiple stages of
/// projection part merges.
MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempProjectionPart(
    const MergeTreeData & data,
    Block block,
    const ProjectionDescription & projection,
    IMergeTreeDataPart * parent_part,
    size_t block_num,
    ContextPtr context,
    const MergeTreeData::DataPartsVector & source_parts,
    const PatchPartsForReader & patch_parts,
    const std::vector<AlterConversionsPtr> & source_part_alter_conversions)
{
    const auto & table_settings = data.getSettings();
    auto indices = collectSkipIndicesToMaterialize(
        projection.metadata,
        (*table_settings)[MergeTreeSetting::materialize_skip_indexes_on_merge],
        (*table_settings)[MergeTreeSetting::exclude_materialize_skip_indexes_on_merge].toString(),
        context->getSettingsRef(),
        *table_settings);

    auto part_name = fmt::format("{}_{}", projection.name, block_num);
    auto new_part = writeProjectionPartImpl(
        part_name,
        /*is_temp=*/ true,
        parent_part,
        data,
        std::move(block),
        projection,
        std::move(indices),
        /*merge_is_needed=*/ true,
        /*try_adaptive_codec=*/ true,
        source_parts,
        patch_parts,
        source_part_alter_conversions);

    new_part->part->temp_projection_block_number = block_num;
    return new_part;
}

}
