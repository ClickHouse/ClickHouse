#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/ObjectUtils.h>
#include <Disks/createVolume.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/RowOrderOptimizer.h>
#include <Common/ColumnsHashing.h>
#include <Common/DateLUTImpl.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/intExp.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>

#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/CollapsingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/VersionedCollapsingAlgorithm.h>
#include <Processors/Merges/Algorithms/GraphiteRollupSortedAlgorithm.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

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
    extern const Event RejectedInserts;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool materialize_skip_indexes_on_insert;
    extern const SettingsString exclude_materialize_skip_indexes_on_insert;
    extern const SettingsBool materialize_statistics_on_insert;
    extern const SettingsBool optimize_on_insert;
    extern const SettingsBool throw_on_max_partitions_per_insert_block;
    extern const SettingsUInt64 min_free_disk_bytes_to_perform_insert;
    extern const SettingsFloat min_free_disk_ratio_to_perform_insert;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsBool fsync_after_insert;
    extern const MergeTreeSettingsBool fsync_part_directory;
    extern const MergeTreeSettingsUInt64 min_free_disk_bytes_to_perform_insert;
    extern const MergeTreeSettingsFloat min_free_disk_ratio_to_perform_insert;
    extern const MergeTreeSettingsBool optimize_row_order;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
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
        typename Data::LookupResult it;
        bool inserted;
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
        ttl_infos.updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
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

    part->getDataPartStorage().precommitTransaction();
    for (const auto & [_, projection] : part->getProjectionParts())
        projection->getDataPartStorage().precommitTransaction();
}

/// This method must be called after rename and commit of part
/// because a correct path is required for the keys of caches.
void MergeTreeTemporaryPart::prewarmCaches()
{
    size_t bytes_uncompressed = part->getBytesUncompressedOnDisk();

    if (auto mark_cache = part->storage.getMarkCacheToPrewarm(bytes_uncompressed))
    {
        for (const auto & stream : streams)
        {
            auto marks = stream.stream->releaseCachedMarks();
            addMarksToCache(*part, marks, mark_cache.get());
        }
    }

    if (auto index_cache = part->storage.getPrimaryIndexCacheToPrewarm(bytes_uncompressed))
    {
        /// Index was already set during writing. Now move it to cache.
        part->moveIndexToCache(*index_cache);
    }
}

std::vector<AsyncInsertInfoPtr> scatterAsyncInsertInfoBySelector(AsyncInsertInfoPtr async_insert_info, const IColumn::Selector & selector, size_t partition_num)
{
    if (nullptr == async_insert_info)
    {
        return {};
    }
    if (selector.empty())
    {
        return {async_insert_info};
    }
    std::vector<AsyncInsertInfoPtr> result(partition_num);
    std::vector<Int64> last_row_for_partition(partition_num, -1);
    size_t offset_idx = 0;
    for (size_t i = 0; i < selector.size(); ++i)
    {
        ++last_row_for_partition[selector[i]];
        if (i + 1 == async_insert_info->offsets[offset_idx])
        {
            for (size_t part_id = 0; part_id < last_row_for_partition.size(); ++part_id)
            {
                Int64 last_row = last_row_for_partition[part_id];
                if (-1 == last_row)
                    continue;
                size_t offset = static_cast<size_t>(last_row + 1);
                if (result[part_id] == nullptr)
                    result[part_id] = std::make_shared<AsyncInsertInfo>();
                if (result[part_id]->offsets.empty() || offset > *result[part_id]->offsets.rbegin())
                {
                    result[part_id]->offsets.push_back(offset);
                    result[part_id]->tokens.push_back(async_insert_info->tokens[offset_idx]);
                }
            }
            ++offset_idx;
        }
    }
    if (offset_idx != async_insert_info->offsets.size())
    {
        LOG_ERROR(
            getLogger("MergeTreeDataWriter"),
            "ChunkInfo of async insert offsets doesn't match the selector size {}. Offsets content is ({})",
            selector.size(), fmt::join(async_insert_info->offsets.begin(), async_insert_info->offsets.end(), ","));
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error for async deduplicated insert, please check error logs");
    }
    return result;
}

BlocksWithPartition MergeTreeDataWriter::splitBlockIntoParts(
    Block && block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, AsyncInsertInfoPtr async_insert_info)
{
    BlocksWithPartition result;
    if (block.empty() || !block.rows())
        return result;

    metadata_snapshot->check(block, true);

    if (!metadata_snapshot->hasPartitionKey()) /// Table is not partitioned.
    {
        result.emplace_back(block, Row{});
        if (async_insert_info != nullptr)
        {
            result[0].offsets = std::move(async_insert_info->offsets);
            result[0].tokens = std::move(async_insert_info->tokens);
        }
        return result;
    }

    Block block_copy = block;
    /// After expression execution partition key columns will be added to block_copy with names regarding partition function.
    auto partition_key_names_and_types = MergeTreePartition::executePartitionByExpression(metadata_snapshot, block_copy, context);

    ColumnRawPtrs partition_columns;
    partition_columns.reserve(partition_key_names_and_types.size());
    bool all_partition_columns_are_equal = true;
    for (const auto & element : partition_key_names_and_types)
    {
        partition_columns.emplace_back(block_copy.getByName(element.name).column.get());
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

    if (!all_partition_columns_are_equal)
    {
        PODArray<size_t> partition_num_to_first_row;
        IColumn::Selector selector;
        buildScatterSelector(partition_columns, partition_num_to_first_row, selector, max_parts, context);

        auto async_insert_info_with_partition = scatterAsyncInsertInfoBySelector(async_insert_info, selector, partition_num_to_first_row.size());

        size_t partitions_count = partition_num_to_first_row.size();
        result.reserve(partitions_count);

        for (size_t i = 0; i < partitions_count; ++i)
            result.emplace_back(block.cloneEmpty(), get_partition(partition_num_to_first_row[i]));

        for (size_t col = 0; col < block.columns(); ++col)
        {
            MutableColumns scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
            for (size_t i = 0; i < partitions_count; ++i)
                result[i].block.getByPosition(col).column = std::move(scattered[i]);
        }

        for (size_t i = 0; i < async_insert_info_with_partition.size(); ++i)
        {
            if (async_insert_info_with_partition[i] == nullptr)
            {
                LOG_ERROR(
                    getLogger("MergeTreeDataWriter"),
                    "The {}th element in async_insert_info_with_partition is nullptr. There are totally {} partitions in the insert. Selector content is ({}). Offsets content is ({})",
                    i, partitions_count, fmt::join(selector.begin(), selector.end(), ","), fmt::join(async_insert_info->offsets.begin(), async_insert_info->offsets.end(), ","));
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error for async deduplicated insert, please check error logs");
            }
            result[i].offsets = std::move(async_insert_info_with_partition[i]->offsets);
            result[i].tokens = std::move(async_insert_info_with_partition[i]->tokens);
        }
    }
    else
    {
        /// A typical case is when there is one partition (you do not need to split anything).
        /// NOTE: returning a copy of the original block so that calculated partition key columns
        /// do not interfere with possible calculated primary key columns of the same name.
        result.emplace_back(block, get_partition(0));
        if (async_insert_info != nullptr)
        {
            result[0].offsets = std::move(async_insert_info->offsets);
            result[0].tokens = std::move(async_insert_info->tokens);
        }
    }

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
                    header, 1, sort_description, merging_params.columns_to_sum,
                    required_columns, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt, "sumWithOverflow", "sumMapWithOverflow", true, false);
            }
            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedAlgorithm>(header, 1, sort_description, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt);
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
                    required_columns, block_size + 1, /*block_size_bytes=*/0, /*max_dynamic_subcolumns=*/std::nullopt, "last_value", "last_value", false, true);
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


MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPart(BlockWithPartition & block, StorageMetadataPtr metadata_snapshot, ContextPtr context)
{
    auto partition_id = block.partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    return writeTempPartImpl(block, std::move(metadata_snapshot), std::move(partition_id), /*source_parts_set=*/ {}, std::move(context), data.insert_increment.get());
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPatchPart(
    BlockWithPartition & block,
    StorageMetadataPtr metadata_snapshot,
    String partition_id,
    SourcePartsSetForPatch source_parts_set,
    ContextPtr context)
{
    return writeTempPartImpl(block, std::move(metadata_snapshot), std::move(partition_id), std::move(source_parts_set), std::move(context), data.insert_increment.get());
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempPartImpl(
    BlockWithPartition & block_with_partition,
    StorageMetadataPtr metadata_snapshot,
    String partition_id,
    SourcePartsSetForPatch source_parts_set,
    ContextPtr context,
    UInt64 block_number)
{
    auto temp_part = std::make_unique<MergeTreeTemporaryPart>();
    Block & block = block_with_partition.block;
    MergeTreePartition & partition = block_with_partition.partition;

    auto columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    for (auto & column : columns)
        if (column.type->hasDynamicSubcolumnsDeprecated())
            column.type = block.getByName(column.name).type;

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    bool optimize_on_insert = !isPatchPartitionId(partition_id)
        && context->getSettingsRef()[Setting::optimize_on_insert]
        && data.merging_params.mode != MergeTreeData::MergingParams::Ordinary;

    UInt32 new_part_level = optimize_on_insert ? 1 : 0;
    MergeTreePartInfo new_part_info(std::move(partition_id), block_number, block_number, new_part_level);

    if (!source_parts_set.empty())
        new_part_info.mutation = source_parts_set.getMaxDataVersion();

    String part_name;
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date(minmax_idx->hyperrectangle[data.minmax_idx_date_column_pos].left.safeGet<UInt64>());
        DayNum max_date(minmax_idx->hyperrectangle[data.minmax_idx_date_column_pos].right.safeGet<UInt64>());

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
    temp_part->temporary_directory_lock = data.getTemporaryPartDirectoryHolder(part_dir);

    MergeTreeIndices indices;
    /// Create skip indexes if setting materialize_skip_indexes_on_insert = true
    if (context->getSettingsRef()[Setting::materialize_skip_indexes_on_insert])
    {
        const auto & index_descriptions = metadata_snapshot->getSecondaryIndices();
        auto exclude_indexes_string = context->getSettingsRef()[Setting::exclude_materialize_skip_indexes_on_insert].toString();

        /// Check if user specified list of indexes to exclude from materialize on INSERT
        if (!exclude_indexes_string.empty())
        {
            std::unordered_set<String> exclude_index_names
                = parseIdentifiersOrStringLiteralsToSet(exclude_indexes_string, context->getSettingsRef());

            for (const auto & index : index_descriptions)
                if (!exclude_index_names.contains(index.name))
                    indices.emplace_back(MergeTreeIndexFactory::instance().get(index));
        }
        else /// All indexes will be materialized on INSERT
            indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());
    }


    ColumnsStatistics statistics;
    if (context->getSettingsRef()[Setting::materialize_statistics_on_insert])
        statistics = MergeTreeStatisticsFactory::instance().getMany(metadata_snapshot->getColumns());

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
    {
        auto expr = data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices);
        addSubcolumnsFromSortingKeyAndSkipIndicesExpression(expr, block);
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices)->execute(block);
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

    if ((*data.getSettings())[MergeTreeSetting::optimize_row_order]
            && data.merging_params.mode == MergeTreeData::MergingParams::Mode::Ordinary) /// Nobody knows if this optimization messes up specialized MergeTree engines.
    {
        RowOrderOptimizer::optimize(block, sort_description, perm);
        perm_ptr = &perm;
    }

    if (optimize_on_insert)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterMergingBlocksMicroseconds);
        block = mergeBlock(std::move(block), metadata_snapshot, sort_description, perm_ptr, data.merging_params);
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

    ReservationPtr reservation = data.reserveSpacePreferringTTLRules(metadata_snapshot, expected_size, move_ttl_infos, time(nullptr), 0, true);
    VolumePtr volume = data.getStoragePolicy()->getVolume(0);
    VolumePtr data_part_volume = createVolumeFromReservation(reservation, volume);

    const auto & global_settings = context->getSettingsRef();
    const auto & data_settings = data.getSettings();

    const UInt64 & min_bytes_to_perform_insert =
            (*data_settings)[MergeTreeSetting::min_free_disk_bytes_to_perform_insert].changed
            ? (*data_settings)[MergeTreeSetting::min_free_disk_bytes_to_perform_insert]
            : global_settings[Setting::min_free_disk_bytes_to_perform_insert];

    const Float32 & min_ratio_to_perform_insert =
        (*data_settings)[MergeTreeSetting::min_free_disk_ratio_to_perform_insert].changed
        ? (*data_settings)[MergeTreeSetting::min_free_disk_ratio_to_perform_insert]
        : global_settings[Setting::min_free_disk_ratio_to_perform_insert];

    const bool is_system_database = (data.getStorageID().getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);

    if (!is_system_database && (min_bytes_to_perform_insert > 0 || min_ratio_to_perform_insert > 0.0))
    {
        const auto & disk = data_part_volume->getDisk();
        const UInt64 & total_disk_bytes = disk->getTotalSpace().value_or(0);
        const UInt64 & free_disk_bytes = disk->getUnreservedSpace().value_or(0);

        const UInt64 & min_bytes_from_ratio = static_cast<UInt64>(min_ratio_to_perform_insert * total_disk_bytes);
        const UInt64 & needed_free_bytes = std::max(min_bytes_to_perform_insert, min_bytes_from_ratio);

        if (needed_free_bytes > free_disk_bytes)
        {
            throw Exception(
                ErrorCodes::NOT_ENOUGH_SPACE,
                "Could not perform insert. "
                "The amount of free space ({}) on disk {} is less than the configured threshold ({})."
                "The threshold can be configured either in MergeTree settings or User settings, "
                "using the following settings: "
                "(1) `min_free_disk_bytes_to_perform_insert` "
                "(2) `min_free_disk_ratio_to_perform_insert`. "
                "The total disk capacity of {} is {}",
                formatReadableSizeWithBinarySuffix(free_disk_bytes),
                backQuote(disk->getName()),
                formatReadableSizeWithBinarySuffix(needed_free_bytes),
                backQuote(disk->getName()),
                formatReadableSizeWithBinarySuffix(total_disk_bytes));
        }
    }

    auto new_data_part = data.getDataPartBuilder(part_name, data_part_volume, part_dir, getReadSettings())
        .withPartFormat(data.choosePartFormat(expected_size, block.rows(), new_part_level))
        .withPartInfo(new_part_info)
        .build();

    auto data_part_storage = new_data_part->getDataPartStoragePtr();
    data_part_storage->beginTransaction();

    if ((*data.storage_settings.get())[MergeTreeSetting::assign_part_uuids])
        new_data_part->uuid = UUIDHelpers::generateV4();

    SerializationInfo::Settings settings
    {
        (*data_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        true,
        (*data_settings)[MergeTreeSetting::serialization_info_version],
        (*data_settings)[MergeTreeSetting::string_serialization_version],
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
    new_data_part->setSourcePartsSet(std::move(source_parts_set));
    new_data_part->rows_count = block.rows();
    new_data_part->existing_rows_count = block.rows();
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->is_temp = true;
    /// In case of replicated merge tree with zero copy replication
    /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    SyncGuardPtr sync_guard;

    /// The name could be non-unique in case of stale files from previous runs.
    String full_path = new_data_part->getDataPartStorage().getFullPath();

    if (new_data_part->getDataPartStorage().exists())
    {
        LOG_WARNING(log, "Removing old temporary directory {}", full_path);
        data_part_storage->removeRecursive();
    }

    data_part_storage->createDirectories();

    if ((*data_settings)[MergeTreeSetting::fsync_part_directory])
    {
        const auto disk = data_part_volume->getDisk();
        sync_guard = disk->getDirectorySyncGuard(full_path);
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

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.getContext()->chooseCompressionCodec(0, 0);

    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        block.rows(),
        block.bytes(),
        *data.getSettings(),
        new_data_part->index_granularity_info,
        /*blocks_are_granules=*/ false);

    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        columns,
        indices,
        statistics,
        compression_codec,
        std::move(index_granularity_ptr),
        context->getCurrentTransaction() ? context->getCurrentTransaction()->tid : Tx::PrehistoricTID,
        block.bytes(),
        /*reset_columns=*/ false,
        /*blocks_are_granules_size=*/ false,
        context->getWriteSettings());

    out->writeWithPermutation(block, perm_ptr);

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        Block projection_block;
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterProjectionsCalculationMicroseconds);
            projection_block = projection.calculate(block, context, perm_ptr);
            LOG_DEBUG(
                log, "Spent {} ms calculating projection {} for the part {}", watch.elapsed() / 1000, projection.name, new_data_part->name);
        }

        if (projection_block.rows())
        {
            auto proj_temp_part
                = writeProjectionPart(data, log, projection_block, projection, new_data_part.get(), /*merge_is_needed=*/false);
            new_data_part->addProjectionPart(projection.name, std::move(proj_temp_part->part));
            for (auto & stream : proj_temp_part->streams)
                temp_part->streams.emplace_back(std::move(stream));
        }
    }

    auto finalizer = out->finalizePartAsync(
        new_data_part,
        (*data_settings)[MergeTreeSetting::fsync_after_insert],
        nullptr, nullptr);

    temp_part->part = new_data_part;
    temp_part->streams.emplace_back(MergeTreeTemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return temp_part;
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeProjectionPartImpl(
    const String & part_name,
    bool is_temp,
    IMergeTreeDataPart * parent_part,
    const MergeTreeData & data,
    LoggerPtr log,
    Block block,
    const ProjectionDescription & projection,
    bool merge_is_needed)
{
    auto temp_part = std::make_unique<MergeTreeTemporaryPart>();
    const auto & metadata_snapshot = projection.metadata;

    MergeTreeDataPartType part_type;
    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();
    // just check if there is enough space on parent volume
    MergeTreeData::reserveSpace(expected_size, parent_part->getDataPartStorage());
    part_type = data.choosePartFormat(expected_size, block.rows(), parent_part->info.level).part_type;

    auto new_data_part = parent_part->getProjectionPartBuilder(part_name, is_temp).withPartType(part_type).build();
    auto projection_part_storage = new_data_part->getDataPartStoragePtr();

    if (is_temp)
        projection_part_storage->beginTransaction();

    new_data_part->is_temp = is_temp;

    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());
    SerializationInfo::Settings settings
    {
        (*data.getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        true,
        (*data.getSettings())[MergeTreeSetting::serialization_info_version],
        (*data.getSettings())[MergeTreeSetting::string_serialization_version],
    };
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());

    /// The name could be non-unique in case of stale files from previous runs.
    if (projection_part_storage->exists())
    {
        LOG_WARNING(log, "Removing old temporary directory {}", projection_part_storage->getFullPath());
        projection_part_storage->removeRecursive();
    }

    projection_part_storage->createDirectories();

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, {})->execute(block);

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

    if ((*data.getSettings())[MergeTreeSetting::optimize_row_order]
            && data.merging_params.mode == MergeTreeData::MergingParams::Mode::Ordinary) /// Nobody knows if this optimization messes up specialized MergeTree engines.
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

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.getContext()->chooseCompressionCodec(0, 0);

    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        block.rows(),
        block.bytes(),
        *data.getSettings(),
        new_data_part->index_granularity_info,
        /*blocks_are_granules=*/ false);

    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        columns,
        MergeTreeIndices{},
        /// TODO(hanfei): It should be helpful to write statistics for projection result.
        ColumnsStatistics{},
        compression_codec,
        std::move(index_granularity_ptr),
        Tx::PrehistoricTID,
        block.bytes(),
        /*reset_columns=*/ false,
        /*blocks_are_granules_size=*/ false,
        data.getContext()->getWriteSettings());

    out->writeWithPermutation(block, perm_ptr);
    auto finalizer = out->finalizePartAsync(new_data_part, false);
    temp_part->part = new_data_part;
    temp_part->streams.emplace_back(MergeTreeTemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});

    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataProjectionWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return temp_part;
}

MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeProjectionPart(
    const MergeTreeData & data,
    LoggerPtr log,
    Block block,
    const ProjectionDescription & projection,
    IMergeTreeDataPart * parent_part,
    bool merge_is_needed)
{
    return writeProjectionPartImpl(
        projection.name, false /* is_temp */, parent_part, data, log, std::move(block), projection, merge_is_needed);
}

/// This is used for projection materialization process which may contain multiple stages of
/// projection part merges.
MergeTreeTemporaryPartPtr MergeTreeDataWriter::writeTempProjectionPart(
    const MergeTreeData & data,
    LoggerPtr log,
    Block block,
    const ProjectionDescription & projection,
    IMergeTreeDataPart * parent_part,
    size_t block_num)
{
    auto part_name = fmt::format("{}_{}", projection.name, block_num);
    auto new_part = writeProjectionPartImpl(
        part_name, /*is_temp=*/ true, parent_part, data, log, std::move(block), projection, /*merge_is_needed=*/true);

    new_part->part->temp_projection_block_number = block_num;
    return new_part;
}

}
