#include <Storages/MergeTree/WhatIf/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnSparse.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/WhatIf/InterpreterHypotheticalObjectQuery.h>
#include <IO/NullWriteBuffer.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/WhatIf/HypotheticalProjection.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/WhatIf/WhatIfSampling.h>
#include <Storages/MergeTree/PartDirIntent.h>
#include <Storages/MergeTree/WhatIf/WhatIfSettings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/getEffectiveRowPolicyFilter.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

namespace FailPoints
{
    extern const char whatif_projection_scan_cut_short[];
}

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsBool prefer_optimize_projection;
    extern const SettingsBool use_constant_folding_in_index_analysis;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
    extern const MergeTreeSettingsNonZeroUInt64 merge_max_block_size;
    extern const MergeTreeSettingsUInt64 merge_max_block_size_bytes;
}

namespace
{

struct ProjectionPartData
{
    /// projection key columns, in read order
    Block key_block;
    /// sort permutation of key_block
    IColumn::Permutation order;
    size_t rows = 0;
    /// parent rows read, the filter of the projection keeps a share of them
    size_t read_rows = 0;
    /// uncompressed bytes with the key columns, used for granularity
    size_t bytes = 0;
    /// uncompressed bytes without the key columns, used for the part format
    size_t format_bytes = 0;
    /// per-row bytes, only filled for adaptive granularity
    PaddedPODArray<UInt32> row_bytes;
    /// for a level-zero aggregate part: the part that `MATERIALIZE PROJECTION` writes, if it can differ from the part of an insert
    /// its `format_bytes` are those of the block before the merge, the mutation chooses the format from them
    std::unique_ptr<ProjectionPartData> materialized;
    /// some stored column has values of different sizes
    bool variable_width = false;
    /// a merge sizes the granules by what the part does not record: the memory it builds some states in, or the bytes of the parts it merges
    bool merge_history_unknown = false;
    /// an order-dependent state that is not of a fixed size can keep other values in another order of the rows, as `any` does
    /// a part does not record the order of the insert or of the merged parts, the estimate reads the rows in the order of the part
    bool order_dependent_states = false;
    /// for a merged wide part under a constant granularity: the fewest and the most bytes that its merged parts can store for a row
    size_t stored_row_bytes_least = 0;
    size_t stored_row_bytes_most = 0;
    /// the writer can choose the other part format too: the part records neither the parts of a merge nor the settings of an insert
    bool both_formats = false;
    /// for a merged wide part under a constant granularity: the most marks of its layouts, if a granule size between them has no layout
    size_t marks_between_layouts = 0;
    /// for a sample of a projection that drops rows: the rows that the projection keeps of each granule read
    std::vector<UInt32> granule_rows;
};

/// per-row version of `getBlockSizeForGranularity`
/// a merge builds the states in the memory of its column, and the writer counts that memory if it is more than the serialized states
void appendRowSizes(ProjectionPartData & out, const Block & block, bool merged)
{
    auto & row_bytes = out.row_bytes;
    const size_t rows = block.rows();
    const size_t offset = row_bytes.size();
    row_bytes.resize_fill(offset + rows, 0);

    UInt32 fixed = 0;
    for (const auto & elem : block)
    {
        if (!elem.column)
            continue;
        /// the writer sizes a state as it serializes it, see `ColumnAggregateFunction::serializedSizeEstimate`
        if (const auto * states = typeid_cast<const ColumnAggregateFunction *>(elem.column.get()))
        {
            const auto & function = *states->getAggregateFunction();
            if (function.hasTrivialDestructor() && !function.allocatesMemoryInArena())
            {
                fixed += static_cast<UInt32>(sizeof(AggregateDataPtr) + function.sizeOfData());
                continue;
            }
            out.variable_width = true;
            const auto properties = AggregateFunctionFactory::instance().tryGetProperties(function.getName(), NullsAction::EMPTY);
            out.order_dependent_states |= !properties || properties->is_order_dependent;
            /// a state built from one state, more merges can leave more memory
            out.merge_history_unknown |= merged && function.allocatesMemoryInArena();
            auto rebuilt = merged ? states->cloneEmpty() : nullptr;
            for (size_t i = 0; i < rows; ++i)
            {
                NullWriteBuffer serialized;
                function.serialize(states->getData()[i], serialized);
                size_t bytes = sizeof(AggregateDataPtr) + serialized.count();
                if (rebuilt)
                {
                    const size_t before = rebuilt->byteSize();
                    assert_cast<ColumnAggregateFunction &>(*rebuilt).insertFrom(states->getData()[i]);
                    bytes = std::max(bytes, rebuilt->byteSize() - before);
                }
                row_bytes[offset + i] += static_cast<UInt32>(bytes);
            }
            continue;
        }
        if (elem.column->valuesHaveFixedSize())
            fixed += static_cast<UInt32>(elem.column->sizeOfValueIfFixed());
        else
        {
            out.variable_width = true;
            for (size_t i = 0; i < rows; ++i)
                row_bytes[offset + i] += static_cast<UInt32>(elem.column->byteSizeAt(i));
        }
    }
    for (size_t i = 0; i < rows; ++i)
        row_bytes[offset + i] += fixed;
}

/// the bytes that a part stores for a column
size_t storedBytes(const ISerialization & serialization, const IColumn & column)
{
    NullWriteBuffer stored;
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath &) -> WriteBuffer * { return &stored; };
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(column, settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(column, 0, 0, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
    return stored.count();
}

/// the fewest and the most bytes that a part stores for one row of the column
std::pair<size_t, size_t> rowStoredBytes(const ISerialization & serialization, const IColumn & column)
{
    size_t least = std::numeric_limits<size_t>::max();
    size_t most = 0;
    const size_t rows = column.valuesHaveFixedSize() ? 1 : column.size();
    for (size_t row = 0; row < rows; ++row)
    {
        const size_t bytes = storedBytes(serialization, *column.cut(row, 1));
        least = std::min(least, bytes);
        most = std::max(most, bytes);
    }
    return {least, most};
}

/// a merge under a constant granularity sizes the granule of a wide part from the bytes per row that its merged parts store
/// a group of the part is in one or more of them, so that average is between the fewest and the most bytes of one row
/// a state there is at least an empty state, and a state that is not of a fixed size can be of any size there
/// a part can store a key column as sparse, then a row with the default value can take no bytes there
/// a part records neither the settings of the parts it merged nor whether they were sparse
void setStoredBytes(ProjectionPartData & out, const Block & block)
{
    if (block.rows() == 0)
        return;
    for (const auto & elem : block)
    {
        const auto serialization = elem.type->getDefaultSerialization();
        auto [least, most] = rowStoredBytes(*serialization, *elem.column);
        out.stored_row_bytes_most += most;

        /// a type that stores data for each part, as the dictionary of `LowCardinality`, stores more for a row in more parts
        /// two parts of one row store that data once more than one part of two rows, and a row alone in a part stores the most
        auto row = elem.column->cut(0, 1);
        auto row_twice = row->cloneEmpty();
        row_twice->insertManyFrom(*row, 0, 2);
        if (2 * storedBytes(*serialization, *row) > storedBytes(*serialization, *row_twice))
        {
            out.merge_history_unknown = true;
            continue;
        }

        if (const auto * states = typeid_cast<const ColumnAggregateFunction *>(elem.column.get()))
        {
            const auto & function = *states->getAggregateFunction();
            out.merge_history_unknown |= !function.hasTrivialDestructor() || function.allocatesMemoryInArena();
            auto empty = states->cloneEmpty();
            empty->insertDefault();
            NullWriteBuffer empty_state;
            function.serialize(assert_cast<const ColumnAggregateFunction &>(*empty).getData()[0], empty_state);
            least = std::min(least, empty_state.count());
        }
        else if (isTuple(elem.type) || (elem.type->supportsSparseSerialization() && elem.column->getNumberOfDefaultRows() != 0))
            least = 0;
        out.stored_row_bytes_least += least;
    }
}

/// the whole part, or a sample of its granules
MarkRanges marksToScan(const DataPartPtr & part, size_t sample_step)
{
    const size_t marks = part->index_granularity->getMarksCountWithoutFinal();
    if (sample_step <= 1)
        return {{0, marks}};
    MarkRanges ranges;
    for (size_t mark : sampleUnits(marks, sample_step, part->name))
        ranges.emplace_back(mark, mark + 1);
    return ranges;
}

/// grow a [low, high] mark range by `spread` on both sides
void widen(UInt64 & low, UInt64 & high, double spread)
{
    const auto by = static_cast<UInt64>(std::ceil(spread));
    low = low > by ? low - by : 0;
    high += by;
}

/// the fewest and most marks that a granule size between two layouts of a part can select, from the marks that one layout selects
/// a run of marks of the finer layout gains at most one mark, a run of the coarser one keeps at least every other mark
std::pair<UInt64, UInt64> marksBetweenLayouts(const MarkRanges & ranges)
{
    UInt64 fewest = 0;
    UInt64 runs = 0;
    size_t run = 0;
    size_t run_end = 0;
    for (const auto & range : ranges)
    {
        if (run == 0 || range.begin != run_end)
        {
            fewest += (run + 1) / 2;
            ++runs;
            run = 0;
        }
        run += range.end - range.begin;
        run_end = range.end;
    }
    fewest += (run + 1) / 2;
    return {fewest, ranges.getNumberOfMarks() + runs};
}

/// read the given granules of a part
Pipe makePartPipe(
    const DataPartPtr & part,
    const MarkRanges & ranges,
    const Names & columns_to_read,
    ReadFromMergeTree * read_step,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    /// apply patch parts and on-the-fly mutations
    auto alter_conversions = mutations_snapshot
        ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
            )
        : std::make_shared<AlterConversions>();

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        data,
        read_step->getStorageSnapshot(),
        RangesInDataPart(part),
        alter_conversions,
        nullptr,
        columns_to_read,
        ranges,
        std::make_shared<std::atomic<size_t>>(0),
        false,
        false,
        /*allow_remote_fs_prefetch=*/ true);

    /// keep speed limits, the caller checks sizes
    if (auto query_limits = read_step->getQueryInfo().storage_limits)
    {
        auto speed_limits = std::make_shared<StorageLimitsList>(*query_limits);
        for (auto & entry : *speed_limits)
        {
            entry.local_limits.size_limits = {};
            entry.leaf_limits = {};
            entry.local_limits.speed_limits.max_execution_time = {};
        }
        for (const auto & processor : pipe.getProcessors())
            processor->setStorageLimits(speed_limits);
    }

    return pipe;
}

/// the bytes of a calculated block with its states in memory of the aggregation, and with its states in memory of their own
/// an insert keeps the memory of the aggregation for one block, and copies the states for more blocks, see `ProjectionDataSink`
std::pair<size_t, size_t> calculatedBlockBytes(const Block & block)
{
    size_t shared = 0;
    size_t owned = 0;
    for (const auto & elem : block)
    {
        const auto * states = typeid_cast<const ColumnAggregateFunction *>(elem.column.get());
        if (!states)
        {
            shared += elem.column->byteSize();
            owned += elem.column->byteSize();
            continue;
        }
        auto copy = states->cloneEmpty();
        copy->insertRangeFrom(*states, 0, states->size());
        copy->ensureOwnership();
        shared += states->size() * sizeof(AggregateDataPtr);
        owned += copy->byteSize();
    }
    return {shared, owned};
}

/// one row for each group, with the merged states, as in a merged aggregate projection part
/// the same sort and merge as the writer of a projection part, see `MergeTreeDataWriter::writeProjectionPartImpl`
Block mergeGroups(const ProjectionDescription & projection, const Blocks & blocks)
{
    Block block = concatenateBlocks(blocks);
    SortDescription sort_description;
    for (const auto & column_name : projection.metadata->getSortingKeyColumns())
        sort_description.emplace_back(column_name, 1, 1);

    IColumn::Permutation permutation;
    stableGetPermutation(block, sort_description, permutation);
    IColumn::Permutation * permutation_ptr = &permutation;

    MergeTreeData::MergingParams merging_params;
    merging_params.mode = MergeTreeData::MergingParams::Aggregating;
    return MergeTreeDataWriter::mergeBlock(std::move(block), projection.metadata, std::move(sort_description), permutation_ptr, merging_params);
}

/// read only the key columns, returns why the estimate is unsupported or an empty string
String buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    const MarkRanges & ranges,
    bool sampled,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    bool need_row_bytes,
    bool merged_part,
    bool constant_granularity,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    /// a projection part keeps only the rows that exist, `calculate` drops the deleted ones by this column
    Names columns_to_read = projection.required_columns;
    const bool has_delete_mask = part->hasLightweightDelete();
    if (has_delete_mask)
        columns_to_read.push_back(RowExistsColumn::name);

    Pipe pipe = makePartPipe(part, ranges, columns_to_read, read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// count against the query's quota
    pipeline.setNormalizedQueryHash(context->getNormalizedQueryHash());
    PullingPipelineExecutor executor(pipeline);

    /// an aggregate projection part has one row for each group of the part, so the groups of all batches merge first
    const bool aggregate = projection.type == ProjectionDescription::Type::Aggregate;
    /// an insert calculates the projection of the whole part at once, see `MergeTreeDataWriter::writeTempPart`
    /// other parts: the source gives one block for each granule, and each `calculate` builds an interpreter, so call it for each batch
    /// the filter of the projection works row by row, so the size of the batch does not change the result
    /// a sample keeps the granule of each projection row, so the scan calculates a projection that drops rows for each granule
    const bool per_granule = sampled && (projection.where_clause_ast || has_delete_mask);
    const size_t batch_limit = per_granule ? 1
        : aggregate && !merged_part        ? std::numeric_limits<size_t>::max()
                                           : context->getSettingsRef()[Setting::max_block_size];
    Blocks batch;
    size_t batch_rows = 0;
    MutableColumns key_columns;
    /// the stored columns, as the writer gets them
    auto add = [&](ProjectionPartData & target, MutableColumns & keys, Block projected, bool merged_states)
    {
        /// the writer chooses the part format before the key expression and sizes the granules after it
        target.format_bytes += projected.bytes();
        /// `required_columns` can skip a subcolumn the key needs, add it back like the writer does
        for (const auto & required : proj_key.expression->getRequiredColumns())
            if (!projected.has(required))
                projected.insert(projected.getSubcolumnByName(required));
        proj_key.expression->execute(projected);
        target.bytes += getBlockSizeForGranularity(projected);
        if (need_row_bytes)
            appendRowSizes(target, projected, merged_states);

        if (keys.empty())
            for (const auto & name : proj_key.column_names)
                keys.push_back(projected.getByName(name).column->convertToFullColumnIfConst()->cloneEmpty());

        for (size_t i = 0; i < keys.size(); ++i)
        {
            auto source = projected.getByName(proj_key.column_names[i]).column->convertToFullColumnIfConst();
            keys[i]->insertRangeFrom(*source, 0, source->size());
        }
    };
    auto finish = [&](ProjectionPartData & target, MutableColumns & keys)
    {
        if (keys.empty() || keys[0]->empty())
            return;
        target.rows = keys[0]->size();
        for (size_t i = 0; i < keys.size(); ++i)
            target.key_block.insert({std::move(keys[i]), proj_key.data_types[i], proj_key.column_names[i]});
    };
    Blocks groups;
    /// past the group limit of the query the estimate is unsupported instead of failing the query, so a batch stops at the limit
    const UInt64 max_groups = context->getSettingsRef()[Setting::max_rows_to_group_by];
    ContextPtr projection_context = context;
    if (aggregate && max_groups)
    {
        auto limited_context = Context::createCopy(context);
        limited_context->setSetting("group_by_overflow_mode", Field("break"));
        projection_context = std::move(limited_context);
    }
    /// `MATERIALIZE PROJECTION` calculates with the settings of the background profile, see `MutatePlainMergeTreeTask::createTaskContext`
    /// the settings of the aggregation can change how the calculated block holds the states, and so its bytes
    ContextMutablePtr mutation_context;
    Block mutation_projected;
    if (aggregate && !merged_part)
    {
        mutation_context = Context::createCopy(read_step->getMergeTreeData().getContext()->getBackgroundContext());
        mutation_context->makeQueryContext();
        /// `KILL QUERY` and the time limit of the query stop this calculation too
        mutation_context->setProcessListElement(context->getProcessListElement());
        /// the group limit of the query bounds this calculation too, the estimate is unsupported past it
        if (max_groups)
        {
            mutation_context->setSetting("max_rows_to_group_by", Field(max_groups));
            mutation_context->setSetting("group_by_overflow_mode", Field("break"));
        }
    }
    auto flush = [&]
    {
        if (batch.empty())
            return;
        const auto rows = concatenateBlocks(batch);
        auto projected = projection.calculate(rows, out.read_rows - batch_rows, projection_context);
        if (mutation_context)
            mutation_projected = projection.calculate(rows, out.read_rows - batch_rows, mutation_context);
        batch.clear();
        batch_rows = 0;
        if (per_granule)
            out.granule_rows.push_back(static_cast<UInt32>(projected.rows()));
        if (aggregate)
            groups.push_back(std::move(projected));
        else
            add(out, key_columns, std::move(projected), false);
    };

    size_t read_bytes = 0;
    Block block;
    while (executor.pull(block))
    {
        /// a test stops the read here, as a time limit in `break` mode does
        bool cut_short = false;
        fiu_do_on(FailPoints::whatif_projection_scan_cut_short, { cut_short = true; });
        if (cut_short)
            break;

        if (!block.rows())
            continue;

        total_rows_read += block.rows();
        total_bytes_read += block.bytes();
        read_bytes += block.bytes();
        /// past a limit the estimate is unsupported instead of failing the query, reaching it is fine as for the query
        if ((read_limits.max_rows && total_rows_read > read_limits.max_rows)
            || (read_limits.max_bytes && total_bytes_read > read_limits.max_bytes))
            return "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";

        /// key expression and sort need full columns
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);

        out.read_rows += block.rows();
        batch_rows += block.rows();
        batch.push_back(std::move(block));
        if (batch_rows >= batch_limit)
            flush();
    }
    flush();
    if (!groups.empty())
    {
        auto merged = merged_part ? mergeGroups(projection, groups) : std::move(groups.front());
        /// the groups of all batches hold the groups of each batch, so a batch that stopped at the limit is over it here
        if (max_groups && merged.rows() > max_groups)
            return fmt::format(
                "The projection part of part {} has more than max_rows_to_group_by = {} groups", part->name, max_groups);
        if (merged_part)
        {
            /// a merge chooses the format from the rows and bytes of the parts it merges, and keeps a wide one
            /// a part records neither these parts nor the settings that chose their formats, so each format is possible
            out.both_formats = read_step->getMergeTreeData().canUsePolymorphicParts();
            if (constant_granularity)
                setStoredBytes(out, merged);
        }
        else
        {
            /// a mutation calculates the projection in batches of the parent rows and merges the parts of more batches, see `PartMergerWriter`
            const auto & background = read_step->getMergeTreeData().getContext()->getBackgroundContext()->getSettingsRef();
            const size_t mutation_rows = background[Setting::min_insert_block_size_rows];
            const size_t mutation_bytes = background[Setting::min_insert_block_size_bytes];
            out.merge_history_unknown |= (mutation_rows == 0 && mutation_bytes == 0) || (mutation_rows != 0 && out.read_rows > mutation_rows)
                || (mutation_bytes != 0 && read_bytes > mutation_bytes);
            /// `MATERIALIZE PROJECTION` merges the block that it calculates, see `writeTempProjectionPart`
            /// the merge builds each state anew, and it compares the keys as a sort does: 0 and -0 or two NaNs are one group there,
            /// the hash table of an insert keeps them apart
            out.materialized = std::make_unique<ProjectionPartData>();
            MutableColumns materialized_keys;
            add(*out.materialized, materialized_keys, mergeGroups(projection, {mutation_projected}), true);
            finish(*out.materialized, materialized_keys);
            out.materialized->format_bytes = mutation_projected.bytes();
            /// the part does not record the settings of its insert, so the format of each block of the insert is possible
            const auto & merge_tree = read_step->getMergeTreeData();
            const auto [shared_bytes, owned_bytes] = calculatedBlockBytes(merged);
            out.both_formats = merge_tree.choosePartFormat(shared_bytes, merged.rows(), part->info.level, &projection).part_type
                != merge_tree.choosePartFormat(owned_bytes, merged.rows(), part->info.level, &projection).part_type;
        }
        add(out, key_columns, std::move(merged), merged_part);
    }
    finish(out, key_columns);
    if (out.materialized && out.materialized->rows == out.rows && out.materialized->row_bytes == out.row_bytes
        && out.materialized->format_bytes == out.format_bytes)
        out.materialized.reset();

    return {};
}

/// the whole part seen through its sample, in projection order: part row `r` falls on sample row `r * sample rows / part rows`
struct PartFromSample
{
    const ProjectionPartData & sample;
    size_t rows;
    /// bytes of the part rows before the first one each sample row stands for
    std::vector<UInt64> bytes_before_sample_row;

    PartFromSample(const ProjectionPartData & sample_, size_t rows_)
        : sample(sample_), rows(rows_), bytes_before_sample_row(sample.rows + 1)
    {
        for (size_t sample_row = 0; sample_row < sample.rows; ++sample_row)
        {
            const size_t part_rows_covered = firstPartRow(sample_row + 1) - firstPartRow(sample_row);
            bytes_before_sample_row[sample_row + 1] = bytes_before_sample_row[sample_row] + sampleRowBytes(sample_row) * part_rows_covered;
        }
    }

    size_t sampleRow(size_t part_row) const { return static_cast<size_t>(UInt128(part_row) * sample.rows / rows); }
    size_t firstPartRow(size_t sample_row) const
    {
        return static_cast<size_t>((UInt128(sample_row) * rows + sample.rows - 1) / sample.rows);
    }
    /// the first sample row whose part rows start at or after `part_row`
    size_t firstSampleRowFrom(size_t part_row) const { return part_row == 0 ? 0 : sampleRow(part_row - 1) + 1; }

    size_t sampleRowBytes(size_t sample_row) const
    {
        const bool per_row = sample.row_bytes.size() == sample.rows;
        return per_row ? sample.row_bytes[sample.order[sample_row]] : std::max<size_t>(sample.bytes / sample.rows, 1);
    }

    UInt64 bytesBefore(size_t part_row) const
    {
        const size_t sample_row = sampleRow(part_row);
        if (sample_row == sample.rows)
            return bytes();
        return bytes_before_sample_row[sample_row] + sampleRowBytes(sample_row) * (part_row - firstPartRow(sample_row));
    }

    UInt64 bytes() const { return bytes_before_sample_row.back(); }
    UInt64 formatBytes() const { return static_cast<UInt64>(UInt128(sample.format_bytes) * rows / sample.rows); }
};

/// end of the block a writer gets at `row`: at most `rows_limit` rows and `bytes_limit` bytes, but at least one row
size_t blockEnd(const PartFromSample & part, size_t row, size_t rows_limit, size_t bytes_limit)
{
    const size_t end = std::min(part.rows, row + rows_limit);
    if (bytes_limit == 0)
        return end;
    const UInt64 start_bytes = part.bytesBefore(row);
    const auto longer_ends = std::views::iota(row + 2, end + 1);
    const auto fits = [&](size_t block_end) { return part.bytesBefore(block_end) - start_bytes <= bytes_limit; };
    return row + 1 + static_cast<size_t>(std::ranges::partition_point(longer_ends, fits) - longer_ends.begin());
}

/// replays the writer over the blocks, one granule size per block
std::vector<size_t> simulateWriterMarks(
    const PartFromSample & part,
    MergeTreeDataPartType part_type,
    const MergeTreeSettings & mt_settings,
    bool adaptive_marks,
    size_t block_rows_limit,
    size_t block_bytes_limit)
{
    const size_t granularity_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
    const size_t fixed_granularity_rows = mt_settings[MergeTreeSetting::index_granularity];

    MergeTreeIndexGranularityAdaptive granularity;
    size_t written = 0;
    for (size_t row = 0; row < part.rows;)
    {
        const size_t end = blockEnd(part, row, block_rows_limit, block_bytes_limit);
        const size_t block_rows = end - row;
        const size_t block_bytes = part.bytesBefore(end) - part.bytesBefore(row);
        row = end;

        const size_t granule_rows = computeIndexGranularity(
            block_rows, block_bytes, granularity_bytes, fixed_granularity_rows, /* blocks_are_granules */ false, adaptive_marks);

        /// rows that go into the mark the previous block left open
        size_t open_rows_missing = granularity.getTotalRows() - written;
        /// the wide writer first shrinks an open mark wider than this granule
        if (part_type == MergeTreeDataPartType::Wide && open_rows_missing > granule_rows)
        {
            granularity.adjustLastMark(std::max(granularity.getLastMarkRows() - open_rows_missing, granule_rows));
            open_rows_missing = granularity.getTotalRows() - written;
        }

        if (part_type == MergeTreeDataPartType::Compact)
            fillIndexGranularityForCompactPart(granularity, open_rows_missing, granule_rows, block_rows);
        else
            fillIndexGranularityForWidePart(granularity, open_rows_missing, granule_rows, block_rows);
        written += block_rows;
    }

    /// the writer trims the last mark when it closes the part
    if (granularity.getTotalRows() > written)
        granularity.adjustLastMark(granularity.getLastMarkRows() - (granularity.getTotalRows() - written));

    std::vector<size_t> mark_rows(granularity.getMarksCount());
    for (size_t mark = 0; mark < mark_rows.size(); ++mark)
        mark_rows[mark] = granularity.getMarkRows(mark);
    return mark_rows;
}


/// builds an in-memory projection part of `part_rows` rows for each layout the writer can leave
/// returns the parts and the index of the likeliest layout
std::pair<std::vector<MergeTreeDataPartPtr>, size_t> buildSyntheticProjectionParts(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const MergeTreeData & merge_tree,
    const DataPartPtr & parent_part,
    const MergeTreeSettings & mt_settings,
    bool uneven_rows,
    size_t part_rows)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    SortDescription sort_description;
    sort_description.reserve(proj_key.column_names.size());
    for (const auto & name : proj_key.column_names)
        sort_description.emplace_back(name, 1, 1);

    /// sorted order via one permutation
    stableGetPermutation(data.key_block, sort_description, data.order);
    const PartFromSample whole_part(data, part_rows);
    std::optional<PartFromSample> materialized_part;
    if (data.materialized)
    {
        stableGetPermutation(data.materialized->key_block, sort_description, data.materialized->order);
        materialized_part.emplace(*data.materialized, data.materialized->rows);
    }
    /// the layouts the writer can leave for a part format, and the index of the likeliest one
    auto layouts_of = [&](MergeTreeDataPartType part_type)
    {
        /// the writer takes the mark type from the table settings, not from the parent part
        const bool adaptive_marks = MergeTreeIndexGranularityInfo(merge_tree, mt_settings, part_type).mark_type.adaptive;
        /// only adaptive granularity lets block sizes change the layout
        const bool granularity_per_block = part_type == MergeTreeDataPartType::Compact
            || (adaptive_marks && !mt_settings[MergeTreeSetting::use_const_adaptive_granularity]);

        /// an insert writes one block, a merge writes blocks of `merge_max_block_size` cut at each source
        /// a part does not record which, so build each layout
        const size_t merge_rows = mt_settings[MergeTreeSetting::merge_max_block_size];
        const size_t merge_bytes = mt_settings[MergeTreeSetting::merge_max_block_size_bytes];
        /// one granule of bytes is the finest block size that matters
        const size_t granule_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
        std::vector<std::pair<size_t, size_t>> chunkings;
        size_t primary = 0;
        chunkings.emplace_back(whole_part.rows, 0);
        /// a level-zero aggregate part gets merge blocks only if `MATERIALIZE PROJECTION` calculates it in more batches,
        /// one batch gives one temporary part that the mutation renames, see `MergeProjectionPartsTask`
        const bool merge_blocks = parent_part->info.level > 0 || projection.type != ProjectionDescription::Type::Aggregate
            || data.merge_history_unknown;
        if (granularity_per_block && merge_blocks)
        {
            chunkings.emplace_back(merge_rows, merge_bytes);
            chunkings.emplace_back(merge_rows, granule_bytes);
            /// the writer writes a level-zero part in one block and a merged part in merge blocks
            /// when the row widths differ, the merge also cuts a block at each source
            if (parent_part->info.level > 0)
                primary = uneven_rows ? chunkings.size() - 1 : chunkings.size() - 2;
        }

        std::vector<std::vector<size_t>> layouts;
        for (const auto & [rows_limit, bytes_limit] : chunkings)
            layouts.push_back(simulateWriterMarks(whole_part, part_type, mt_settings, adaptive_marks, rows_limit, bytes_limit));
        /// a merge under a constant granularity: n rows of width w in the merged parts give granules of n / floor(n * w / granule_bytes)
        /// rows, at least granule_bytes / w, and less than granule_bytes / w * (k + 1) / k for k >= 1 granules of bytes, or twice that
        /// for fewer
        if (data.stored_row_bytes_most != 0 && !granularity_per_block)
        {
            const auto rows = static_cast<double>(whole_part.rows);
            const auto bytes = static_cast<double>(granule_bytes);
            const double widest = static_cast<double>(data.stored_row_bytes_most);
            const double narrowest = std::max(static_cast<double>(data.stored_row_bytes_least), 1.0);
            const double granules = std::floor(rows * narrowest / bytes);
            auto constant = [&](double granule_rows)
            {
                const auto rows_per_mark = static_cast<size_t>(
                    std::clamp(granule_rows, 1.0, static_cast<double>(mt_settings[MergeTreeSetting::index_granularity])));
                std::vector<size_t> rows_per_marks(whole_part.rows / rows_per_mark, rows_per_mark);
                if (whole_part.rows % rows_per_mark != 0)
                    rows_per_marks.push_back(whole_part.rows % rows_per_mark);
                return rows_per_marks;
            };
            /// the layout of one block stays, `MATERIALIZE PROJECTION` writes it
            layouts.insert(
                layouts.begin(), {constant(bytes / widest), constant(bytes / narrowest * (granules >= 1 ? (granules + 1) / granules : 2))});
            if (layouts[1].front() > layouts[0].front() + 1)
                data.marks_between_layouts = layouts[0].size();
        }
        return std::pair{std::move(layouts), primary};
    };

    const auto part_type
        = merge_tree.choosePartFormat(whole_part.formatBytes(), whole_part.rows, parent_part->info.level, &projection).part_type;
    auto [layouts, primary] = layouts_of(part_type);
    if (data.both_formats)
        std::ranges::move(
            layouts_of(part_type == MergeTreeDataPartType::Wide ? MergeTreeDataPartType::Compact : MergeTreeDataPartType::Wide).first,
            std::back_inserter(layouts));
    /// `MATERIALIZE PROJECTION` writes a part that it calculates in one batch as one block, in the format that it chose before the merge
    /// the block before the merge has a row for each group of the hash table, as the block of an insert
    const size_t written_layouts = layouts.size();
    if (materialized_part)
    {
        const auto materialized_type
            = merge_tree.choosePartFormat(data.materialized->format_bytes, whole_part.rows, parent_part->info.level, &projection).part_type;
        layouts.push_back(simulateWriterMarks(
            *materialized_part,
            materialized_type,
            mt_settings,
            MergeTreeIndexGranularityInfo(merge_tree, mt_settings, materialized_type).mark_type.adaptive,
            materialized_part->rows,
            0));
    }

    auto build = [&](const std::vector<size_t> & rows_per_mark, const PartFromSample & source) -> MergeTreeDataPartPtr
    {
        const size_t num_marks = rows_per_mark.size();
        std::vector<size_t> partial_sums(num_marks);
        for (size_t mark = 0, row = 0; mark < num_marks; ++mark)
        {
            row += rows_per_mark[mark];
            partial_sums[mark] = row;
        }

        /// primary index = the key at the first row of every granule
        Columns index_columns;
        index_columns.reserve(source.sample.key_block.columns());
        for (const auto & key_column : source.sample.key_block)
        {
            auto index_column = key_column.column->cloneEmpty();
            for (size_t mark = 0; mark < num_marks; ++mark)
                index_column->insertFrom(
                    *key_column.column, source.sample.order[source.sampleRow(mark != 0 ? partial_sums[mark - 1] : 0)]);
            index_columns.push_back(std::move(index_column));
        }

        /// `Synthetic` leaves the part directory alone, `CreateFresh` can delete a leftover `.tmp_proj`
        auto part = const_cast<IMergeTreeDataPart &>(*parent_part)
                        .getProjectionPartBuilder(projection.name, &projection, PartDirIntent::Synthetic, /* is_temp_projection */ true)
                        .withPartType(MergeTreeDataPartType::Compact)
                        .withBytesAndRows(0, source.rows, 0)
                        .build();
        part->setColumns(
            projection.metadata->getColumns().getAllPhysical(),
            SerializationInfoByName(SerializationInfoSettings{}),
            projection.metadata->getMetadataVersion());
        part->index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>(partial_sums);
        part->setIndex(index_columns);
        return part;
    };

    /// equal layouts use the same part, they have the same rows and so the same groups
    std::vector<MergeTreeDataPartPtr> built(layouts.size());
    for (size_t i = 0; i < layouts.size(); ++i)
    {
        for (size_t j = 0; j < i && !built[i]; ++j)
            if (layouts[j] == layouts[i])
                built[i] = built[j];
        if (!built[i])
            built[i] = build(layouts[i], i < written_layouts ? whole_part : *materialized_part);
    }
    return {std::move(built), primary};
}

/// the granules to read from each part: all, or a sample past the budget
struct ScanPlan
{
    size_t sample_step = 1;
    std::vector<MarkRanges> ranges;
    /// the marks of a full scan
    UInt64 marks_to_scan = 0;
};

/// plans the scan, or returns false with the reason if a query limit forbids it
bool planScan(
    WhatIfCandidateResult & result,
    ScanPlan & plan,
    const RangesInDataParts & baseline_parts,
    bool filters_on_offsets,
    UInt64 projection_scan_budget_rows,
    const Settings & query_settings)
{
    UInt64 rows_to_scan = 0;
    for (const auto & part_with_ranges : baseline_parts)
    {
        rows_to_scan += part_with_ranges.data_part->rows_count;
        plan.marks_to_scan += part_with_ranges.data_part->index_granularity->getMarksCountWithoutFinal();
    }
    plan.sample_step
        = samplingStep(rows_to_scan, plan.marks_to_scan, projection_scan_budget_rows, query_settings[Setting::max_rows_to_read]);

    /// a sample's row offsets are not the part's, so an offset filter can't be applied to it
    if (plan.sample_step > 1 && filters_on_offsets)
    {
        result.empirical_unsupported_reason
            = "The query filters on part offsets, which an estimate from a sample of granules cannot follow "
              "(see projection_scan_budget_rows)";
        return false;
    }

    /// the granule floor and one granule per part can outgrow the budget, so check the read limit before reading
    plan.ranges.reserve(baseline_parts.size());
    UInt64 rows_planned = 0;
    for (const auto & part_with_ranges : baseline_parts)
    {
        plan.ranges.push_back(marksToScan(part_with_ranges.data_part, plan.sample_step));
        rows_planned += part_with_ranges.data_part->index_granularity->getRowsCountInRanges(plan.ranges.back());
    }
    if (const UInt64 read_limit = query_settings[Setting::max_rows_to_read]; read_limit != 0 && rows_planned > read_limit)
    {
        result.empirical_unsupported_reason = fmt::format(
            "The estimate would read {} rows, over max_rows_to_read = {} (a sample keeps at least ~{} granules and one per part)",
            rows_planned,
            read_limit,
            min_sampled_units);
        return false;
    }
    return true;
}

/// a sampled part, kept for the error estimate
struct SampledPart
{
    String name;
    ProjectionPartData sample;
    size_t part_rows = 0;
    MarkRanges ranges_read;
    DataPartPtr parent;
    /// the projection part in its likeliest layout
    MergeTreeDataPartPtr synthetic;
};

/// scenario 0 has every part in its likeliest layout, the next scenarios have every part in one layout each
/// a part has at most three layouts for each part format, both formats, and the layout of `MATERIALIZE PROJECTION`
struct Scenarios
{
    std::array<HypotheticalProjectionPtr, 1 + 3 * 2 + 1> scenarios;
    bool layouts_differ = false;
    /// the most layouts of a part
    size_t layouts = 1;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;
    UInt64 uneven_width_parts = 0;
    UInt64 merge_history_parts = 0;
    /// the parts with a granule size between their layouts, and the most marks of their layouts
    std::unordered_map<String, size_t> marks_between_layouts;
    std::vector<SampledPart> samples;
};

/// reads the planned granules and builds the projection parts, or returns false with the reason
bool buildScenarios(
    WhatIfCandidateResult & result,
    Scenarios & out,
    const ProjectionDescription & projection,
    const ScanPlan & plan,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings_ptr = data.getSettings(&projection.settings_changes);
    const auto & mt_settings = *mt_settings_ptr;
    const auto & query_settings = context->getSettingsRef();
    auto log = getLogger("WhatIfProjectionEstimator");

    /// check read limits by hand
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    for (auto & scenario : out.scenarios)
        scenario = std::make_shared<HypotheticalProjection>(projection.clone());

    for (size_t part_idx = 0; part_idx < baseline_parts.size(); ++part_idx)
    {
        const auto & part = baseline_parts[part_idx].data_part;
        if (part->index_granularity->getMarksCountWithoutFinal() == 0)
            continue;

        /// row bytes only matter when granules are sized by bytes
        const bool adaptive = data.canUseAdaptiveGranularity() && mt_settings[MergeTreeSetting::index_granularity_bytes] != 0;
        /// a merge sizes an aggregate projection part from its states, a level-zero part comes from an insert
        const bool merged_part = projection.type == ProjectionDescription::Type::Aggregate && part->info.level > 0;
        const bool constant_granularity = adaptive && mt_settings[MergeTreeSetting::use_const_adaptive_granularity];

        const MarkRanges & ranges = plan.ranges[part_idx];
        ProjectionPartData part_data;
        if (auto reason = buildProjectionPart(
                part_data, projection, part, ranges, plan.sample_step > 1, read_step, read_limits, adaptive, merged_part,
                constant_granularity, total_rows_read, total_bytes_read, context);
            !reason.empty())
        {
            result.empirical_unsupported_reason = std::move(reason);
            return false;
        }
        /// a `break` time limit or a cancelled query stops the read silently
        if (part_data.read_rows != part->index_granularity->getRowsCountInRanges(ranges))
        {
            auto reason = PreformattedMessage::create("The projection scan was cut short by a time limit in `break` mode or a cancelled query");
            result.empirical_unsupported_reason = reason.text;
            /// the limit also drops the output, so log the reason
            LOG_DEBUG(log, reason);
            return false;
        }

        ++out.scanned_parts;
        out.scanned_marks += ranges.getNumberOfMarks();
        /// with uneven row widths the layout depends on block boundaries we can't know
        /// a sample can't show that rows it didn't read have the same width
        bool uneven_rows = plan.sample_step > 1 && part_data.variable_width;
        if (!uneven_rows && part_data.row_bytes.size() == part_data.rows && !part_data.row_bytes.empty())
        {
            const auto [narrowest, widest] = std::minmax_element(part_data.row_bytes.begin(), part_data.row_bytes.end());
            uneven_rows = *narrowest != *widest;
        }
        if (uneven_rows)
            ++out.uneven_width_parts;
        out.merge_history_parts += part_data.merge_history_unknown || part_data.order_dependent_states;
        /// no row passes the filter of the projection, so the writer writes no projection part
        if (part_data.rows == 0)
        {
            /// a sample shows only that the granules it read have no such row
            if (part_data.read_rows < part->rows_count)
            {
                result.empirical_unsupported_reason = fmt::format(
                    "No row of the sample of part {} passes the filter of the projection, so the sample cannot show its projection "
                    "part (see projection_scan_budget_rows)",
                    part->name);
                return false;
            }
            continue;
        }

        /// a sample stands for the whole part, and the filter of the projection keeps the same share of its rows
        const size_t part_rows = plan.sample_step > 1
            ? static_cast<size_t>(UInt128(part->rows_count) * part_data.rows / part_data.read_rows)
            : part_data.rows;
        const auto [parts, primary]
            = buildSyntheticProjectionParts(part_data, projection, data, part, mt_settings, uneven_rows, part_rows);
        out.scenarios[0]->parts[part->name] = parts[primary];
        /// without a filter every layout reads all its marks, so the layouts bound the marks of a granule size between them
        if (part_data.marks_between_layouts != 0 && read_step->getFilterActionsDAG())
            out.marks_between_layouts[part->name] = part_data.marks_between_layouts;
        chassert(parts.size() < out.scenarios.size());
        out.layouts = std::max(out.layouts, parts.size());
        /// a part with one layout uses it in every scenario
        for (size_t layout = 0; layout + 1 < out.scenarios.size(); ++layout)
        {
            const auto & layout_part = parts[std::min(layout, parts.size() - 1)];
            out.scenarios[1 + layout]->parts[part->name] = layout_part;
            out.layouts_differ |= layout_part != parts[primary];
        }
        if (part_rows > part_data.rows)
            out.samples.push_back({part->name, std::move(part_data), part_rows, ranges, part, parts[primary]});
    }

    return true;
}

/// widens the mark range by the sampling error: range ends and the selected share
void widenForSamples(
    const std::vector<SampledPart> & samples, const HypotheticalProjection::Outcome & outcome, UInt64 & marks_low, UInt64 & marks_high)
{
    /// the estimate knows a range end only up to the gap between neighbouring sample rows
    /// that gap is a sampling step if the key follows the parent order
    std::vector<double> granule_shares;
    UInt64 layout_marks = 0;
    double range_end_spread = 0;
    for (const auto & sampled : samples)
    {
        const auto & sample = sampled.sample;
        const PartFromSample whole_part(sample, sampled.part_rows);
        const auto & granularity = *sampled.synthetic->index_granularity;
        const auto pruned_it = outcome.ranges.find(sampled.name);
        const MarkRanges pruned = pruned_it == outcome.ranges.end() ? MarkRanges{} : pruned_it->second;
        layout_marks += granularity.getMarksCount();

        /// the sampled granule each sample row came from, and the rows of each
        const auto & parent_granularity = *sampled.parent->index_granularity;
        std::vector<size_t> sampled_granule_rows;
        std::vector<UInt32> granule_of_row;
        granule_of_row.reserve(sample.rows);
        for (const auto & range : sampled.ranges_read)
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                const auto granule = static_cast<UInt32>(sampled_granule_rows.size());
                sampled_granule_rows.push_back(parent_granularity.getMarkRows(mark));
                const size_t kept_rows = sample.granule_rows.empty() ? sampled_granule_rows.back() : sample.granule_rows[granule];
                granule_of_row.resize(granule_of_row.size() + kept_rows, granule);
            }
        chassert(granule_of_row.size() == sample.rows);
        /// the projection keeps this share of the rows read
        const double kept_share = static_cast<double>(sample.rows) / static_cast<double>(sample.read_rows);

        size_t same_granule_neighbours = 0;
        for (size_t pos = 1; pos < sample.rows; ++pos)
            same_granule_neighbours += granule_of_row[sample.order[pos]] == granule_of_row[sample.order[pos - 1]];
        const double follows_parent
            = static_cast<double>(same_granule_neighbours) / static_cast<double>(std::max<size_t>(1, sample.rows - 1));
        const double sampling_step
            = static_cast<double>(parent_granularity.getMarksCountWithoutFinal()) / static_cast<double>(sampled_granule_rows.size());
        const double marks_per_sample_row = static_cast<double>(granularity.getMarksCount()) / static_cast<double>(sample.rows);
        const double granules_per_end = 1.0 + follows_parent * (sampling_step - 1.0) + (1.0 - follows_parent) * marks_per_sample_row;
        range_end_spread += 2.0 * static_cast<double>(std::max<size_t>(1, pruned.size())) * granules_per_end;

        std::vector<size_t> selected_rows(sampled_granule_rows.size());
        for (const auto & range : pruned)
        {
            const size_t sample_begin = whole_part.firstSampleRowFrom(granularity.getMarkStartingRow(range.begin));
            const size_t sample_end = whole_part.firstSampleRowFrom(granularity.getMarkStartingRow(range.end));
            for (size_t sample_row = sample_begin; sample_row < sample_end; ++sample_row)
                ++selected_rows[granule_of_row[sample.order[sample_row]]];
        }
        /// a share of the projection rows, as the marks of the projection part count them
        for (size_t i = 0; i < sampled_granule_rows.size(); ++i)
            granule_shares.push_back(static_cast<double>(selected_rows[i]) / (static_cast<double>(sampled_granule_rows[i]) * kept_share));
    }
    widen(marks_low, marks_high, range_end_spread);

    /// widen by two standard errors of the selected share (successive differences, fits a systematic sample)
    if (granule_shares.size() > 1)
        widen(marks_low, marks_high, 2.0 * standardErrorOfSample(granule_shares) * static_cast<double>(layout_marks));

}

/// the verdict and its reason from the scenario outcomes
void setVerdict(
    WhatIfCandidateResult & result,
    const HypotheticalProjection::Outcome & outcome,
    size_t chosen_in,
    size_t weighed,
    std::string_view relaxing_setting,
    UInt64 baseline_marks,
    const Scenarios & built)
{
    const UInt64 projection_marks = *outcome.marks;
    const UInt64 marks_low = result.estimated_marks_low;
    const UInt64 marks_high = result.estimated_marks_high;
    auto marks_text = [](UInt64 marks) { return fmt::format("{} mark{}", marks, marks == 1 ? "" : "s"); };
    const bool aggregated = built.scenarios[0]->projection.type == ProjectionDescription::Type::Aggregate;
    /// the rule against the base table: fewer marks, or the same marks and a served ORDER BY or aggregated rows
    auto beats_base = [&](UInt64 marks)
    { return marks < baseline_marks || (marks == baseline_marks && (outcome.serves_order || aggregated)); };
    if (chosen_in == weighed && outcome.forced)
    {
        String cost;
        if (outcome.nothing_to_serve)
            cost = "the query has no filter or ORDER BY for the projection to help with";
        else if (projection_marks > baseline_marks)
            cost = fmt::format("the projection reads {} instead of {} from the base table", marks_text(projection_marks), baseline_marks);
        else if (projection_marks == baseline_marks && !outcome.serves_order && !aggregated)
            cost = fmt::format("the projection reads the same {} as the base table and serves no ORDER BY", marks_text(projection_marks));
        else
            cost = fmt::format("the projection reads {} against {} from the base table", marks_text(projection_marks), baseline_marks);
        result.verdict = "chosen (forced)";
        result.verdict_reason = fmt::format("`{} = 1` overrides the cost; {}", relaxing_setting, cost);
    }
    else if (built.uneven_width_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but the rows are not known to have the same width on {} of the {} parts read, "
            "so the granule layout depends on the blocks the writer was fed and the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            built.uneven_width_parts,
            built.scanned_parts);
    }
    else if (built.merge_history_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but on {} of the {} parts read the writer sizes the granules from data that a part "
            "does not record (the order of the rows that built order-dependent aggregate states, the memory that a merge builds "
            "aggregate states in, or the bytes of the merged parts), so the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            built.merge_history_parts,
            built.scanned_parts);
    }
    else if ((chosen_in != 0 && chosen_in != weighed) || beats_base(marks_low) != beats_base(marks_high))
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, and a part does not record the layout that a merge or `MATERIALIZE PROJECTION` "
            "would leave, so the comparison comes out both ways over `marks_span`",
            marks_text(projection_marks),
            baseline_marks);
    }
    else
    {
        result.verdict = chosen_in != 0 ? "chosen" : "not chosen";
        if (projection_marks != baseline_marks)
            result.verdict_reason
                = fmt::format("{} would be read instead of {} from the base table", marks_text(projection_marks), baseline_marks);
        else if (aggregated)
            result.verdict_reason = fmt::format("the same {} would be read, of aggregated rows", marks_text(projection_marks));
        else
            result.verdict_reason = fmt::format(
                "the same {} would be read, and the projection order {}",
                marks_text(projection_marks),
                outcome.serves_order ? "serves the ORDER BY" : "serves no ORDER BY");
        /// another projection beats this one
        if (chosen_in == 0 && projection_marks < baseline_marks)
            result.verdict_reason = outcome.reason;
    }
}

bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    bool filters_on_offsets,
    std::string_view relaxing_setting,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    UInt64 projection_scan_budget_rows,
    const WeighHypotheticalProjection & weigh)
{
    /// the scan obeys the read limits of the scope that reads the table
    const auto context = read_step->getContext();
    ScanPlan plan;
    if (!planScan(result, plan, baseline_parts, filters_on_offsets, projection_scan_budget_rows, context->getSettingsRef()))
        return false;
    /// the groups of a sample do not scale to the groups of the part
    if (projection.type == ProjectionDescription::Type::Aggregate && plan.sample_step > 1)
    {
        result.empirical_unsupported_reason = "EXPLAIN WHATIF estimates an aggregate projection only from a full scan, because a sample "
                                              "cannot show the number of groups of a part (see projection_scan_budget_rows)";
        return false;
    }

    Stopwatch watch;
    Scenarios built;
    if (!buildScenarios(result, built, projection, plan, read_step, baseline_parts, context))
        return false;

    const size_t weighed = built.layouts_differ ? 1 + built.layouts : 1;
    for (size_t i = 0; i < weighed; ++i)
        weigh(built.scenarios[i]);

    const auto & outcome = built.scenarios[0]->outcome;
    result.sampled_parts = built.scanned_parts;
    result.sampled_marks = built.scanned_marks;
    /// out of what a full scan would read
    result.total_marks = plan.marks_to_scan;
    result.elapsed_us = watch.elapsedMicroseconds();
    if (!outcome.marks)
    {
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason = outcome.reason.empty() ? "The optimizer did not weigh the projection for this read" : outcome.reason;
        return true;
    }

    size_t chosen_in = 0;
    for (size_t i = 0; i < weighed; ++i)
        chosen_in += built.scenarios[i]->outcome.chosen;

    /// parts have their own histories, so each part takes the extremes of its own layouts
    UInt64 marks_low = 0;
    UInt64 marks_high = 0;
    for (const auto & [part_name, part] : built.scenarios[0]->parts)
    {
        UInt64 part_low = std::numeric_limits<UInt64>::max();
        UInt64 part_high = 0;
        const auto between = built.marks_between_layouts.find(part_name);
        for (size_t i = 0; i < weighed; ++i)
        {
            const auto & scenario_outcome = built.scenarios[i]->outcome;
            if (!scenario_outcome.marks)
                continue;
            const auto it = scenario_outcome.ranges.find(part_name);
            static const MarkRanges no_ranges;
            const MarkRanges & ranges = it == scenario_outcome.ranges.end() ? no_ranges : it->second;
            const UInt64 marks = ranges.getNumberOfMarks();
            part_low = std::min(part_low, marks);
            part_high = std::max(part_high, marks);
            /// a granule boundary of a size between the layouts can fall on the filtered keys
            if (between != built.marks_between_layouts.end())
            {
                const auto [fewest, most] = marksBetweenLayouts(ranges);
                part_low = std::min(part_low, fewest);
                part_high = std::max(part_high, std::min<UInt64>(most, between->second));
            }
        }
        marks_low += part_low == std::numeric_limits<UInt64>::max() ? 0 : part_low;
        marks_high += part_high;
    }
    /// the optimizer reads a part with no projection part from the base table, the same in every layout
    UInt64 projection_marks = 0;
    for (const auto & [_, ranges] : outcome.ranges)
        projection_marks += ranges.getNumberOfMarks();
    const UInt64 base_table_marks = *outcome.marks > projection_marks ? *outcome.marks - projection_marks : 0;
    marks_low += base_table_marks;
    marks_high += base_table_marks;
    widenForSamples(built.samples, outcome, marks_low, marks_high);

    result.estimated_marks = outcome.marks;
    result.estimated_rows = outcome.rows;
    result.estimated_marks_low = marks_low;
    result.estimated_marks_high = marks_high;
    setVerdict(result, outcome, chosen_in, weighed, relaxing_setting, baseline_marks, built);
    result.estimate_source = WhatIfCandidateResult::Empirical;
    result.empirical_status = WhatIfCandidateResult::Ok;
    return true;
}

}

std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason)
{
    if (!stored.required_columns.empty())
        context->checkAccess(
            AccessType::SELECT, data.getStorageID(), metadata->getColumns().getColumnNamesForSelectAccessCheck(stored.required_columns, context, data.getStorageID()));
    context->checkAccess(AccessType::ALTER_ADD_PROJECTION, data.getStorageID());

    std::optional<ProjectionDescription> fresh;
    try
    {
        checkHypotheticalProjectionIsAddable(data, metadata, stored.definition_ast, /* if_not_exists */ false, context);
        fresh = ProjectionDescription::getProjectionFromAST(
            stored.definition_ast, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);
    }
    catch (const Exception &)
    {
        reason = "Hypothetical projection can no longer be added to this table: " + getCurrentExceptionMessage(false);
        return std::nullopt;
    }

    /// an ALTER may retarget an ALIAS, so check the columns actually read
    if (!fresh->required_columns.empty())
        context->checkAccess(
            AccessType::SELECT, data.getStorageID(), metadata->getColumns().getColumnNamesForSelectAccessCheck(fresh->required_columns, context, data.getStorageID()));
    return fresh;
}

WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    const WeighHypotheticalProjection & weigh,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfCandidateResult result;
    result.kind = WhatIfCandidateResult::Projection;
    result.name = stored_projection.name;
    result.type = stored_projection.type == ProjectionDescription::Type::Aggregate ? "aggregate projection" : "normal projection";
    result.status = WhatIfCandidateResult::NotApplicable;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// before the refresh: a projection-served read carries that projection's metadata
    if (analysis.readFromProjection() && !baseline_parts.empty())
    {
        result.not_applicable_reason = "The query is already served from projection '" + baseline_parts.front().data_part->name
            + "', EXPLAIN WHATIF estimates candidates against the base table read only";
        return result;
    }

    auto metadata = read_step->getStorageMetadata();
    auto projection = refreshHypotheticalProjection(stored_projection, data, metadata, context, result.not_applicable_reason);
    if (!projection)
        return result;
    const bool aggregate = projection->type == ProjectionDescription::Type::Aggregate;

    /// Same as for indexes: building the projection reads every row, including the rows a row policy hides
    if (getEffectiveRowPolicyFilter(data, context))
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Cannot use `EXPLAIN WHATIF` because a row policy is applied on table {}. "
            "The projection estimate is derived from all rows, so it could violate the row policy",
            data.getStorageID().getNameForLogs());

    if (!context->getSettingsRef()[Setting::optimize_use_projections])
    {
        result.not_applicable_reason = "Projections are disabled by `optimize_use_projections = 0`";
        return result;
    }

    if (!projection->metadata->getSecondaryIndices().empty())
    {
        result.not_applicable_reason = "EXPLAIN WHATIF does not estimate projections with their own skip indexes yet";
        return result;
    }

    /// with no parts the optimizer finds no projection parts to read, whatever the settings
    if (baseline_parts.empty())
    {
        result.not_applicable_reason = "The query reads no parts, so the optimizer would not consider a projection";
        return result;
    }

    if (!QueryPlanOptimizations::canUseProjectionForReadingStep(read_step))
    {
        result.not_applicable_reason = "The optimizer does not consider projections for this read (for example FINAL, SAMPLE, "
                                       "reading in order, pending mutations, or a parallel-replicas mode without projection support)";
        return result;
    }

    /// the optimization of aggregate projections matches the aggregation of the query, not the columns that the query reads
    for (const auto & column_name : aggregate ? Names{} : read_step->getAllColumnNames())
    {
        if (!projection->sample_block.findColumnOrSubcolumnByName(column_name) && !projection->metadata->virtuals.has(column_name))
        {
            result.not_applicable_reason = fmt::format(
                "Projection does not contain column {} required by the query, so it could only filter the base table's "
                "parts, which EXPLAIN WHATIF does not estimate yet",
                backQuoteIfNeed(column_name));
            return result;
        }
    }

    const auto & proj_key = projection->metadata->getSortingKey();
    if (proj_key.column_names.empty())
    {
        result.not_applicable_reason = aggregate
            ? "Aggregate projection has no GROUP BY keys, which EXPLAIN WHATIF does not estimate yet"
            : "Projection has no sort key to prune on";
        return result;
    }

    /// commit-order keys and keys over columns the projection doesn't store can't be rebuilt
    for (const auto & required : proj_key.expression->getRequiredColumns())
    {
        if (required == BlockNumberColumn::name || required == BlockOffsetColumn::name)
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by the commit order ({}, {}), which EXPLAIN WHATIF does not estimate yet",
                backQuote(BlockNumberColumn::name),
                backQuote(BlockOffsetColumn::name));
            return result;
        }
        /// the key of an aggregate projection is over its own `GROUP BY` columns
        const auto & stored = aggregate ? projection->metadata : metadata;
        if (!stored->getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required))
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by {}, which it does not store, so EXPLAIN WHATIF cannot rebuild its key",
                backQuoteIfNeed(required));
            return result;
        }
    }

    /// the writer builds these only on merge, so fresh parts have none
    if (projection->with_block_number)
    {
        result.not_applicable_reason = fmt::format(
            "Projection stores {}, so it is built only when a part is merged, which EXPLAIN WHATIF does not estimate yet",
            backQuote(BlockNumberColumn::name));
        return result;
    }

    /// offset conditions as `ReadFromMergeTree` builds them, except for projections storing parent offsets
    bool filters_on_offsets = false;
    if (const auto & filter_dag = read_step->getFilterActionsDAG())
    {
        const auto predicate_dag = std::make_shared<ActionsDAGWithInversionPushDown>(
            filter_dag->getOutputs().front(), context, /* boolean_context */ true);
        const bool skip_folding = !context->getSettingsRef()[Setting::use_constant_folding_in_index_analysis];
        const auto & proj_columns = projection->metadata->getColumns();
        if (!projection->with_parent_part_offset && !proj_columns.has("_part_offset"))
        {
            const auto & proj_metadata = projection->metadata;
            filters_on_offsets
                = (!proj_columns.has("_part")
                   && MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(predicate_dag, proj_metadata, skip_folding, context))
                || (!proj_columns.has("_part_starting_offset")
                    && MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(predicate_dag, proj_metadata, skip_folding, context));
        }
    }

    /// read the setting from the context of the read, as the optimizer does
    const auto & read_settings = read_step->getContext()->getSettingsRef();
    const std::string_view relaxing_setting = read_settings[Setting::force_optimize_projection] ? "force_optimize_projection"
        : read_settings[Setting::prefer_optimize_projection] ? "prefer_optimize_projection" : "";

    /// with no parts, the optimizer still rejects a projection that the query cannot use, and WHATIF reads no data
    auto probe = std::make_shared<HypotheticalProjection>(projection->clone());
    weigh(probe);
    if (!probe->outcome.weighed)
    {
        result.not_applicable_reason = aggregate
            ? "The optimizer weighs an aggregate projection only for a query with GROUP BY or DISTINCT on this table"
            : "The optimizer does not weigh projections for this read";
        return result;
    }
    if (probe->outcome.rejected_for_query)
    {
        result.not_applicable_reason = probe->outcome.reason;
        return result;
    }

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        /// an exact count can remove parts from the base read, but the optimizer weighs an aggregate projection on all of them
        const auto & parts_to_scan = aggregate ? probe->outcome.parent_read : baseline_parts;
        if (tryEstimateProjection(
                result, *projection, filters_on_offsets, relaxing_setting, read_step, parts_to_scan, analysis.selected_marks,
                settings.projection_scan_budget_rows, weigh))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    return result;
}

}
