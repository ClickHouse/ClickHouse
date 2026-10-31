#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <Analyzer/TableNode.h>

#include <Common/Exception.h>
#include <Common/JSONBuilder.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>

#include <Core/Settings.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFilterInfo.h>
#include <Functions/FunctionTopKFilter.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <IO/Operators.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Sources/NullSource.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{

extern const SettingsBool enable_multiple_prewhere_read_steps;
extern const SettingsBool use_query_condition_cache;
extern const SettingsBool use_query_condition_cache_for_top_k;

}

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/// In-source filtering for the TopN threshold, the row-level security filter and PREWHERE.
/// The steps are applied to every stored block one after another, and each step reads its columns
/// only for the rows that passed the previous steps. A conjunction in PREWHERE is split into several
/// steps, like `MergeTreeSelectProcessor` does with `enable_multiple_prewhere_read_steps`, with the
/// conditions over the cheapest columns first. So for a table with `compress = true` a selective
/// condition only decompresses the columns it uses, and blocks where no row passes are skipped
/// without touching the columns of the later steps and the rest of the query.
struct MemorySourceFilter
{
    struct Step
    {
        ExpressionActionsPtr actions;
        String filter_column_name;
        /// A filter column that is kept holds its own values for the passing rows, as in
        /// `MergeTreeRangeReader`. It must not be replaced by a constant, like the header in
        /// `SourceStepWithFilter::applyPrewhereActions` is: for `PREWHERE k` it is the column `k`
        /// itself, which is not read again.
        bool remove_filter_column = false;
        /// The requested physical columns that the step's actions take as input.
        /// Those not produced by the previous steps are read before the step is executed.
        NamesAndTypesList input_columns;
    };

    std::vector<Step> steps;

    /// The columns of the output header that are requested physical columns, in header order.
    /// Those not produced by the steps are read after all steps, only for the passing rows.
    NamesAndTypesList output_columns;

    /// The query condition cache, or nullptr if it is not used. An entry describes one block of the
    /// snapshot, split into granules of `query_condition_cache_granule_rows` rows, and records the
    /// granules where no row satisfies a condition. The source skips such blocks and granules without
    /// evaluating the steps on them. There are two conditions:
    /// - PREWHERE, which the source evaluates itself. No row passing the steps is the same as no row
    ///   satisfying PREWHERE only if no other step (the row-level security filter, the TopN threshold)
    ///   has removed rows before it, so only then the source writes the entries.
    /// - The filter of the query pushed down to the read (`filter_actions_dag`), the same key as for
    ///   `MergeTree`. Its entries are written by the `FilterTransform` of the `WHERE` filter, from the
    ///   `MarkRangesInfo` of the chunks, which the source attaches when it has removed no rows itself.
    QueryConditionCachePtr query_condition_cache;
    UUID table_uuid;
    std::optional<UInt64> prewhere_condition_hash;
    String prewhere_condition;
    bool write_prewhere_condition = false;
    std::optional<UInt64> filter_condition_hash;
    bool attach_mark_ranges_info = false;
    /// The identity of the blocks of the snapshot, see `StorageMemory::BlocksWithCounts`.
    UInt64 generation = 0;
    UInt64 first_block_number = 0;
    /// The hash of the columns of the metadata snapshot. An `ALTER` of the columns keeps the stored
    /// blocks, but can change the values read from them (e.g. the array sizes of a `Nested` subcolumn
    /// missing in the blocks, which come from its stored siblings, or a cast to a changed type).
    UInt64 columns_hash = 0;

    static constexpr size_t query_condition_cache_granule_rows = 8192;

    String getQueryConditionCachePartName(size_t block_index) const
    {
        return fmt::format("{}_{:016x}_{}", generation, columns_hash, first_block_number + block_index);
    }
};

using MemorySourceFilterPtr = std::shared_ptr<const MemorySourceFilter>;

static constexpr auto global_row_index_column_name = "__global_row_index";

/// The number of the first row of each block in the snapshot, for the global row index of lazy materialization.
using BlockStartRows = std::vector<UInt64>;
using BlockStartRowsPtr = std::shared_ptr<const BlockStartRows>;

static BlockStartRowsPtr makeBlockStartRows(const Blocks & blocks)
{
    auto res = std::make_shared<BlockStartRows>();
    res->reserve(blocks.size());
    UInt64 num_rows = 0;
    for (const auto & block : blocks)
    {
        res->push_back(num_rows);
        num_rows += block.rows();
    }
    return res;
}

static ColumnPtr readColumnFromBlock(const Block & src, const NameAndTypePair & name_and_type)
{
    if (name_and_type.isSubcolumn())
        return tryGetSubcolumnFromBlock(src, name_and_type.getTypeInStorage(), name_and_type);
    return tryGetColumnFromBlock(src, name_and_type);
}

/// Only dotted names are members of a `Nested`, and the members of one `Nested` are checked to have equal sizes on insert.
static const ColumnWithTypeAndName * findSizesSource(const Block & src, const NameAndTypePair & column, const Names & nested_arrays)
{
    if (nested_arrays.empty() || !isArray(column.getTypeInStorage()))
        return nullptr;

    const String name_in_storage = column.getNameInStorage();
    const auto [nested_name, member_name] = Nested::splitName(name_in_storage);
    if (member_name.empty() || src.has(name_in_storage))
        return nullptr;

    for (const auto & array_name : nested_arrays)
    {
        if (Nested::splitName(std::string_view(array_name)).first != nested_name)
            continue;

        const auto * stored = src.findByName(array_name);
        if (stored && stored->column && isArray(stored->type))
            return stored;
    }

    return nullptr;
}

/// Each missing entry is filled from `src` alone, independently of the other columns of the read.
/// `nested_arrays` are the arrays of the table that are members of the `Nested` of a requested member.
static void fillColumnsMissingFromBlock(const Block & src, const NamesAndTypesList & columns_to_read, Columns & columns, const Names & nested_arrays)
{
    NamesAndTypesList columns_without_sizes;
    std::vector<size_t> positions_without_sizes;
    std::unordered_map<const ColumnWithTypeAndName *, ColumnPtr> offsets_of_sources;

    auto column_it = columns_to_read.begin();
    for (size_t i = 0; i < columns.size(); ++i, ++column_it)
    {
        if (columns[i])
            continue;

        ColumnPtr offsets_column;
        if (const auto * source = findSizesSource(src, *column_it, nested_arrays))
        {
            auto & source_offsets = offsets_of_sources[source];
            if (!source_offsets)
                source_offsets = tryDecompressArrayOffsets(*source->type, *source->column);
            if (!source_offsets)
            {
                auto decompressed = source->column->decompress();
                if (const auto * source_array = typeid_cast<const ColumnArray *>(decompressed.get()))
                    source_offsets = source_array->getOffsetsPtr();
            }
            offsets_column = source_offsets;
        }

        if (!offsets_column)
        {
            columns_without_sizes.push_back(*column_it);
            positions_without_sizes.push_back(i);
            continue;
        }

        /// Only the outer sizes: the deeper levels of a member are its own.
        const auto & type_in_storage = column_it->getTypeInStorage();
        const auto & offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData();
        auto elements = assert_cast<const DataTypeArray &>(*type_in_storage).getNestedType()
            ->createColumnConstWithDefaultValue(offsets.empty() ? 0 : offsets.back())->convertToFullColumnIfConst();
        ColumnPtr column = ColumnArray::create(elements, offsets_column);
        columns[i] = column_it->isSubcolumn() ? type_in_storage->getSubcolumn(column_it->getSubcolumnName(), column) : column;
    }

    if (!positions_without_sizes.empty())
    {
        Columns filled(positions_without_sizes.size());
        fillMissingColumns(filled, src.rows(), columns_without_sizes, columns_without_sizes, {}, nullptr);
        for (size_t j = 0; j < positions_without_sizes.size(); ++j)
            columns[positions_without_sizes[j]] = std::move(filled[j]);
    }
}

/// A column that is read together with some of its subcolumns is decompressed once, and the subcolumns are taken from it.
static Columns readColumnsFromBlock(const Block & src, const NamesAndTypesList & columns_to_read, const Names & nested_arrays)
{
    std::unordered_set<String> parents_of_subcolumns;
    for (const auto & column : columns_to_read)
    {
        if (column.isSubcolumn())
            parents_of_subcolumns.insert(column.getNameInStorage());
    }

    std::optional<Block> decompressed;
    for (const auto & column : columns_to_read)
    {
        if (column.isSubcolumn() || !parents_of_subcolumns.contains(column.name))
            continue;

        const auto * elem = src.findByName(column.name);
        if (!elem || !elem->column)
            continue;

        auto decompressed_column = elem->column->decompress();
        if (decompressed_column == elem->column)
            continue;

        if (!decompressed)
            decompressed = src;
        decompressed->getByName(column.name).column = std::move(decompressed_column);
    }

    const Block & block = decompressed ? *decompressed : src;

    Columns columns;
    columns.reserve(columns_to_read.size());
    for (const auto & name_and_type : columns_to_read)
        columns.emplace_back(readColumnFromBlock(block, name_and_type));

    fillColumnsMissingFromBlock(block, columns_to_read, columns, nested_arrays);
    return columns;
}

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;

    static Block getHeader(const NamesAndTypesList & physical, const NamesAndTypesList & virtuals, bool with_global_row_index)
    {
        Block res;
        for (const auto & name_type : physical)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        for (const auto & name_type : virtuals)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        if (with_global_row_index)
            res.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), global_row_index_column_name});
        return res;
    }

public:
    MemorySource(
        NamesAndTypesList physical_columns_,
        NamesAndTypesList virtual_columns_,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {},
        MaterializedCTEPtr materialized_cte_ = {},
        MemorySourceFilterPtr filter_ = {},
        SharedHeader filtered_header_ = {},
        BlockStartRowsPtr block_start_rows_ = {},
        Names nested_arrays_ = {})
        : ISource(filter_ ? filtered_header_ : std::make_shared<const Block>(getHeader(physical_columns_, virtual_columns_, block_start_rows_ != nullptr)))
        , physical_columns(std::move(physical_columns_))
        , virtual_columns(std::move(virtual_columns_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
        , materialized_cte(std::move(materialized_cte_))
        , filter(std::move(filter_))
        , block_start_rows(std::move(block_start_rows_))
        , nested_arrays(std::move(nested_arrays_))
    {
        if (filter && !virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_columns.getNames());
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            if (materialized_cte)
            {
                /// Fail-fast invariant: by the time `MemorySource::generate`
                /// runs, `DelayedPortsProcessor` (inserted by
                /// `MaterializingCTEsStep::updatePipeline` via
                /// `addPipelineBefore`) has already gated this reader on the
                /// corresponding `MaterializingCTETransform` finishing. If we
                /// observe `is_built == false` here, the planner failed to
                /// wire the gate - fail loudly rather than read from a
                /// half-populated `StorageMemory`.
                if (!materialized_cte->is_built.load(std::memory_order_acquire))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Reading from materialized CTE '{}' before its materialization completed - "
                        "DelayedPortsProcessor gate is missing in the query plan",
                        materialized_cte->cte_name);
            }

            initializer_func(data);
            initializer_func = {};
        }

        while (true)
        {
            size_t current_index = getAndIncrementExecutionIndex();

            if (!data || current_index >= data->size())
                return {};

            const Block & src = (*data)[current_index];

            if (filter)
            {
                if (auto chunk = generateFiltered(src, current_index))
                    return std::move(*chunk);

                /// Every row of this block was filtered out; move on to the next block
                /// without reading the rest of its columns. `generateFiltered` has already
                /// reported the block's scanned rows as read.
                if (isCancelled())
                    return {};
                continue;
            }

            Columns columns;
            fillPhysicalColumns(src, columns);

            UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
            if (!columns.empty())
                fillVirtualColumns(columns, num_rows);

            if (block_start_rows)
            {
                num_rows = src.rows();
                auto global_row_index = ColumnUInt64::create(num_rows);
                std::iota(global_row_index->getData().begin(), global_row_index->getData().end(), (*block_start_rows)[current_index]);
                columns.push_back(std::move(global_row_index));
            }

            return Chunk(std::move(columns), num_rows);
        }
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }

        return execution_index++;
    }

    void fillPhysicalColumns(const Block & src, Columns & result_columns) const
    {
        result_columns = readColumnsFromBlock(src, physical_columns, nested_arrays);
        chassert(std::all_of(result_columns.begin(), result_columns.end(), [](const auto & column) { return column != nullptr; }));
    }

    /// Applies the filter steps (row-level security filter, PREWHERE) to one stored block.
    /// Returns std::nullopt when no row passes.
    ///
    /// Reports the read progress explicitly, because the automatic accounting of `ISource` uses
    /// the returned chunk, which here holds only the rows that passed the filter - and nothing at
    /// all for a block that the filter eliminated completely. The reported number of rows is the
    /// number of rows scanned, the same as what `ReadFromMergeTree` reports for its `PREWHERE`,
    /// so `max_rows_to_read`, read quotas and `SelectedRows` still see the whole scan. The granules
    /// skipped by the query condition cache are not scanned and not reported, as with `MergeTree`,
    /// so that the cache cannot make `max_rows_to_read` with `read_overflow_mode = 'break'` stop
    /// the read earlier.
    ///
    /// The layout of the block after the steps depends on how the steps were split, so the result
    /// is assembled by name in the order of the output header, as in `MergeTreeSelectProcessor`.
    std::optional<Chunk> generateFiltered(const Block & src, size_t block_index)
    {
        const size_t num_src_rows = src.rows();

        /// The size of the columns materialized from this block, accumulated as they are read.
        size_t num_read_bytes = 0;

        Block block;
        size_t num_rows = num_src_rows;

        /// The number of rows of the block that are scanned: all of them except the granules skipped
        /// by the query condition cache.
        size_t num_scanned_rows = num_src_rows;

        /// Mask over the stored block's rows combining all steps so far, for cutting the columns
        /// read after some step has filtered. Empty while no step has filtered anything.
        IColumn::Filter combined_mask;

        const size_t granule_rows = MemorySourceFilter::query_condition_cache_granule_rows;
        const size_t num_granules = (num_src_rows + granule_rows - 1) / granule_rows;
        const bool use_query_condition_cache = filter->query_condition_cache && num_granules;
        String query_condition_cache_part_name;

        /// The granules skipped by the entry of the filter of the query. Nothing is known about PREWHERE
        /// on them, so they must not be recorded as having no rows satisfying it.
        QueryConditionCache::MatchingMarks granules_skipped_by_filter_condition;

        if (use_query_condition_cache)
        {
            query_condition_cache_part_name = filter->getQueryConditionCachePartName(block_index);

            /// The granules that may have rows satisfying both conditions, or std::nullopt if nothing is known.
            std::optional<QueryConditionCache::MatchingMarks> matching_granules;
            for (const auto & condition_hash : {filter->prewhere_condition_hash, filter->filter_condition_hash})
            {
                if (!condition_hash)
                    continue;

                auto entry = filter->query_condition_cache->read(filter->table_uuid, query_condition_cache_part_name, *condition_hash);
                if (!entry)
                    continue;

                if (entry->size() != num_granules)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "The query condition cache entry for block {} of a Memory table has {} granules instead of {}",
                        query_condition_cache_part_name, entry->size(), num_granules);

                if (condition_hash == filter->filter_condition_hash && filter->write_prewhere_condition)
                {
                    granules_skipped_by_filter_condition.resize(num_granules);
                    for (size_t granule = 0; granule < num_granules; ++granule)
                        granules_skipped_by_filter_condition[granule] = !(*entry)[granule];
                }

                if (!matching_granules)
                    matching_granules = std::move(entry);
                else
                    for (size_t granule = 0; granule < num_granules; ++granule)
                        (*matching_granules)[granule] = (*matching_granules)[granule] && (*entry)[granule];
            }

            if (matching_granules)
            {
                const size_t num_matching_granules = std::ranges::count(*matching_granules, true);
                if (num_matching_granules == 0)
                    return std::nullopt;

                /// Start with the rows of the granules that may have matches, as if a step had filtered the others out.
                if (num_matching_granules != num_granules)
                {
                    combined_mask.resize_fill(num_src_rows, 0);
                    num_rows = 0;
                    for (size_t granule = 0; granule < num_granules; ++granule)
                    {
                        if (!(*matching_granules)[granule])
                            continue;
                        const size_t begin = granule * granule_rows;
                        const size_t end = std::min(begin + granule_rows, num_src_rows);
                        std::fill(combined_mask.begin() + begin, combined_mask.begin() + end, 1);
                        num_rows += end - begin;
                    }
                    num_scanned_rows = num_rows;
                }
            }
        }

        /// Records the granules where no row has passed the steps.
        auto write_to_query_condition_cache = [&](bool no_rows_passed)
        {
            if (!use_query_condition_cache || !filter->write_prewhere_condition)
                return;

            if (!no_rows_passed && combined_mask.empty())
                return;

            MarkRanges granules_without_matches;
            for (size_t granule = 0; granule < num_granules; ++granule)
            {
                if (!granules_skipped_by_filter_condition.empty() && granules_skipped_by_filter_condition[granule])
                    continue;
                if (!no_rows_passed)
                {
                    const size_t begin = granule * granule_rows;
                    const size_t end = std::min(begin + granule_rows, num_src_rows);
                    /// The mask can hold any non-zero value for a passing row: it is taken from the filter column
                    /// as is, which is e.g. the column itself for `PREWHERE k`.
                    if (!memoryIsZero(combined_mask.data(), begin, end))
                        continue;
                }
                if (!granules_without_matches.empty() && granules_without_matches.back().end == granule)
                    ++granules_without_matches.back().end;
                else
                    granules_without_matches.emplace_back(granule, granule + 1);
            }

            if (!granules_without_matches.empty())
                filter->query_condition_cache->write(
                    filter->table_uuid, query_condition_cache_part_name, *filter->prewhere_condition_hash, filter->prewhere_condition,
                    granules_without_matches, num_granules, /*has_final_mark=*/ false);
        };

        /// Reads those of the columns that the block does not have yet, only for the rows that
        /// passed the steps so far.
        auto read_missing_columns = [&](const NamesAndTypesList & columns_to_read)
        {
            NamesAndTypesList missing;
            for (const auto & name_and_type : columns_to_read)
                if (!block.has(name_and_type.name))
                    missing.push_back(name_and_type);

            if (missing.empty())
                return;

            Columns columns = readColumnsFromBlock(src, missing, nested_arrays);

            auto column_it = columns.begin();
            for (const auto & name_and_type : missing)
            {
                ColumnPtr column = std::move(*column_it);
                ++column_it;
                num_read_bytes += column->byteSize();
                if (!combined_mask.empty())
                    column = column->filter(combined_mask, num_rows);
                block.insert({column, name_and_type.type, name_and_type.name});
            }
        };

        for (const auto & step : filter->steps)
        {
            read_missing_columns(step.input_columns);
            step.actions->execute(block, num_rows);

            const size_t filter_column_position = block.getPositionByName(step.filter_column_name);
            ColumnPtr filter_column = block.getByPosition(filter_column_position).column;

            ConstantFilterDescription constant_filter(*filter_column);
            if (constant_filter.always_false)
            {
                write_to_query_condition_cache(/*no_rows_passed=*/ true);
                progress(num_scanned_rows, num_read_bytes);
                return std::nullopt;
            }

            if (!constant_filter.always_true)
            {
                FilterDescription filter_description(*filter_column);
                const size_t num_passed_rows = filter_description.countBytesInFilter();
                if (num_passed_rows == 0)
                {
                    write_to_query_condition_cache(/*no_rows_passed=*/ true);
                    progress(num_scanned_rows, num_read_bytes);
                    return std::nullopt;
                }

                if (num_passed_rows != num_rows)
                {
                    for (auto & elem : block)
                        elem.column = filter_description.filter(*elem.column, num_passed_rows);

                    if (combined_mask.empty())
                    {
                        combined_mask.assign(*filter_description.data);
                    }
                    else
                    {
                        /// This step's mask indexes the rows that passed the previous steps.
                        size_t pos = 0;
                        for (auto & passed : combined_mask)
                            if (passed)
                                passed = (*filter_description.data)[pos++];
                        chassert(pos == filter_description.data->size());
                    }
                }

                num_rows = num_passed_rows;
            }

            if (step.remove_filter_column)
                block.erase(filter_column_position);
        }

        write_to_query_condition_cache(/*no_rows_passed=*/ false);

        read_missing_columns(filter->output_columns);

        if (block_start_rows)
        {
            const UInt64 block_start_row = (*block_start_rows)[block_index];
            auto global_row_index = ColumnUInt64::create();
            auto & global_row_index_data = global_row_index->getData();
            global_row_index_data.reserve(num_rows);
            for (size_t row = 0; row < num_src_rows; ++row)
                if (combined_mask.empty() || combined_mask[row])
                    global_row_index_data.push_back(block_start_row + row);
            chassert(global_row_index_data.size() == num_rows);
            block.insert({std::move(global_row_index), std::make_shared<DataTypeUInt64>(), global_row_index_column_name});
        }

        progress(num_scanned_rows, num_read_bytes);

        const auto & header = getPort().getHeader();
        Columns result_columns;
        result_columns.reserve(header.columns());
        for (const auto & elem : header)
            result_columns.push_back(block.getByName(elem.name).column);

        Chunk chunk(std::move(result_columns), num_rows);

        if (use_query_condition_cache && filter->attach_mark_ranges_info)
        {
            /// Without the granules removed above, which are already known to have no matches, the chunk
            /// does not hold all rows of the block, so only the whole block can be recorded.
            auto mark_ranges_info = std::make_shared<MarkRangesInfo>(
                filter->table_uuid, query_condition_cache_part_name, num_granules, /*has_final_mark=*/ false,
                MarkRanges{MarkRange(0, num_granules)});
            if (combined_mask.empty())
                mark_ranges_info->rows_per_mark = granule_rows;
            chunk.getChunkInfos().add(std::move(mark_ranges_info));
        }

        return chunk;
    }

    void fillVirtualColumns([[maybe_unused]] Columns & result_columns, [[maybe_unused]] UInt64 num_rows) const
    {
        if (!virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_columns.getNames());
    }

    const NamesAndTypesList physical_columns;
    const NamesAndTypesList virtual_columns;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
    MaterializedCTEPtr materialized_cte;
    MemorySourceFilterPtr filter;
    /// Set if the source produces the `__global_row_index` column for lazy materialization.
    BlockStartRowsPtr block_start_rows;
    /// The arrays of the table that are members of the `Nested` of a requested member; see `findSizesSource`.
    const Names nested_arrays;
};

/// The rows of one block of the snapshot to read in the lazy branch of lazy materialization.
struct MemoryLazyBlockRows
{
    size_t block_index = 0;
    /// `ColumnUInt64` with the numbers of the rows in the block, in ascending order.
    ColumnPtr rows_in_block;
};

/// Reads the columns deferred by lazy materialization for the given rows of a range of blocks
/// and returns them as a single chunk.
class LazyReadFromMemoryBlocksSource final : public ISource
{
public:
    LazyReadFromMemoryBlocksSource(
        SharedHeader header,
        NamesAndTypesList columns_,
        std::shared_ptr<const Blocks> data_,
        std::vector<MemoryLazyBlockRows> block_rows_,
        Names nested_arrays_)
        : ISource(std::move(header))
        , columns(std::move(columns_))
        , data(std::move(data_))
        , block_rows(std::move(block_rows_))
        , nested_arrays(std::move(nested_arrays_))
    {
    }

    String getName() const override { return "LazyReadFromMemoryBlocks"; }

    void cancel(CancelReason reason) noexcept override
    {
        /// A partial result must still get the deferred columns of the rows already selected by the main branch.
        if (reason == CancelReason::PartialResult)
            return;

        ISource::cancel(reason);
    }
    using ISource::cancel;

protected:
    Chunk generate() override
    {
        if (block_rows.empty())
            return {};

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();
        size_t num_rows = 0;
        /// The size of the whole deferred columns materialized from the blocks.
        size_t num_read_bytes = 0;

        for (const auto & [block_index, rows_in_block] : block_rows)
        {
            /// The range can span many blocks, so stop rereading them as soon as the query is cancelled.
            if (isCancelled())
                return {};

            const Block & src = (*data)[block_index];

            Columns block_columns = readColumnsFromBlock(src, columns, nested_arrays);

            for (size_t i = 0; i < block_columns.size(); ++i)
            {
                num_read_bytes += block_columns[i]->byteSize();
                result_columns[i]->insertRangeFrom(*block_columns[i]->index(*rows_in_block, 0), 0, rows_in_block->size());
            }

            num_rows += rows_in_block->size();
        }

        block_rows.clear();

        /// Reports the read progress explicitly, because the automatic accounting of `ISource` uses
        /// the returned chunk, which holds only the requested rows, while the deferred columns of each
        /// block are materialized as a whole (and decompressed for a table with `compress = true`).
        /// The rows are already counted by the main branch, so only the requested rows are reported here,
        /// the same as the automatic accounting would do.
        progress(num_rows, num_read_bytes);

        return Chunk(std::move(result_columns), num_rows);
    }

private:
    const NamesAndTypesList columns;
    std::shared_ptr<const Blocks> data;
    std::vector<MemoryLazyBlockRows> block_rows;
    /// The same as in `MemorySource`, so that a missing member of a `Nested` gets the same sizes in both branches.
    const Names nested_arrays;
};

/// The source of the lazy branch of lazy materialization for the `Memory` storage.
/// The rows to read become known only at run time, after the main branch of the query (with the `LIMIT`)
/// is fully executed. Until then, `prepare` reports `UpdatePipeline`: the executor calls `updatePipeline`
/// when the downstream `LazyMaterializingTransform` starts pulling from this processor, which happens
/// strictly after it has filled `MemoryLazyMaterializingRows`.
///
/// The rows usually come from different blocks, and for a table with `compress = true` every block
/// decompresses each deferred column as a whole, so the blocks are read in parallel: by several
/// `LazyReadFromMemoryBlocksSource`, each for a contiguous range of them, whose chunks are passed on
/// in the order of the ranges, which is the order of the global row index.
class LazyReadFromMemorySource final : public IProcessor
{
public:
    LazyReadFromMemorySource(
        SharedHeader header,
        NamesAndTypesList columns_,
        std::shared_ptr<const Blocks> data_,
        MemoryLazyMaterializingRowsPtr lazy_materializing_rows_,
        size_t num_streams_,
        Names nested_arrays_)
        : IProcessor({}, {std::move(header)})
        , columns(std::move(columns_))
        , data(std::move(data_))
        , lazy_materializing_rows(std::move(lazy_materializing_rows_))
        , num_streams(num_streams_)
        , nested_arrays(std::move(nested_arrays_))
    {
    }

    String getName() const override { return "LazyReadFromMemory"; }

    Status prepare() override
    {
        auto & output = outputs.front();
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (lazy_materializing_rows)
            return Status::UpdatePipeline;

        for (; current_input != inputs.end(); ++current_input)
        {
            if (current_input->isFinished())
                continue;

            if (!current_input->hasData())
                return Status::NeedData;

            output.push(current_input->pull());
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    PipelineUpdate updatePipeline() override
    {
        const auto rows = std::move(lazy_materializing_rows);
        lazy_materializing_rows.reset();

        std::vector<MemoryLazyBlockRows> block_rows = groupRowsByBlock(rows->rows);

        Processors sources;
        const size_t num_sources = std::min(std::max<size_t>(num_streams, 1), block_rows.size());
        for (size_t i = 0; i < num_sources; ++i)
        {
            std::vector<MemoryLazyBlockRows> source_block_rows(
                std::make_move_iterator(block_rows.begin() + i * block_rows.size() / num_sources),
                std::make_move_iterator(block_rows.begin() + (i + 1) * block_rows.size() / num_sources));

            auto source = std::make_shared<LazyReadFromMemoryBlocksSource>(
                outputs.front().getSharedHeader(), columns, data, std::move(source_block_rows), nested_arrays);

            auto & source_output = source->getOutputs().front();
            inputs.emplace_back(source_output.getHeader(), this);
            connect(source_output, inputs.back());
            /// All at once, so that the sources run in parallel.
            inputs.back().setNeeded();
            sources.push_back(std::move(source));
        }

        current_input = inputs.begin();
        return PipelineUpdate{.to_add = std::move(sources), .to_remove = {}};
    }

private:
    std::vector<MemoryLazyBlockRows> groupRowsByBlock(const PaddedPODArray<UInt64> & rows) const
    {
        std::vector<MemoryLazyBlockRows> res;
        if (rows.empty())
            return res;

        const auto block_start_rows = makeBlockStartRows(*data);
        size_t next_row = 0;
        while (next_row < rows.size())
        {
            /// The block of the row is the last one that starts not after it.
            const UInt64 row = rows[next_row];
            const auto it = std::upper_bound(block_start_rows->begin(), block_start_rows->end(), row);
            if (it == block_start_rows->begin())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected global row index {} in an empty Memory table", row);

            const size_t block_index = it - block_start_rows->begin() - 1;
            const UInt64 block_start_row = (*block_start_rows)[block_index];
            const UInt64 block_end_row = block_start_row + (*data)[block_index].rows();
            if (row >= block_end_row)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Global row index {} is out of range of the Memory table snapshot with {} rows", row, block_end_row);

            auto rows_in_block = ColumnUInt64::create();
            auto & rows_in_block_data = rows_in_block->getData();
            for (; next_row < rows.size() && rows[next_row] < block_end_row; ++next_row)
                rows_in_block_data.push_back(rows[next_row] - block_start_row);

            res.push_back({block_index, std::move(rows_in_block)});
        }

        return res;
    }

    const NamesAndTypesList columns;
    std::shared_ptr<const Blocks> data;
    MemoryLazyMaterializingRowsPtr lazy_materializing_rows;
    const size_t num_streams;
    const Names nested_arrays;
    InputPorts::iterator current_input = inputs.end();
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    const size_t num_streams_,
    const bool delay_read_for_global_sub_queries_)
    : SourceStepWithFilter(
        /// `query_info` may already carry PREWHERE (an explicit `PREWHERE` clause) and a pushed-down
        /// row-level security filter; they are applied inside the source, so the output header must
        /// reflect them. This is a no-op when both are absent.
        std::make_shared<const Block>(SourceStepWithFilter::applyPrewhereActions(
            storage_snapshot_->getSampleBlockForColumns(columns_to_read_),
            query_info_.row_level_filter,
            query_info_.prewhere_info)),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , columns_to_read(columns_to_read_)
    , storage(std::move(storage_))
    , num_streams(num_streams_)
    , delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromMemoryStorageStep::clone() const
{
    return std::make_unique<ReadFromMemoryStorageStep>(*this);
}

void ReadFromMemoryStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    /// The row-level filter and `PREWHERE` are evaluated inside the source, which starts running
    /// as soon as the pipeline is executed. A condition such as `PREWHERE k IN (SELECT ...)` carries
    /// a `FutureSet` that the pipeline-level `CreatingSetsStep` fills in; relying on it is not
    /// enough, because `DelayedPortsProcessor` can be short-circuited by a downstream processor that
    /// closes its inputs early, and the source would then see a not-ready set. Build the sets in
    /// place, the same way `ReadFromMergeTree` does for its storage-level `PREWHERE`.
    /// This has to happen here, during plan optimization: at the end of `QueryPlan::optimize`,
    /// `DelayedCreatingSetsStep` takes the subquery plans out of the sets, and an in-place build
    /// from `initializePipeline` would find nothing to execute.
    /// Sets of `GLOBAL IN` in an explicit `PREWHERE` or a row policy are built here too; only the
    /// optimizer-moved condition in `updatePrewhereInfo` has to leave them out.
    if (query_info.row_level_filter)
        VirtualColumnUtils::buildSetsForDAG(query_info.row_level_filter->actions, context);
    if (query_info.prewhere_info)
        VirtualColumnUtils::buildSetsForDAG(query_info.prewhere_info->prewhere_actions, context);

    filters_applied = true;
}

void ReadFromMemoryStorageStep::rebuildOutputHeader()
{
    Block header = SourceStepWithFilter::applyPrewhereActions(
        storage_snapshot->getSampleBlockForColumns(required_source_columns),
        query_info.row_level_filter,
        query_info.prewhere_info);

    if (read_global_row_index)
        header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), global_row_index_column_name});

    output_header = std::make_shared<const Block>(std::move(header));
}

void ReadFromMemoryStorageStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    SourceStepWithFilter::updatePrewhereInfo(prewhere_info_value);
    rebuildOutputHeader();

    /// `optimizePrewhere` runs after `applyFilters`, so a condition with `IN (subquery)` that it moves
    /// into `PREWHERE` still needs its set built in place, for the reason given in `applyFilters`.
    /// Only when `applyFilters` did run for this step: otherwise the sets are left to the
    /// `CreatingSetsStep` of the plan, and building one here would execute the subquery twice.
    /// Sets of `GLOBAL IN` are excluded: `ReadFromRemote` may still have to attach an external table
    /// to them, which fails on a set that is already built.
    if (filters_applied && query_info.prewhere_info)
        VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(query_info.prewhere_info->prewhere_actions, context);
}

bool ReadFromMemoryStorageStep::supportsTopKDynamicFilter(const ColumnWithTypeAndName & sort_column) const
{
    /// Virtual columns do not qualify.
    if (std::ranges::find(columns_to_read, sort_column.name) == columns_to_read.end())
        return false;

    const auto column = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(), sort_column.name);
    if (!column || !column->type->equals(*sort_column.type))
        return false;

    /// The threshold is computed from the output column, while the filter reads the physical column
    /// of the same name. They are the same only if the in-source filters pass it through unchanged.
    auto is_shadowed = [&](const ActionsDAG & dag)
    {
        const auto * node = dag.tryFindInOutputs(sort_column.name);
        return node && node->type != ActionsDAG::ActionType::INPUT;
    };
    if (query_info.row_level_filter && is_shadowed(query_info.row_level_filter->actions))
        return false;
    if (query_info.prewhere_info && is_shadowed(query_info.prewhere_info->prewhere_actions))
        return false;

    /// The source fills a column that a block does not have (e.g. one added by `ALTER TABLE ADD COLUMN`
    /// after the block was inserted) with the defaults of the type. For a column without a default
    /// expression these are exactly the values the query sees. For a column with a `DEFAULT` expression
    /// the values agree only as long as nothing evaluates the expression above the source; do not rely
    /// on that and exclude such columns and their subcolumns, the same way
    /// `StorageMemory::supportedPrewhereColumns` does for `PREWHERE`.
    return !storage_snapshot->metadata->getColumns().hasDefault(column->getNameInStorage());
}

void ReadFromMemoryStorageStep::setTopKFilter(FormatTopKFilterInfoPtr info)
{
    top_k_filter = std::move(info);
}

void ReadFromMemoryStorageStep::describeActions(FormatSettings & format_settings) const
{
    SourceStepWithFilter::describeActions(format_settings);
    if (top_k_filter)
        format_settings.out << format_settings.detail_prefix << "TopN filter column: " << top_k_filter->column_name << '\n';
}

void ReadFromMemoryStorageStep::describeActions(JSONBuilder::JSONMap & map) const
{
    SourceStepWithFilter::describeActions(map);
    if (top_k_filter)
        map.add("TopN Filter Column", top_k_filter->column_name);
}

std::optional<UInt64> ReadFromMemoryStorageStep::getFilterConditionHashForQueryConditionCache() const
{
    if (!filter_actions_dag || !context->getSettingsRef()[Setting::use_query_condition_cache])
        return {};

    /// Same as for `MergeTree`, see `updateQueryConditionCache`.
    const auto & outputs = filter_actions_dag->getOutputs();
    if (outputs.size() != 1 || !VirtualColumnUtils::isDeterministic(outputs[0]))
        return {};

    return queryConditionCacheHash(outputs[0]->getHash(), queryConditionCacheSettingsSalt(context->getSettingsRef()));
}

bool ReadFromMemoryStorageStep::canUseLazyMaterialization() const
{
    /// A read for a global subquery takes the blocks of the storage when it starts, and the lazy branch
    /// could not see the same ones.
    return !delay_read_for_global_sub_queries && !read_global_row_index;
}

std::unique_ptr<LazilyReadFromMemoryStorage> ReadFromMemoryStorageStep::keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names)
{
    if (!canUseLazyMaterialization())
        return {};

    /// The in-source filters run in the main branch. A column they output as a computed node may shadow
    /// the physical column of the same name, and the lazy branch would read the physical one instead,
    /// so such columns also stay in the main branch.
    NameSet names_to_keep = required_names;
    auto keep_columns_of_filter = [&](const ActionsDAG & dag)
    {
        for (const auto & column : dag.getRequiredColumns())
            names_to_keep.insert(column.name);
        for (const auto * output : dag.getOutputs())
            if (output->type != ActionsDAG::ActionType::INPUT)
                names_to_keep.insert(output->result_name);
    };
    if (query_info.row_level_filter)
        keep_columns_of_filter(query_info.row_level_filter->actions);
    if (query_info.prewhere_info)
        keep_columns_of_filter(query_info.prewhere_info->prewhere_actions);
    if (top_k_filter)
        names_to_keep.insert(top_k_filter->column_name);

    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    /// The source fills a column that a block does not have (e.g. one added by `ALTER TABLE ADD COLUMN`
    /// after the block was inserted) with the defaults of the type in both branches, not by evaluating
    /// the `DEFAULT` expression. Do not rely on that, the same way `supportsTopKDynamicFilter` does:
    /// a column with a default expression and the transitive inputs of the expression stay in the main branch.
    const auto & column_defaults = storage_snapshot->metadata->getColumns().getDefaults();
    NameSet names_in_default_expressions;
    std::vector<String> names_to_visit;
    for (const auto & column_name : columns_to_read)
    {
        auto column = storage_snapshot->tryGetColumn(options, column_name);
        if (column && column_defaults.contains(column->getNameInStorage()) && names_in_default_expressions.insert(column->getNameInStorage()).second)
            names_to_visit.push_back(column->getNameInStorage());
    }
    while (!names_to_visit.empty())
    {
        const auto it = column_defaults.find(names_to_visit.back());
        names_to_visit.pop_back();
        if (it == column_defaults.end() || !it->second.expression)
            continue;

        RequiredSourceColumnsVisitor::Data columns_context;
        auto expression = it->second.expression->clone();
        RequiredSourceColumnsVisitor(columns_context).visit(expression);
        for (const auto & required_name : columns_context.requiredColumns())
            if (names_in_default_expressions.insert(required_name).second)
                names_to_visit.push_back(required_name);
    }

    Names main_columns;
    NamesAndTypesList lazy_columns;
    for (const auto & column_name : columns_to_read)
    {
        /// The global row index would be confused with a column of the same name.
        if (column_name == global_row_index_column_name)
            return {};

        auto column = storage_snapshot->tryGetColumn(options, column_name);
        if (column && !names_to_keep.contains(column_name) && output_header->has(column_name)
            && !names_in_default_expressions.contains(column_name) && !names_in_default_expressions.contains(column->getNameInStorage()))
            lazy_columns.push_back(*column);
        else
            main_columns.push_back(column_name);
    }

    if (lazy_columns.empty())
        return {};

    columns_to_read = main_columns;
    required_source_columns = std::move(main_columns);
    read_global_row_index = true;
    rebuildOutputHeader();

    Block lazy_header;
    for (const auto & column : lazy_columns)
        lazy_header.insert({column.type->createColumn(), column.type, column.name});

    return std::make_unique<LazilyReadFromMemoryStorage>(
        std::make_shared<const Block>(std::move(lazy_header)), std::move(lazy_columns), storage_snapshot, num_streams);
}

MemorySourceFilterPtr ReadFromMemoryStorageStep::makeSourceFilter(const NamesAndTypesList & physical_columns) const
{
    /// The threshold filter runs first and shrinks the block before the other steps, which then see
    /// a different set of rows. `tryOptimizeTopK` checks that the filters do not depend on that, but
    /// `optimizePrewhere` may have moved such a condition into PREWHERE after it.
    const bool filters_depend_on_row_set
        = (query_info.row_level_filter && query_info.row_level_filter->actions.hasNonDeterministicOrStatefulFunctions())
        || (query_info.prewhere_info && query_info.prewhere_info->prewhere_actions.hasNonDeterministicOrStatefulFunctions());
    const bool use_top_k_filter = top_k_filter && !filters_depend_on_row_set;

    auto result = std::make_shared<MemorySourceFilter>();
    ExpressionActionsSettings actions_settings(context);

    /// Drop the rows that cannot enter the top-K heap of the query. The comparison is cheap and, once
    /// the threshold is set, usually the most selective of the filters, so it goes first: the columns
    /// of the later steps are then read only for the few remaining rows, and a block where no row is
    /// within the threshold is skipped after reading only the sort column.
    if (use_top_k_filter)
    {
        const auto sort_column = physical_columns.tryGetByName(top_k_filter->column_name);
        if (!sort_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The sort column '{}' of the TopN filter is not read", top_k_filter->column_name);

        ActionsDAG dag({*sort_column});
        const auto * input_node = dag.getInputs().front();
        const auto & filter_node = dag.addFunction(
            createInternalFunctionTopKFilterResolver(top_k_filter->threshold_tracker), {input_node}, {});
        dag.getOutputs() = {input_node, &filter_node};

        /// The steps find columns in the block by name.
        if (!physical_columns.contains(filter_node.result_name) && !output_header->has(filter_node.result_name))
        {
            String filter_column_name = filter_node.result_name;
            result->steps.push_back({
                .actions = std::make_shared<ExpressionActions>(std::move(dag), actions_settings),
                .filter_column_name = std::move(filter_column_name),
                .remove_filter_column = true,
                .input_columns = {},
            });
        }
    }

    /// The row-level security filter runs first, so PREWHERE expressions are never evaluated
    /// on the rows the policy hides.
    if (query_info.row_level_filter)
    {
        const auto & row_level_filter = *query_info.row_level_filter;
        result->steps.push_back({
            .actions = std::make_shared<ExpressionActions>(row_level_filter.actions.clone(), actions_settings),
            .filter_column_name = row_level_filter.column_name,
            .remove_filter_column = row_level_filter.do_remove_column,
            .input_columns = {},
        });
    }

    if (query_info.prewhere_info)
    {
        /// Split a conjunction into steps, so that the columns of a later condition are read only
        /// for the rows that passed the earlier ones. The steps always filter the block, which is
        /// what their `need_filter` asks for, so it is not needed here.
        /// A stateful function (e.g. `runningConcurrency`, `rowNumberInBlock`) or a function that is
        /// non-deterministic in scope of the query (e.g. `blockSize`, `rand`) depends on the set of
        /// rows it is evaluated on, so a condition with it must see all rows, not only those that
        /// passed the preceding conditions: such a PREWHERE is evaluated in a single step.
        PrewhereExprInfo prewhere_steps;
        if (context->getSettingsRef()[Setting::enable_multiple_prewhere_read_steps]
            && !query_info.prewhere_info->prewhere_actions.hasNonDeterministicOrStatefulFunctions()
            && tryBuildPrewhereSteps(
                query_info.prewhere_info,
                actions_settings,
                prewhere_steps,
                /*force_short_circuit_execution*/ false,
                &storage_snapshot->metadata->getColumns()))
        {
            for (const auto & step : prewhere_steps.steps)
            {
                result->steps.push_back({
                    .actions = step->actions,
                    .filter_column_name = step->filter_column_name,
                    .remove_filter_column = step->remove_filter_column,
                    .input_columns = {},
                });
            }
        }
        else
        {
            const auto & prewhere_info = *query_info.prewhere_info;
            result->steps.push_back({
                .actions = std::make_shared<ExpressionActions>(prewhere_info.prewhere_actions.clone(), actions_settings),
                .filter_column_name = prewhere_info.prewhere_column_name,
                .remove_filter_column = prewhere_info.remove_prewhere_column,
                .input_columns = {},
            });
        }
    }

    for (auto & step : result->steps)
    {
        const Names required_columns = step.actions->getRequiredColumns();
        const NameSet required_column_names(required_columns.begin(), required_columns.end());
        for (const auto & name_and_type : physical_columns)
            if (required_column_names.contains(name_and_type.name))
                step.input_columns.push_back(name_and_type);
    }

    for (const auto & elem : *output_header)
        if (auto name_and_type = physical_columns.tryGetByName(elem.name))
            result->output_columns.push_back(*name_and_type);

    /// The query condition cache. A read for a global subquery takes the blocks of the storage when it
    /// starts, not those of the snapshot, so their identity is unknown here. The granules skipped by
    /// the cache are removed before the steps, so it cannot be used if they depend on the set of rows
    /// they see, like the top-K filter above (e.g. `PREWHERE rowNumberInBlock() = 1` would match
    /// a different row).
    const auto & settings = context->getSettingsRef();
    const UUID table_uuid = storage->getStorageID().uuid;
    if (settings[Setting::use_query_condition_cache] && table_uuid != UUIDHelpers::Nil && !delay_read_for_global_sub_queries
        && !filters_depend_on_row_set
        && (!use_top_k_filter || settings[Setting::use_query_condition_cache_for_top_k]))
    {
        if (query_info.prewhere_info)
        {
            const auto & prewhere_info = *query_info.prewhere_info;
            const auto * prewhere_node = prewhere_info.prewhere_actions.tryFindInOutputs(prewhere_info.prewhere_column_name);
            if (prewhere_node && VirtualColumnUtils::isDeterministic(prewhere_node))
            {
                result->prewhere_condition_hash = queryConditionCacheHash(prewhere_node->getHash(), queryConditionCacheSettingsSalt(settings));
                result->prewhere_condition = prewhere_info.prewhere_column_name;
                result->write_prewhere_condition = !use_top_k_filter && !query_info.row_level_filter;
            }
        }

        result->filter_condition_hash = getFilterConditionHashForQueryConditionCache();
        result->attach_mark_ranges_info = result->filter_condition_hash && result->steps.empty();

        if (result->prewhere_condition_hash || result->filter_condition_hash)
        {
            const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
            result->query_condition_cache = context->getQueryConditionCache();
            result->table_uuid = table_uuid;
            result->generation = snapshot_data.generation;
            result->first_block_number = snapshot_data.first_block_number;
            result->columns_hash = sipHash64(storage_snapshot->metadata->getColumns().toString(/*include_comments=*/ false));
        }
    }

    /// Without the steps, the filter is needed only for the query condition cache.
    if (result->steps.empty() && !result->query_condition_cache)
        return nullptr;

    return result;
}

static Names getArraysOfRequestedNested(const NamesAndTypesList & physical_columns, const StorageSnapshotPtr & storage_snapshot)
{
    NameSet nested_names;
    for (const auto & column : physical_columns)
    {
        auto [nested_name, member_name] = Nested::splitName(column.getNameInStorage());
        if (isArray(column.getTypeInStorage()) && !member_name.empty())
            nested_names.insert(std::move(nested_name));
    }

    Names result;
    if (nested_names.empty())
        return result;

    for (const auto & column : storage_snapshot->metadata->getColumns().getAllPhysical())
    {
        auto [nested_name, member_name] = Nested::splitName(column.name);
        if (isArray(column.type) && !member_name.empty() && nested_names.contains(nested_name))
            result.push_back(column.name);
    }

    return result;
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    /// With lazy materialization, every column may be deferred to the lazy branch, e.g. for
    /// `ORDER BY rowNumberInAllBlocks()`, and the main branch reads only the global row index.
    if (!read_global_row_index || !columns_to_read.empty())
        storage_snapshot->check(columns_to_read);

    auto [physical_column_names, virtual_column_names] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(columns_to_read, storage_snapshot);
    auto physical_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), physical_column_names);
    auto virtual_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader), virtual_column_names);

    auto source_filter = makeSourceFilter(physical_columns);
    /// Virtual columns are not filled by a source with a filter. Such a filter is needed only for the query condition cache then.
    if (source_filter && source_filter->steps.empty() && !virtual_columns.empty())
        source_filter = nullptr;
    auto nested_arrays = getArraysOfRequestedNested(physical_columns, storage_snapshot);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            physical_columns,
            virtual_columns,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [my_storage = storage](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                auto current = assert_cast<const StorageMemory &>(*my_storage).data.get();
                data_to_initialize = std::shared_ptr<const Blocks>(current, &current->blocks);
            },
            typeid_cast<StorageMemory *>(storage.get())->getMaterializedCTE(),
            source_filter,
            output_header,
            nullptr /* block start rows */,
            nested_arrays));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);
    auto block_start_rows = read_global_row_index ? makeBlockStartRows(*current_data) : nullptr;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto source = std::make_shared<MemorySource>(
            physical_columns, virtual_columns, current_data, parallel_execution_index, nullptr, nullptr, source_filter, output_header, block_start_rows,
            nested_arrays);
        if (stream == 0)
            source->addTotalRowsApprox(snapshot_data.rows);
        pipes.emplace_back(std::move(source));
    }
    return Pipe::unitePipes(std::move(pipes));
}

LazilyReadFromMemoryStorage::LazilyReadFromMemoryStorage(
    SharedHeader header, NamesAndTypesList columns_, StorageSnapshotPtr storage_snapshot_, size_t num_streams_)
    : ISourceStep(std::move(header))
    , columns(std::move(columns_))
    , storage_snapshot(std::move(storage_snapshot_))
    , num_streams(num_streams_)
{
}

void LazilyReadFromMemoryStorage::setLazyMaterializingRows(MemoryLazyMaterializingRowsPtr lazy_materializing_rows_)
{
    lazy_materializing_rows = std::move(lazy_materializing_rows_);
}

void LazilyReadFromMemoryStorage::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazilyReadFromMemoryStorage: lazy_materializing_rows is not set");

    /// The same snapshot as the main branch has, so the global row indexes refer to the same blocks.
    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto source = std::make_shared<LazyReadFromMemorySource>(
        getOutputHeader(), columns, snapshot_data.blocks, lazy_materializing_rows, num_streams,
        getArraysOfRequestedNested(columns, storage_snapshot));

    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

void LazilyReadFromMemoryStorage::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column : *getOutputHeader())
    {
        if (!first)
            settings.out << ", ";
        first = false;

        settings.out << column.name;
    }

    settings.out << '\n';
}

void LazilyReadFromMemoryStorage::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & column : *getOutputHeader())
        json_array->add(column.name);

    map.add("Lazily read columns", std::move(json_array));
}

}
