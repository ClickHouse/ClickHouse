#include <Processors/Transforms/FilterTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Interpreters/ActionsDAG.h>
#include <Functions/IFunction.h>
#include <Common/assert_cast.h>

namespace ProfileEvents
{
    extern const Event FilterTransformPassedRows;
    extern const Event FilterTransformPassedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

bool FilterTransform::canUseType(const DataTypePtr & filter_type)
{
    return filter_type->canBeUsedInBooleanContext();
}

auto incrementProfileEvents = [](size_t num_rows, const Columns & columns)
{
    ProfileEvents::increment(ProfileEvents::FilterTransformPassedRows, num_rows);

    size_t num_bytes = 0;
    for (const auto & column : columns)
    {
        if (column)
            num_bytes += column->byteSize();
    }
    ProfileEvents::increment(ProfileEvents::FilterTransformPassedBytes, num_bytes);
};

/// The part of transformHeader after the expression: validate the filter column type
/// and remove the filter column if requested.
static Block checkAndRemoveFilterColumn(Block result, const String & filter_column_name, bool remove_filter_column)
{
    auto filter_type = result.getByName(filter_column_name).type;
    if (!FilterTransform::canUseType(filter_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column {} for filter. Must be native integer or float type",
            filter_type->getName(), filter_column_name);

    if (remove_filter_column)
        result.erase(filter_column_name);

    return result;
}

/// constant folding in prepare misses an empty set behind a Nullable argument - no constness at 0 rows
static bool isAlwaysFalseByEmptySet(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.at(0);

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    const auto & function_name = node->function_base->getName();

    if (function_name == "and")
        return std::any_of(node->children.begin(), node->children.end(), isAlwaysFalseByEmptySet);

    /// notIn over an empty set is always true, and the -IgnoreSet variants must not fold
    if (function_name != "in" && function_name != "globalIn")
        return false;

    const IColumn * set_column = node->children[1]->column.get();
    if (!set_column)
        return false;

    if (const auto * const_column = typeid_cast<const ColumnConst *>(set_column))
        set_column = &const_column->getDataColumn();

    const auto * column_set = typeid_cast<const ColumnSet *>(set_column);
    if (!column_set)
        return false;

    auto future_set = column_set->getData();
    if (!future_set)
        return false;

    auto set = future_set->get();
    return set && set->getTotalRowCount() == 0;
}

Block FilterTransform::transformHeader(
    const Block & header, const ActionsDAG * expression, const String & filter_column_name, bool remove_filter_column)
{
    /// updateHeader (dry-run evaluation) correctly handles constant propagation,
    /// unlike expression->execute() which would fail with not-ready sets.
    return checkAndRemoveFilterColumn(
        expression ? expression->updateHeader(header) : header, filter_column_name, remove_filter_column);
}

FilterTransform::FilterTransform(
    SharedHeader header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_,
    std::optional<std::pair<UInt64, String>> condition_,
    bool update_row_numbers_info_)
    : FilterTransform(
            header_,
            std::make_shared<const Block>(expression_ ? expression_->getActionsDAG().updateHeader(*header_) : *header_),
            expression_, /// Deliberately a copy, not a move: the previous argument reads expression_,
                         /// and the evaluation order of arguments is unspecified.
            std::move(filter_column_name_),
            remove_filter_column_,
            on_totals_,
            std::move(rows_filtered_),
            std::move(condition_),
            update_row_numbers_info_)
{
}

FilterTransform::FilterTransform(
    SharedHeader header_,
    SharedHeader transformed_header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_,
    std::optional<std::pair<UInt64, String>> condition_,
    bool update_row_numbers_info_)
    : ISimpleTransform(
            header_,
            std::make_shared<const Block>(checkAndRemoveFilterColumn(*transformed_header_, filter_column_name_, remove_filter_column_)),
            true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , update_row_numbers_info(update_row_numbers_info_)
    , rows_filtered(rows_filtered_)
    , condition(condition_)
    , transformed_header(*transformed_header_)
{
    if (expression)
    {
        /// Special check to stop queries like "WHERE ignore(...)"
        const auto * node = &expression->getActionsDAG().findInOutputs(filter_column_name);
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children[0];

        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "ignore")
            always_false = true;
    }

    filter_column_position = transformed_header.getPositionByName(filter_column_name);

    auto & column = transformed_header.getByPosition(filter_column_position).column;
    if (column)
        always_false = always_false || ConstantFilterDescription(*column).always_false;

    if (condition.has_value())
        query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
}

IProcessor::Status FilterTransform::prepare()
{
    /// Re-evaluate the filter on the header to enable constant-fold early exit
    /// for expressions like `WHERE 1 IN (subquery)` or `WHERE x IN (empty set)`
    /// where the result becomes known only after the set is built.
    /// Use updateHeader (dry-run) because sets may not be ready yet even when
    /// output.isNeeded() is true (e.g. delayed set creation in mutations).
    if (!are_prepared_sets_initialized && output.isNeeded())
    {
        are_prepared_sets_initialized = true;

        if (!always_false && expression && !on_totals)
        {
            const auto & actions_dag = expression->getActionsDAG();
            always_false = isAlwaysFalseByEmptySet(&actions_dag.findInOutputs(filter_column_name));

            if (!always_false)
            {
                auto header = actions_dag.updateHeader(getInputPort().getHeader());
                auto & column = header.getByPosition(filter_column_position).column;
                if (column)
                    always_false = ConstantFilterDescription(*column).always_false;
            }
        }
    }

    if (always_false && !on_totals)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    auto status = ISimpleTransform::prepare();

    if (status == IProcessor::Status::Finished)
        writeIntoQueryConditionCache({});

    return status;
}


void FilterTransform::removeFilterIfNeed(Columns & columns) const
{
    if (remove_filter_column)
        columns.erase(columns.begin() + filter_column_position);
}

void FilterTransform::transform(Chunk & chunk)
{
    auto chunk_rows_before = chunk.getNumRows();
    doTransform(chunk);
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk.getNumRows();
}

namespace
{

/// Compose `filter` (a dense mask over this chunk's pre-filter rows) into the chunk's
/// `ChunkInfoRowNumbers.applied_filter`, mirroring `DeletionVectorTransform`, so physical row
/// numbers survive filtering. No-op when the chunk carries no such info.
void updateRowNumbersInfo(const Chunk & chunk, const IColumn::Filter & filter)
{
    auto row_numbers_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
    if (!row_numbers_info)
        return;

    auto & applied_filter = row_numbers_info->applied_filter;
    if (applied_filter.has_value())
    {
        /// The mask must have one element per set bit of the existing one. A mismatch means an
        /// upstream row-changing transform already left the info stale, so it cannot be repaired
        /// here; the callers that opt in are exactly those whose upstream transforms maintain it.
        if (countBytesInFilter(*applied_filter) != filter.size())
            return;

        /// Walk the set bits of the existing mask and clear the ones this filter drops.
        size_t idx_in_chunk = 0;
        for (auto & passed : applied_filter.value())
        {
            if (passed)
            {
                if (!filter[idx_in_chunk])
                    passed = 0;
                ++idx_in_chunk;
            }
        }
    }
    else
    {
        /// First filtering on this chunk: the mask directly becomes the applied filter.
        /// `IColumnFilter` (PaddedPODArray) is noncopyable, so copy explicitly via `assign`.
        applied_filter.emplace();
        applied_filter->assign(filter);
    }
}

}

void FilterTransform::doTransform(Chunk & chunk)
{
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    DataTypes types;

    {
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        columns.clear();

        if (expression)
            expression->execute(block, num_rows_before_filtration);

        columns = block.getColumns();
        types = block.getDataTypes();
    }

    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];
    ConstantFilterDescription constant_filter_description(*filter_column);

    if (constant_filter_description.always_true || on_totals || isVirtualRow(chunk))
    {
        incrementProfileEvents(num_rows_before_filtration, columns);
        removeFilterIfNeed(columns);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return;
    }

    if (constant_filter_description.always_false)
    {
        writeIntoQueryConditionCache(chunk.getChunkInfos().get<MarkRangesInfo>());
        incrementProfileEvents(0, {});
        return;
    }

    std::unique_ptr<IFilterDescription> filter_description;

    if (isColumnConst(*filter_column))
        filter_column = filter_column->convertToFullColumnIfConst();

    if (filter_column->isSparse())
    {
        /// SparseFilterDescription only supports Sparse(UInt8) and Sparse(Nullable(UInt8)).
        /// For other types (e.g. Float64 when WHERE uses sin(col)), fall back to the
        /// regular FilterDescription which converts any numeric type to a boolean filter.
        const auto * column_sparse = typeid_cast<const ColumnSparse *>(filter_column.get());
        const auto & values_column = column_sparse->getValuesColumn();
        if (typeid_cast<const ColumnUInt8 *>(&values_column)
            || (typeid_cast<const ColumnNullable *>(&values_column)
                && typeid_cast<const ColumnUInt8 *>(&assert_cast<const ColumnNullable &>(values_column).getNestedColumn())))
        {
            filter_description = std::make_unique<SparseFilterDescription>(*filter_column);
        }
        else
        {
            filter_column = filter_column->convertToFullColumnIfSparse();
            filter_description = std::make_unique<FilterDescription>(*filter_column);
        }
    }
    else
        filter_description = std::make_unique<FilterDescription>(*filter_column);

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    size_t min_size_in_memory = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < num_columns; ++i)
    {
        DataTypePtr type_not_null = removeNullableOrLowCardinalityNullable(types[i]);
        if (i != filter_column_position && !isColumnConst(*columns[i]) && type_not_null->isValueRepresentedByNumber())
        {
            size_t size_in_memory = type_not_null->getSizeOfValueInMemory() + (isNullableOrLowCardinalityNullable(types[i]) ? 1 : 0);
            if (size_in_memory < min_size_in_memory)
            {
                min_size_in_memory = size_in_memory;
                first_non_constant_column = i;
            }
        }
    }
    (void)min_size_in_memory; /// Suppress error of clang-analyzer-deadcode.DeadStores

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = filter_description->filter(*columns[first_non_constant_column], -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = filter_description->countBytesInFilter();

    incrementProfileEvents(num_filtered_rows, columns);

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
    {
        writeIntoQueryConditionCache(chunk.getChunkInfos().get<MarkRangesInfo>());
        /// SimpleTransform will skip it.
        return;
    }

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        /// No need to touch the rest of the columns.
        removeFilterIfNeed(columns);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return;
    }

    /// Rows are actually being dropped here. When enabled and the chunk carries `ChunkInfoRowNumbers`,
    /// record the mask so downstream `_row_number` / positional-delete consumers keep the correct
    /// physical row numbers. Opt-in (see `update_row_numbers_info`) and only worth building the dense
    /// mask when the info is present, so guard on both.
    if (update_row_numbers_info && chunk.getChunkInfos().get<ChunkInfoRowNumbers>())
    {
        auto mask_column = FilterDescription::preprocessFilterColumn(filter_column);
        const IColumn::Filter & mask = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
        updateRowNumbersInfo(chunk, mask);
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & current_column = columns[i];

        if (i == filter_column_position && remove_filter_column)
            continue;

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = filter_description->filter(*current_column, num_filtered_rows);
    }

    removeFilterIfNeed(columns);
    chunk.setColumns(std::move(columns), num_filtered_rows);
}

void FilterTransform::writeIntoQueryConditionCache(const MarkRangesInfoPtr & mark_ranges_info)
{
    if (!query_condition_cache)
        return;

    /// A transform between the reading step and this filter (e.g. `FilterSortedStreamByRange`
    /// when the read is split into layers for FINAL or for join-by-PK-ranges) already dropped
    /// rows from the chunk. "No rows matched" then holds only for the surviving rows, not for
    /// every row of the chunk's mark ranges: rows of the same granules in another layer may
    /// still match. Writing such ranges would poison the cache for later queries.
    if (mark_ranges_info && mark_ranges_info->has_dropped_rows)
        return;

    if (!mark_ranges_info)
    {
        /// FilterTransform has finished, we need to flush to the query result cache.

        if (!buffered_mark_ranges_info)
            return;

        query_condition_cache->write(
            buffered_mark_ranges_info->table_uuid,
            buffered_mark_ranges_info->part_name,
            condition->first,
            condition->second,
            buffered_mark_ranges_info->mark_ranges,
            buffered_mark_ranges_info->marks_count,
            buffered_mark_ranges_info->has_final_mark);

        buffered_mark_ranges_info = nullptr;

        return;
    }

    if (!buffered_mark_ranges_info)
    {
        buffered_mark_ranges_info = std::static_pointer_cast<MarkRangesInfo>(mark_ranges_info->clone());
    }
    else
    {
        /// If the current and the buffer mark range info are from the same table/part, append to the buffer.
        /// Otherwise write to the query condition cache and reset the buffer.

        if (buffered_mark_ranges_info->table_uuid != mark_ranges_info->table_uuid || buffered_mark_ranges_info->part_name != mark_ranges_info->part_name)
        {
            query_condition_cache->write(
                buffered_mark_ranges_info->table_uuid,
                buffered_mark_ranges_info->part_name,
                condition->first,
                condition->second,
                buffered_mark_ranges_info->mark_ranges,
                buffered_mark_ranges_info->marks_count,
                buffered_mark_ranges_info->has_final_mark);

            buffered_mark_ranges_info = std::static_pointer_cast<MarkRangesInfo>(mark_ranges_info->clone());
        }
        else
        {
            buffered_mark_ranges_info->appendMarkRanges(mark_ranges_info->mark_ranges);
        }
    }
}

}
