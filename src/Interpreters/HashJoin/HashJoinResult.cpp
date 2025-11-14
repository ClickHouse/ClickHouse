#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnReplicated.h>
#include <Common/memcpySmall.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&*column.column))
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

static void correctNullabilityInplace(
    ColumnWithTypeAndName & column, bool nullable, const IColumn::Filter & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
        if (column.type->isNullable() && !negative_null_map.empty())
        {
            MutableColumnPtr mutable_column = IColumn::mutate(std::move(column.column));
            assert_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    else
        JoinCommon::removeColumnNullability(column);
}

static ColumnWithTypeAndName copyLeftKeyColumnToRight(
    const DataTypePtr & right_key_type,
    const String & renamed_right_column,
    const ColumnWithTypeAndName & left_column,
    const IColumn::Filter * null_map_filter)
{
    ColumnWithTypeAndName right_column = left_column;
    right_column.name = renamed_right_column;

    if (null_map_filter)
        right_column.column = JoinCommon::filterWithBlanks(right_column.column, *null_map_filter);

    bool should_be_nullable = isNullableOrLowCardinalityNullable(right_key_type);
    if (null_map_filter)
        correctNullabilityInplace(right_column, should_be_nullable, *null_map_filter);
    else
        correctNullabilityInplace(right_column, should_be_nullable);

    if (!right_column.type->equals(*right_key_type))
    {
        right_column.column = castColumnAccurate(right_column, right_key_type);
        right_column.type = right_key_type;
    }

    right_column.column = right_column.column->convertToFullColumnIfConst();
    return right_column;
}

static void replicateColumnLazily(ColumnPtr & column, const IColumn::Offsets & offsets, ColumnPtr & indexes)
{
    if (isLazyReplicationUseful(column))
    {
        if (!indexes)
            indexes = convertOffsetsToIndexes(offsets);
        column = ColumnReplicated::create(column, indexes);
    }
    else
    {
        column = column->replicate(offsets);
    }
}

static void appendRightColumns(
    Block & block,
    MutableColumns columns,
    const IColumn::Offsets & offsets,
    const IColumn::Filter & filter,
    const NamesAndTypes & type_name,
    const HashJoinResult::Properties & properties)
{
    size_t existing_columns = block.columns();
    const auto & table_join = properties.table_join;

    std::set<size_t> block_columns_to_erase;
    if (HashJoin::canRemoveColumnsFromLeftBlock(table_join))
    {
        std::unordered_set<String> left_output_columns;
        for (const auto & out_column : table_join.getOutputColumns(JoinTableSide::Left))
            left_output_columns.insert(out_column.name);
        for (size_t i = 0; i < block.columns(); ++i)
        {
            if (!left_output_columns.contains(block.getByPosition(i).name))
                block_columns_to_erase.insert(i);
        }
    }

    for (size_t i = 0; i < columns.size(); ++i)
    {
        ColumnWithTypeAndName col;
        col.column = std::move(columns[i]);
        col.name = table_join.renamedRightColumnName(type_name[i].name);
        col.type = type_name[i].type;
        block.insert(std::move(col));
    }

    bool is_asof_join = table_join.strictness() == JoinStrictness::Asof;
    std::vector<size_t> right_keys_to_replicate;

    /// Add join key columns from right block if needed.
    for (size_t i = 0; i < properties.required_right_keys.columns(); ++i)
    {
        const auto & right_key = properties.required_right_keys.getByPosition(i);
        /// asof column is already in block.
        if (is_asof_join && right_key.name == table_join.getOnlyClause().key_names_right.back())
            continue;

        const auto & left_column = block.getByName(properties.required_right_keys_sources[i]);
        const auto & right_col_name = table_join.renamedRightColumnName(right_key.name);
        const auto * filter_ptr = properties.need_filter ? nullptr : &filter;
        auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column, filter_ptr);
        block.insert(std::move(right_col));

        if (!offsets.empty())
            right_keys_to_replicate.push_back(block.getPositionByName(right_col_name));
    }

    if (!offsets.empty())
    {
        chassert(!block.empty());
        chassert(offsets.size() == block.rows());

        auto columns_to_replicate = block.getColumns();
        if (properties.enable_lazy_columns_replication)
        {
            ColumnPtr indexes;
            for (size_t i = 0; i < existing_columns; ++i)
                replicateColumnLazily(columns_to_replicate[i], offsets, indexes);
            for (size_t pos : right_keys_to_replicate)
                replicateColumnLazily(columns_to_replicate[pos], offsets, indexes);
        }
        else
        {
            for (size_t i = 0; i < existing_columns; ++i)
                columns_to_replicate[i] = columns_to_replicate[i]->replicate(offsets);
            for (size_t pos : right_keys_to_replicate)
                columns_to_replicate[pos] = columns_to_replicate[pos]->replicate(offsets);
        }

        block.setColumns(columns_to_replicate);
    }

    block.erase(block_columns_to_erase);
}

MutableColumns copyEmptyColumns(const MutableColumns & columns)
{
    MutableColumns res_columns;
    res_columns.reserve(columns.size());
    for (const auto & column : columns)
        res_columns.push_back(column->cloneEmpty());
    return res_columns;
}

void applyShiftAndLimitToOffsets(const IColumn::Offsets & offsets, IColumn::Offsets & out_offsets, UInt64 shift, UInt64 limit)
{
    out_offsets.clear();
    out_offsets.resize_fill(offsets.size(), 0);
    if (offsets.empty())
        return;

    const UInt64 total = offsets.back();

    if (limit == 0 || shift >= total)
        return;

    const UInt64 end = shift + std::min(limit, total - shift);

    for (size_t i = 0, n = offsets.size(); i < n; ++i)
    {
        out_offsets[i] = std::min(offsets[i], end);
        out_offsets[i] = std::max<Int64>(0, out_offsets[i] - shift);
    }
}

/// Generates joined block from lazy output
/// If state is not finished, it will keep it for next call, otherwise reset it.
Block HashJoinResult::generateBlock(
    std::optional<HashJoinResult::GenerateCurrentRowState> & state,
    const LazyOutput & lazy_output,
    const HashJoinResult::Properties & properties)
{
    size_t rows_added = 0;
    const auto * off_data = lazy_output.row_refs.data();

    MutableColumns columns;
    if (state->state_row_limit > 0)
    {
        /// columns are empty when using lazy_output
        chassert(std::ranges::all_of(columns, [](const auto & col) { return col->empty(); }));
        columns = copyEmptyColumns(state->columns);
    }
    else
    {
        columns = std::move(state->columns);
    }

    if (properties.is_join_get)
    {
        lazy_output.buildJoinGetOutput(
            state->rows_to_reserve, columns,
            off_data + state->row_ref_begin, off_data + state->row_ref_end);
    }
    else
    {
        rows_added = lazy_output.buildOutput(
            state->rows_to_reserve, state->block, state->offsets, columns,
            off_data + state->row_ref_begin, off_data + state->row_ref_end,
            state->state_row_offset, state->state_row_limit, state->state_bytes_limit);
    }

    IColumn::Offsets offsets;
    auto last_offset = state->offsets.empty() ? 0 : state->offsets.back();

    if (state->state_row_limit > 0)
        /// Apply row offset and limit to create a subset of the original offsets.
        /// Example: original offsets [3, 5, 8, 10], row_offset=4, row_limit=3 -> [0, 1, 3, 3]
        /// This extracts rows [4, 7] from the full result.
        applyShiftAndLimitToOffsets(state->offsets, offsets, state->state_row_offset, rows_added);
    else
        offsets = std::move(state->offsets);

    Block block;
    bool is_state_finished = false;
    if (state->state_row_limit == 0 || rows_added == 0 || state->state_row_offset + rows_added >= last_offset)
    {
        block = std::move(state->block);
        is_state_finished = true;
    }
    else
    {
        state->state_row_offset += rows_added;
        block = state->block;
    }


    appendRightColumns(
        block,
        std::move(columns),
        offsets,
        state->filter,
        lazy_output.type_name,
        properties);

    if (is_state_finished)
        state.reset();

    return block;
}

static size_t numLeftRowsForNextBlock(
    size_t next_row,
    const IColumn::Offsets & offsets,
    size_t max_joined_block_rows,
    size_t max_joined_block_bytes,
    size_t avg_bytes_per_row)
{
    /// If rows are not replicated, do not split block.
    if (offsets.empty() || (max_joined_block_rows == 0 && max_joined_block_bytes == 0))
        return 0;

    /// If offsets does not increase block size, do not split block.
    if (offsets.back() <= offsets.size())
        return 0;

    size_t max_rows = max_joined_block_rows;
    if (max_joined_block_bytes)
        max_rows = std::min<size_t>(max_rows, max_joined_block_bytes / std::max<size_t>(avg_bytes_per_row, 1));

    const size_t prev_offset = next_row ? offsets[next_row - 1] : 0;
    const size_t next_allowed_offset = prev_offset + max_rows;

    if (offsets.back() <= next_allowed_offset)
        return offsets.size() - next_row;

    size_t lhs = next_row;
    size_t rhs = offsets.size();
    while (rhs - lhs > 1)
    {
        size_t mid = (lhs + rhs) / 2;
        if (offsets[mid] > next_allowed_offset)
            rhs = mid;
        else
            lhs = mid;
    }

    return std::max<size_t>(rhs - next_row, 1);
}

HashJoinResult::HashJoinResult(
    LazyOutput && lazy_output_,
    MutableColumns columns_,
    IColumn::Offsets offsets_,
    IColumn::Filter filter_,
    IColumn::Offsets && matched_rows_,
    ScatteredBlock && block_,
    Properties properties_)
    : lazy_output(std::move(lazy_output_))
    , properties(std::move(properties_))
    , scattered_block(std::move(block_))
    , columns(std::move(columns_))
    , offsets(std::move(offsets_))
    , filter(std::move(filter_))
    , matched_rows(std::move(matched_rows_))
{
}

HashJoinResult::~HashJoinResult() = default;

static size_t getAvgBytesPerRow(const Block & block)
{
    return block.allocatedBytes() / std::max<size_t>(1, block.rows());
}

IJoinResult::JoinResultBlock HashJoinResult::next()
{
    if (current_row_state)
    {
        bool is_last = current_row_state->is_last;
        auto block = generateBlock(current_row_state, lazy_output, properties);
        return {std::move(block), is_last && !current_row_state.has_value()};
    }

    if (!scattered_block)
        return {};

    size_t limit_rows_per_key = 0;
    size_t limit_bytes_per_key = 0;
    /// We can split when using lazy_output with row_refs and offsets
    if (properties.joined_block_split_single_row
        && properties.max_joined_block_rows > 0
        /// ignore join get, it has any join semantics
        && !properties.is_join_get
        && !offsets.empty()
        /// check if using lazy_output with row_refs
        && lazy_output.output_by_row_list
        /// sorted need different build output logic that supports ranges
        && !lazy_output.join_data_sorted
        /// columns are empty when using lazy_output
        && std::ranges::all_of(columns, [](const auto & col) { return col->empty(); }))
    {
        limit_rows_per_key = properties.max_joined_block_rows;
        limit_bytes_per_key = properties.max_joined_block_bytes;
    }

    size_t avg_bytes_per_row = properties.avg_joined_bytes_per_row + getAvgBytesPerRow(scattered_block->getSourceBlock());
    auto num_lhs_rows = numLeftRowsForNextBlock(next_row, offsets, properties.max_joined_block_rows, properties.max_joined_block_bytes, avg_bytes_per_row);

    if (num_lhs_rows == 0 || (next_row == 0 && num_lhs_rows >= scattered_block->rows()))
    {
        /// Note: need_filter flag cannot be replaced with !added_columns.need_filter.empty()
        /// This is because e.g. for ALL LEFT JOIN filter is used to replace non-matched right keys to defaults.
        if (properties.need_filter)
            scattered_block->filter(std::span<UInt64>{matched_rows});
        scattered_block->filterBySelector();

        current_row_state.emplace(GenerateCurrentRowState{
            .block = std::move(*scattered_block).getSourceBlock(),
            .rows_to_reserve = lazy_output.row_count,
            .row_ref_begin = 0,
            .row_ref_end = lazy_output.row_refs.size(),
            .columns = std::move(columns),
            .offsets = std::move(offsets),
            .filter = std::move(filter),
            .matched_rows = std::span<UInt64>{matched_rows},
            .is_last = true,
            .state_row_limit = limit_rows_per_key,
            .state_bytes_limit = limit_bytes_per_key,
        });

        auto block = generateBlock(current_row_state, lazy_output, properties);
        scattered_block.reset();
        return {std::move(block), !current_row_state.has_value()};
    }

    const size_t prev_offset = next_row ? offsets[next_row - 1] : 0;
    size_t num_rhs_rows = offsets[next_row + num_lhs_rows - 1] - prev_offset;

    auto current_scattered_block = std::move(*scattered_block);
    scattered_block = current_scattered_block.cut(num_lhs_rows);

    bool add_missing = isLeftOrFull(properties.table_join.kind()) && properties.table_join.strictness() != JoinStrictness::Semi;
    size_t num_skipped_not_matched_rows_in_row_ref_list = 0;

    IColumn::Offsets partial_offsets;
    partial_offsets.resize(num_lhs_rows);

    if (lazy_output.output_by_row_list && !add_missing && !lazy_output.row_refs.empty())
    {
        /// In case of ALL INNER/RIGHT JOIN, non-matched rows are not added to row_refs.
        /// In order to understand, how many row_refs we need to process,
        /// we also need to count how many non-matched rows we have.
        /// Row is non-matched when the offset did not change.
        size_t last_offset = prev_offset;
        for (size_t row = 0; row < num_lhs_rows; ++row)
        {
            auto offset = offsets[row + next_row];
            partial_offsets[row] = offset - prev_offset;
            if (offset == last_offset)
                ++num_skipped_not_matched_rows_in_row_ref_list;
            last_offset = offset;
        }
    }
    else
    {
        for (size_t row = 0; row < num_lhs_rows; ++row)
            partial_offsets[row] = offsets[row + next_row] - prev_offset;
    }

    size_t num_refs = 0;
    if (!lazy_output.row_refs.empty())
    {
        num_refs = num_rhs_rows;
        if (lazy_output.output_by_row_list)
            num_refs = num_lhs_rows - num_skipped_not_matched_rows_in_row_ref_list;
    }

    IColumn::Filter partial_filter;
    std::span<UInt64> partial_matched_rows;
    if (!filter.empty())
    {
        partial_filter.resize(num_lhs_rows);
        memcpySmallAllowReadWriteOverflow15(partial_filter.data(), filter.data() + next_row, num_lhs_rows);
        const auto old_selector_it = next_matched_rows_it;
        while (next_matched_rows_it < matched_rows.size() && matched_rows[next_matched_rows_it] < next_row + num_lhs_rows)
            ++next_matched_rows_it;
        partial_matched_rows = std::span<UInt64>{&matched_rows[old_selector_it], &matched_rows[next_matched_rows_it]};
    }

    const auto row_ref_start = next_row_ref;
    const auto start_row = next_row;

    next_row += num_lhs_rows;
    next_row_ref += num_refs;
    num_joined_rows += num_rhs_rows;

    if (!lazy_output.row_refs.empty())
    {
        if (next_row_ref > lazy_output.row_refs.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The number of joined row_refs {} is more than expected number of row_refs {}",
                lazy_output.row_refs.size(), next_row_ref);

        if (num_joined_rows > lazy_output.row_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The number or joined rows {} is more than expected number of rows {}",
                num_joined_rows, lazy_output.row_count);
    }

    bool is_last = next_row >= offsets.size();

    MutableColumns rhs_columns; /// Columns from the right table
    if (!lazy_output.row_refs.empty())
    {
        rhs_columns = copyEmptyColumns(columns);
    }
    else
    {
        if (start_row == 0 && is_last)
        {
            rhs_columns = std::move(columns);
        }
        else
        {
            /// The result columns should contain only data for the current block.
            /// Copy data from the original columns to preserve columns size in the block.
            rhs_columns.reserve(columns.size());
            for (auto & column : columns)
                rhs_columns.push_back(column->cut(start_row, num_rhs_rows)->assumeMutable());

            if (is_last)
                columns.clear();
        }
    }


    /// Note: need_filter flag cannot be replaced with !added_columns.need_filter.empty()
    /// This is because e.g. for ALL LEFT JOIN filter is used to replace non-matched right keys to defaults.
    if (properties.need_filter)
        current_scattered_block.filter(partial_matched_rows);
    current_scattered_block.filterBySelector();

    current_row_state.emplace(GenerateCurrentRowState{
        .block = std::move(current_scattered_block).getSourceBlock(),
        .rows_to_reserve = num_rhs_rows,
        .row_ref_begin = row_ref_start,
        .row_ref_end = next_row_ref,
        .columns = std::move(rhs_columns),
        .offsets = std::move(partial_offsets),
        .filter = std::move(partial_filter),
        .matched_rows = partial_matched_rows,
        .is_last = is_last,
        .state_row_limit = limit_rows_per_key,
        .state_bytes_limit = limit_bytes_per_key,
    });

    auto block = generateBlock(current_row_state, lazy_output, properties);
    if (is_last)
        scattered_block.reset();

    return {std::move(block), is_last && !current_row_state.has_value()};
}

}
