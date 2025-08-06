#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/castColumn.h>
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
        for (size_t i = 0; i < existing_columns; ++i)
            columns_to_replicate[i] = columns_to_replicate[i]->replicate(offsets);
        for (size_t pos : right_keys_to_replicate)
            columns_to_replicate[pos] = columns_to_replicate[pos]->replicate(offsets);

        block.setColumns(columns_to_replicate);
    }

    block.erase(block_columns_to_erase);
}

static Block generateBlock(
    ScatteredBlock scattered_block,
    const LazyOutput & lazy_output,
    size_t rows_to_reserve,
    size_t row_ref_begin,
    size_t row_ref_end,
    MutableColumns columns,
    const HashJoinResult::Properties & properties,
    const IColumn::Offsets & offsets,
    const IColumn::Filter & filter)
{
    const auto * off_data = lazy_output.row_refs.data();
    if (properties.is_join_get)
        lazy_output.buildJoinGetOutput(
            rows_to_reserve, columns,
            off_data + row_ref_begin, off_data + row_ref_end);
    else
        lazy_output.buildOutput(
            rows_to_reserve, columns,
            off_data + row_ref_begin, off_data + row_ref_end);

    /// Note: need_filter flag cannot be replaced with !added_columns.need_filter.empty()
    /// This is because e.g. for ALL LEFT JOIN filter is used to replace non-matched right keys to defaults.
    /// TODO: Technically, filter can be restored from the offsets.
    //        We can check if this faster vs building filter in main join loop.
    if (properties.need_filter)
        scattered_block.filter(filter);

    scattered_block.filterBySelector();

    auto block = std::move(scattered_block).getSourceBlock();
    appendRightColumns(
        block,
        std::move(columns),
        offsets,
        filter,
        lazy_output.type_name,
        properties);

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
    ScatteredBlock && block_,
    Properties properties_)
    : lazy_output(std::move(lazy_output_))
    , properties(std::move(properties_))
    , scattered_block(std::move(block_))
    , columns(std::move(columns_))
    , offsets(std::move(offsets_))
    , filter(std::move(filter_))
{
}

static size_t getAvgBytesPerRow(const Block & block)
{
    return block.allocatedBytes() / std::max<size_t>(1, block.rows());
}

IJoinResult::JoinResultBlock HashJoinResult::next()
{
    if (!scattered_block)
        return {};

    size_t avg_bytes_per_row = properties.avg_joined_bytes_per_row + getAvgBytesPerRow(scattered_block->getSourceBlock());
    auto num_lhs_rows = numLeftRowsForNextBlock(next_row, offsets, properties.max_joined_block_rows, properties.max_joined_block_bytes, avg_bytes_per_row);
    if (num_lhs_rows == 0 || (next_row == 0 && num_lhs_rows >= scattered_block->rows()))
    {
        auto block = generateBlock(
            std::move(*scattered_block),
            lazy_output,
            lazy_output.row_count,
            0,
            lazy_output.row_refs.size(),
            std::move(columns),
            properties,
            offsets,
            filter);

        scattered_block.reset();
        return {std::move(block), true};
    }

    const size_t prev_offset = next_row ? offsets[next_row - 1] : 0;
    size_t num_rhs_rows = offsets[next_row + num_lhs_rows - 1] - prev_offset;

    auto current_block = std::move(*scattered_block);
    scattered_block = current_block.cut(num_lhs_rows);

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
    if (!filter.empty())
    {
        partial_filter.resize(num_lhs_rows);
        memcpySmallAllowReadWriteOverflow15(partial_filter.data(), filter.data() + next_row, num_lhs_rows);
    }

    const auto row_ref_start = next_row_ref;

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

    MutableColumns next_columns;
    if (!is_last)
    {
        next_columns.reserve(columns.size());
        for (auto & column : columns)
            next_columns.push_back(column->cloneEmpty());
    }

    auto block = generateBlock(
        std::move(current_block),
        lazy_output,
        num_rhs_rows,
        row_ref_start,
        next_row_ref,
        std::move(columns),
        properties,
        partial_offsets,
        partial_filter);

    columns = std::move(next_columns);
    if (is_last)
        scattered_block.reset();

    return {std::move(block), is_last};
}

}
