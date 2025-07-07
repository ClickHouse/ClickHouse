#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/castColumn.h>
#include <Common/memcpySmall.h>
#include "Core/Joins.h"

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
    //const HashJoin * join,
    TableJoin & table_join,
    bool can_remove_columns_from_left_block,
    const Block & required_right_keys,
    const std::vector<String> & required_right_keys_sources,
    HashJoinResult::Data data,
    const NamesAndTypes & type_name,
    bool need_filter,
    bool is_asof_join)
{
    size_t existing_columns = block.columns();

    std::set<size_t> block_columns_to_erase;
    if (can_remove_columns_from_left_block)
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

    for (size_t i = 0; i < data.columns.size(); ++i)
    {
        ColumnWithTypeAndName col;
        col.column = std::move(data.columns[i]);
        col.name = table_join.renamedRightColumnName(type_name[i].name);
        col.type = type_name[i].type;
        block.insert(std::move(col));
    }

    std::vector<size_t> right_keys_to_replicate;

    /// Add join key columns from right block if needed.
    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        /// asof column is already in block.
        if (is_asof_join && right_key.name == table_join.getOnlyClause().key_names_right.back())
            continue;

        const auto & left_column = block.getByName(required_right_keys_sources[i]);
        const auto & right_col_name = table_join.renamedRightColumnName(right_key.name);
        const auto * filter_ptr = need_filter ? nullptr : &data.filter;
        auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column, filter_ptr);
        block.insert(std::move(right_col));

        if (!data.offsets_to_replicate.empty())
            right_keys_to_replicate.push_back(block.getPositionByName(right_col_name));
    }

    if (!data.offsets_to_replicate.empty())
    {
        IColumn::Offsets & offsets = data.offsets_to_replicate;

        chassert(block);
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

HashJoinResult::HashJoinResult(
    LazyOutput && lazy_output_,
    MutableColumns columns_,
    IColumn::Offsets offsets_to_replicate_,
    IColumn::Filter filter_,
    bool need_filter_,
    bool is_join_get_,
    bool is_asof_join_,
    ScatteredBlock && block_,
    const HashJoin * join_)
    : lazy_output(std::move(lazy_output_))
    , scattered_block(std::move(block_))
    , data({std::move(columns_), std::move(offsets_to_replicate_), std::move(filter_)})
    , join(join_)
    , need_filter(need_filter_)
    , is_join_get(is_join_get_)
    , is_asof_join(is_asof_join_)
{
    num_rows_to_join = lazy_output.row_count;
    if (!data.offsets_to_replicate.empty())
    {
        auto rows_count = data.offsets_to_replicate.back();
        if (lazy_output.row_count)
        {
            if (rows_count != lazy_output.row_count)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Init Row count mismatch 1 {} {}", rows_count, lazy_output.row_count);
        }
        if (!lazy_output.row_refs.empty())
        {
            if (!lazy_output.output_by_row_list && rows_count != lazy_output.row_refs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Init Row count mismatch 2 {} {}", rows_count, lazy_output.row_refs.size());
            // if (lazy_output.output_by_row_list && lazy_output.row_refs.size() != lazy_output.offsets_to_replicate.size())
            //     throw Exception(ErrorCodes::LOGICAL_ERROR, "Init Row count mismatch 3 {} {}", lazy_output.row_refs.size(), lazy_output.offsets_to_replicate.size());
        }
    }
}

IJoinResult::JoinResultBlock HashJoinResult::next()
{
    if (!scattered_block)
        return {};

    auto current_block = std::move(scattered_block);
    auto current_data = std::move(data);
    size_t rows_to_reserve = num_rows_to_join;
    bool is_last = true;

    size_t row_ref_begin = next_row_ref;
    size_t row_ref_end = lazy_output.row_refs.size();

    size_t rows_count = 0;
    if (!current_data.offsets_to_replicate.empty())
        rows_count = current_data.offsets_to_replicate.back();

    if (join->max_joined_block_rows && rows_count > join->max_joined_block_rows)
    {
        size_t prev_offset = 0;
        if (next_row)
            prev_offset = current_data.offsets_to_replicate[next_row - 1];

        size_t num_lhs_rows = 0;

        size_t prev_row_offset = prev_offset;
        size_t num_missing = 0;
        for (size_t i = next_row; i < current_data.offsets_to_replicate.size(); ++i)
        {
            auto offset = current_data.offsets_to_replicate[i];
            if (num_lhs_rows && offset > prev_offset + join->max_joined_block_rows)
                break;
            ++num_lhs_rows;
            if (offset == prev_row_offset)
                ++num_missing;
            prev_row_offset = offset;
        }

        is_last = num_lhs_rows + next_row >= current_data.offsets_to_replicate.size();
        size_t num_rhs_rows = prev_row_offset - prev_offset;
        size_t num_refs_to_cut = num_rhs_rows;

        if (lazy_output.output_by_row_list)
        {
            num_refs_to_cut = num_lhs_rows;
            bool add_missing = isLeftOrFull(join->table_join->kind()) && join->table_join->strictness() != JoinStrictness::Semi;
            if (!add_missing)
                num_refs_to_cut -= num_missing;
        }

        // if (lazy_output.output_by_row_list && !lazy_output.row_refs.empty())
        // {
        //     num_refs_to_cut = 0;
        //     size_t num_joined = 0;
        //     for (size_t i = next_row_ref; i < lazy_output.row_refs.size(); ++i)
        //     {
        //         if (num_joined >= num_rhs_rows)
        //             break;
        //         ++num_refs_to_cut;
        //         if (const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(lazy_output.row_refs[i]))
        //             num_joined += row_ref_list->rows;
        //         else
        //             ++num_joined;
        //     }

        //     if (num_joined != num_rhs_rows)
        //         throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 7 {} {}", num_joined, num_rhs_rows);
        // }

        scattered_block = current_block->cut(num_lhs_rows);
        data = std::move(current_data);

        current_data.offsets_to_replicate.resize(num_lhs_rows);
        for (size_t row = 0; row < num_lhs_rows; ++row)
            current_data.offsets_to_replicate[row] = data.offsets_to_replicate[row + next_row] - prev_offset;

        if (!data.filter.empty())
        {
            current_data.filter.resize(num_lhs_rows);
            memcpySmallAllowReadWriteOverflow15(current_data.filter.data(), data.filter.data() + next_row, num_lhs_rows);
        }

        next_row += num_lhs_rows;
        next_row_ref += num_refs_to_cut;
        row_ref_end = next_row_ref;

        if (row_ref_end > lazy_output.row_refs.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The number of rows {} is less than expected rest number of rows {}",
                lazy_output.row_refs.size(), row_ref_end);

        if (rows_to_reserve < num_rhs_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The number or joined rows {} is less than expected rest number of rows {}",
                rows_to_reserve, num_rhs_rows);
        rows_to_reserve -= num_rhs_rows;

        current_data.columns.reserve(data.columns.size());
        for (auto & column : data.columns)
            current_data.columns.push_back(column->cloneEmpty());
    }

    // LOG_TRACE(getLogger("HashJoinResult"), "{} {} {} {}", row_ref_begin, row_ref_end, lazy_output.row_count, lazy_output.row_refs.size());

    const auto * off_data = lazy_output.row_refs.data();
    if (is_join_get)
        lazy_output.buildJoinGetOutput(
            rows_to_reserve, current_data.columns,
            off_data + row_ref_begin, off_data + row_ref_end);
    else
        lazy_output.buildOutput(
            rows_to_reserve, current_data.columns,
            off_data + row_ref_begin, off_data + row_ref_end);

    if (need_filter)
        current_block->filter(current_data.filter);

    current_block->filterBySelector();

    auto block = std::move(*current_block).getSourceBlock();
    appendRightColumns(
        block,
        *join->table_join,
        join->canRemoveColumnsFromLeftBlock(),
        join->required_right_keys,
        join->required_right_keys_sources,
        std::move(current_data),
        lazy_output.type_name,
        need_filter,
        is_asof_join);

    return {std::move(block), is_last};
}

}
