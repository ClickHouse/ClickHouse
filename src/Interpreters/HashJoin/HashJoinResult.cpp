#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/castColumn.h>

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

template <typename T>
PaddedPODArray<T> cut(PaddedPODArray<T> & from, size_t num_rows, const char * msg)
{
    if (from.empty())
        return {};

    // LOG_TRACE(getLogger("cut"), "cutting {} rows from {} rows ({})", num_rows, from.size(), msg);

    if (from.size() < num_rows)
    {
        // LOG_TRACE(getLogger("cut"), "!!!!!!!!!!!!!!!!!!!!!!!!!");
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: expected {} rows, got {}", msg, num_rows, from.size());
    }

    PaddedPODArray<T> result(from.begin() + num_rows, from.end());
    from.resize(num_rows);

    // LOG_TRACE(getLogger("cut"), "result ({}): {} {} rows", msg, from.size(), result.size());
    return result;
}

HashJoinResult::HashJoinResult(
    LazyOutput && lazy_output_,
    bool need_filter_,
    bool is_join_get_,
    bool is_asof_join_,
    ScatteredBlock && block_,
    const HashJoin * join_)
    : lazy_output(std::move(lazy_output_))
    , join(join_)
    , need_filter(need_filter_)
    , is_join_get(is_join_get_)
    , is_asof_join(is_asof_join_)
    , scattered_block(std::move(block_))
{
    if (!lazy_output.offsets_to_replicate.empty())
    {
        auto rows_count = lazy_output.offsets_to_replicate.back();
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
    MutableColumns columns;
    PaddedPODArray<UInt64> row_refs;
    IColumn::Offsets offsets_to_replicate;
    IColumn::Filter filter;
    size_t remaining_rows = 0;
    bool is_last = true;

    size_t rows_count = 0;
    if (!lazy_output.offsets_to_replicate.empty())
    {
        rows_count = lazy_output.offsets_to_replicate.back();
        if (lazy_output.row_count)
        {
            if (rows_count != lazy_output.row_count)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 1 {} {}", rows_count, lazy_output.row_count);
        }
        if (!lazy_output.row_refs.empty())
        {
            if (!lazy_output.output_by_row_list && rows_count != lazy_output.row_refs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 2 {} {}", rows_count, lazy_output.row_refs.size());
            // if (lazy_output.output_by_row_list && lazy_output.row_refs.size() != lazy_output.offsets_to_replicate.size())
            //     throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 3 {} {}", lazy_output.row_refs.size(), lazy_output.offsets_to_replicate.size());
        }
    }
    if (join->max_joined_block_rows && rows_count > join->max_joined_block_rows)
    {
        size_t num_lhs_rows = 0;
        size_t num_rhs_rows = 0;
        for (auto offset : lazy_output.offsets_to_replicate)
        {
            if (num_lhs_rows && offset > join->max_joined_block_rows)
                break;
            num_rhs_rows = offset;
            ++num_lhs_rows;
        }

        size_t num_refs_to_cut = num_lhs_rows;
        if (lazy_output.output_by_row_list && !lazy_output.row_refs.empty())
        {
            num_refs_to_cut = 0;
            size_t num_joined = 0;
            for (auto ref : lazy_output.row_refs)
            {
                if (num_joined >= num_rhs_rows)
                    break;
                ++num_refs_to_cut;
                if (const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(ref))
                    num_joined += row_ref_list->rows;
                else
                    ++num_joined;
            }

            if (num_joined != num_rhs_rows)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 7 {} {}", num_joined, num_rhs_rows);
        }

        is_last = false;

        scattered_block = current_block->cut(num_lhs_rows);
        offsets_to_replicate = cut(lazy_output.offsets_to_replicate, num_lhs_rows, "offsets_to_replicate");
        for (auto & off : offsets_to_replicate)
            off -= lazy_output.offsets_to_replicate.back();
        filter = cut(lazy_output.filter, num_lhs_rows, "filter");
        row_refs = cut(lazy_output.row_refs, num_refs_to_cut, "row_refs");
        if (lazy_output.row_count)
        {
            remaining_rows = lazy_output.row_count - num_rhs_rows;
            lazy_output.row_count = num_rhs_rows;
        }

        //std::cerr << lazy_output.offsets_to_replicate.back() << ' ' << num_rhs_rows << std::endl;

        columns.reserve(lazy_output.columns.size());
        for (auto & column : lazy_output.columns)
            columns.push_back(column->cloneEmpty());
    }

    if (is_join_get)
        lazy_output.buildJoinGetOutput();
    else
        lazy_output.buildOutput();

    if (need_filter)
    {
        // if (!current_block->getSourceBlock())
        //     throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter is not empty but block is empty {}", lazy_output.filter.size());

        // if (current_block->rows() != lazy_output.filter.size())
        //     throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter size {} does not match block size {}", lazy_output.filter.size(), current_block->rows());

        current_block->filter(lazy_output.filter);
    }

    current_block->filterBySelector();

    auto block = std::move(*current_block).getSourceBlock();
    appendRightColumns(
        block,
        join,
        std::move(lazy_output.columns),
        lazy_output.type_name,
        std::move(lazy_output.filter),
        std::move(lazy_output.offsets_to_replicate),
        need_filter,
        is_asof_join);

    if (!is_last)
    {
        lazy_output.filter = std::move(filter);
        lazy_output.offsets_to_replicate = std::move(offsets_to_replicate);
        lazy_output.row_refs = std::move(row_refs);
        lazy_output.row_count = remaining_rows;
        lazy_output.columns = std::move(columns);

        // if (!lazy_output.offsets_to_replicate.empty())
        // {
        //     rows_count = lazy_output.offsets_to_replicate.back();
        //     if (lazy_output.row_count)
        //     {
        //         if (rows_count != lazy_output.row_count)
        //             throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 4 {} {}", rows_count, lazy_output.row_count);
        //     }
        //     if (!lazy_output.row_refs.empty())
        //     {
        //         if (!lazy_output.output_by_row_list && rows_count != lazy_output.row_refs.size())
        //             throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 5 {} {}", rows_count, lazy_output.row_refs.size());
        //         if (lazy_output.output_by_row_list && lazy_output.row_refs.size() != lazy_output.offsets_to_replicate.size())
        //             throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch 6 {} {}", lazy_output.row_refs.size(), lazy_output.offsets_to_replicate.size());
        //     }
        // }
    }

    return {std::move(block), is_last};
}

void HashJoinResult::appendRightColumns(
    Block & block,
    const HashJoin * join,
    MutableColumns columns,
    const NamesAndTypes & type_name,
    IColumn::Filter filter,
    IColumn::Offsets offsets_to_replicate,
    bool need_filter,
    bool is_asof_join)
{
    auto & table_join = *join->table_join;
    size_t existing_columns = block.columns();

    std::set<size_t> block_columns_to_erase;
    if (join->canRemoveColumnsFromLeftBlock())
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

    std::vector<size_t> right_keys_to_replicate;

    /// Add join key columns from right block if needed.
    for (size_t i = 0; i < join->required_right_keys.columns(); ++i)
    {
        const auto & right_key = join->required_right_keys.getByPosition(i);
        /// asof column is already in block.
        if (is_asof_join && right_key.name == table_join.getOnlyClause().key_names_right.back())
            continue;

        const auto & left_column = block.getByName(join->required_right_keys_sources[i]);
        const auto & right_col_name = table_join.renamedRightColumnName(right_key.name);
        const auto * filter_ptr = need_filter ? nullptr : &filter; //(need_filter || filter.empty()) ? nullptr : &filter;
        auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column, filter_ptr);
        block.insert(std::move(right_col));

        if (!offsets_to_replicate.empty())
            right_keys_to_replicate.push_back(block.getPositionByName(right_col_name));
    }

    if (!offsets_to_replicate.empty())
    {
        IColumn::Offsets & offsets = offsets_to_replicate;

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

}
