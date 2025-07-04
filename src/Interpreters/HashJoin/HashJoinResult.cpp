#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/castColumn.h>

namespace DB
{

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

IJoinResult::JoinResultBlock HashJoinResult::next()
{
    if (!scattered_block)
        return {};

    if (is_join_get)
        lazy_output.buildJoinGetOutput();
    else
        lazy_output.buildOutput();

    if (need_filter)
        scattered_block->filter(lazy_output.filter);

    scattered_block->filterBySelector();

    auto block = std::move(*scattered_block).getSourceBlock();
    appendRightColumns(
        block,
        join,
        std::move(lazy_output.columns),
        lazy_output.type_name,
        std::move(lazy_output.filter),
        std::move(lazy_output.offsets_to_replicate),
        need_filter,
        is_asof_join);
    return {block, true};
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
        const auto * filter_ptr = (need_filter || filter.empty()) ? nullptr : &filter;
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
