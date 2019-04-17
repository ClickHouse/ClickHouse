#include <DataStreams/TTLBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


TTLBlockInputStream::TTLBlockInputStream(
    const BlockInputStreamPtr & input_,
    const MergeTreeData & storage_,
    const MergeTreeData::MutableDataPartPtr & data_part_,
    time_t current_time_)
    : storage(storage_)
    , data_part(data_part_)
    , current_time(current_time_)
    , old_ttl_infos(data_part->ttl_infos)
    , log(&Logger::get(storage.getLogName() + " (TTLBlockInputStream)"))
    , date_lut(DateLUT::instance())
{
    children.push_back(input_);

    const auto & column_defaults = storage.getColumns().getDefaults();
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & [name, ttl_info] : old_ttl_infos.columns_ttl)
    {
        if (ttl_info.min <= current_time)
        {
            new_ttl_infos.columns_ttl.emplace(name, MergeTreeDataPart::TTLInfo{});
            empty_columns.emplace(name);

            auto it = column_defaults.find(name);

            if (it != column_defaults.end())
                default_expr_list->children.emplace_back(
                    setAlias(it->second.expression, it->first));
        }
        else
            new_ttl_infos.columns_ttl.emplace(name, ttl_info);
    }

    if (old_ttl_infos.table_ttl.min > current_time)
        new_ttl_infos.table_ttl = old_ttl_infos.table_ttl;

    if (!default_expr_list->children.empty())
    {
        auto syntax_result = SyntaxAnalyzer(storage.global_context).analyze(
            default_expr_list, storage.getColumns().getAllPhysical());
        defaults_expression = ExpressionAnalyzer{default_expr_list, syntax_result, storage.global_context}.getActions(true);
    }
}


Block TTLBlockInputStream::getHeader() const
{
    return children.at(0)->getHeader();
}

Block TTLBlockInputStream::readImpl()
{
    Block block = children.at(0)->read();
    if (!block)
        return block;

    if (storage.hasTableTTL())
    {
        /// Skip all data if table ttl is expired for part
        if (old_ttl_infos.table_ttl.max <= current_time)
        {
            rows_removed = data_part->rows_count;
            return {};
        }

        if (old_ttl_infos.table_ttl.min <= current_time)
            removeRowsWithExpiredTableTTL(block);
    }

    removeValuesWithExpiredColumnTTL(block);

    return block;
}

void TTLBlockInputStream::readSuffixImpl()
{
    for (const auto & elem : new_ttl_infos.columns_ttl)
        new_ttl_infos.updatePartMinTTL(elem.second.min);

    new_ttl_infos.updatePartMinTTL(new_ttl_infos.table_ttl.min);

    data_part->ttl_infos = std::move(new_ttl_infos);
    data_part->empty_columns = std::move(empty_columns);

    if (rows_removed)
        LOG_INFO(log, "Removed " << rows_removed << " rows with expired ttl from part " << data_part->name);
}

void TTLBlockInputStream::removeRowsWithExpiredTableTTL(Block & block)
{
    storage.ttl_table_entry.expression->execute(block);

    const auto & current = block.getByName(storage.ttl_table_entry.result_column);
    const IColumn * ttl_column = current.column.get();

    MutableColumns result_columns;
    result_columns.reserve(getHeader().columns());
    for (const auto & name : storage.getColumns().getNamesOfPhysical())
    {
        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            if (cur_ttl > current_time)
            {
                new_ttl_infos.table_ttl.update(cur_ttl);
                result_column->insertFrom(*values_column, i);
            }
            else
                ++rows_removed;
        }
        result_columns.emplace_back(std::move(result_column));
    }

    block = getHeader().cloneWithColumns(std::move(result_columns));
}

void TTLBlockInputStream::removeValuesWithExpiredColumnTTL(Block & block)
{
    Block block_with_defaults;
    if (defaults_expression)
    {
        block_with_defaults = block;
        defaults_expression->execute(block_with_defaults);
    }

    for (const auto & [name, ttl_entry] : storage.ttl_entries_by_name)
    {
        const auto & old_ttl_info = old_ttl_infos.columns_ttl[name];
        auto & new_ttl_info = new_ttl_infos.columns_ttl[name];

        if (old_ttl_info.min > current_time)
            continue;

        if (old_ttl_info.max <= current_time)
            continue;

        if (!block.has(ttl_entry.result_column))
            ttl_entry.expression->execute(block);

        ColumnPtr default_column = nullptr;
        if (block_with_defaults.has(name))
            default_column = block_with_defaults.getByName(name).column->convertToFullColumnIfConst();

        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        const auto & current = block.getByName(ttl_entry.result_column);
        const IColumn * ttl_column = current.column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);

            if (cur_ttl <= current_time)
            {
                if (default_column)
                    result_column->insertFrom(*default_column, i);
                else
                    result_column->insertDefault();
            }
            else
            {
                new_ttl_info.update(cur_ttl);
                empty_columns.erase(name);
                result_column->insertFrom(*values_column, i);
            }
        }
        column_with_type.column = std::move(result_column);
    }

    for (const auto & elem : storage.ttl_entries_by_name)
        if (block.has(elem.second.result_column))
            block.erase(elem.second.result_column);
}

UInt32 TTLBlockInputStream::getTimestampByIndex(const IColumn * column, size_t ind)
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[ind]));
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[ind];
    else
        throw Exception("Unexpected type of result ttl column", ErrorCodes::LOGICAL_ERROR);
}

}
