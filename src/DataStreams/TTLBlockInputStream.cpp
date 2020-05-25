#include <DataStreams/TTLBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/addTypeConversionToAST.h>

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
    time_t current_time_,
    bool force_)
    : storage(storage_)
    , data_part(data_part_)
    , current_time(current_time_)
    , force(force_)
    , old_ttl_infos(data_part->ttl_infos)
    , log(&Logger::get(storage.getLogName() + " (TTLBlockInputStream)"))
    , date_lut(DateLUT::instance())
{
    children.push_back(input_);
    header = children.at(0)->getHeader();

    const auto & storage_columns = storage.getColumns();
    const auto & column_defaults = storage_columns.getDefaults();

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & [name, _] : storage.getColumnTTLs())
    {
        auto it = column_defaults.find(name);
        if (it != column_defaults.end())
        {
            auto column = storage_columns.get(name);
            auto expression = it->second.expression->clone();
            default_expr_list->children.emplace_back(setAlias(addTypeConversionToAST(std::move(expression), column.type->getName()), it->first));
        }
    }

    for (const auto & [name, ttl_info] : old_ttl_infos.columns_ttl)
    {
        if (force || isTTLExpired(ttl_info.min))
        {
            new_ttl_infos.columns_ttl.emplace(name, IMergeTreeDataPart::TTLInfo{});
            empty_columns.emplace(name);
        }
        else
            new_ttl_infos.columns_ttl.emplace(name, ttl_info);
    }

    if (!force && !isTTLExpired(old_ttl_infos.table_ttl.min))
        new_ttl_infos.table_ttl = old_ttl_infos.table_ttl;

    if (!default_expr_list->children.empty())
    {
        auto syntax_result = SyntaxAnalyzer(storage.global_context).analyze(
            default_expr_list, storage.getColumns().getAllPhysical());
        defaults_expression = ExpressionAnalyzer{default_expr_list, syntax_result, storage.global_context}.getActions(true);
    }
}

bool TTLBlockInputStream::isTTLExpired(time_t ttl) const
{
    return (ttl && (ttl <= current_time));
}

Block TTLBlockInputStream::readImpl()
{
    /// Skip all data if table ttl is expired for part
    if (storage.hasRowsTTL() && isTTLExpired(old_ttl_infos.table_ttl.max))
    {
        rows_removed = data_part->rows_count;
        return {};
    }

    Block block = children.at(0)->read();
    if (!block)
        return block;

    if (storage.hasRowsTTL() && (force || isTTLExpired(old_ttl_infos.table_ttl.min)))
        removeRowsWithExpiredTableTTL(block);

    removeValuesWithExpiredColumnTTL(block);

    updateMovesTTL(block);

    return block;
}

void TTLBlockInputStream::readSuffixImpl()
{
    for (const auto & elem : new_ttl_infos.columns_ttl)
        new_ttl_infos.updatePartMinMaxTTL(elem.second.min, elem.second.max);

    new_ttl_infos.updatePartMinMaxTTL(new_ttl_infos.table_ttl.min, new_ttl_infos.table_ttl.max);

    data_part->ttl_infos = std::move(new_ttl_infos);
    data_part->expired_columns = std::move(empty_columns);

    if (rows_removed)
        LOG_INFO(log, "Removed {} rows with expired TTL from part {}", rows_removed, data_part->name);
}

void TTLBlockInputStream::removeRowsWithExpiredTableTTL(Block & block)
{
    const auto & rows_ttl = storage.getRowsTTL();
    rows_ttl.expression->execute(block);

    const IColumn * ttl_column =
        block.getByName(rows_ttl.result_column).column.get();

    const auto & column_names = header.getNames();
    MutableColumns result_columns;
    result_columns.reserve(column_names.size());

    for (auto it = column_names.begin(); it != column_names.end(); ++it)
    {
        const IColumn * values_column = block.getByName(*it).column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            if (!isTTLExpired(cur_ttl))
            {
                new_ttl_infos.table_ttl.update(cur_ttl);
                result_column->insertFrom(*values_column, i);
            }
            else if (it == column_names.begin())
                ++rows_removed;
        }
        result_columns.emplace_back(std::move(result_column));
    }

    block = header.cloneWithColumns(std::move(result_columns));
}

void TTLBlockInputStream::removeValuesWithExpiredColumnTTL(Block & block)
{
    Block block_with_defaults;
    if (defaults_expression)
    {
        block_with_defaults = block;
        defaults_expression->execute(block_with_defaults);
    }

    std::vector<String> columns_to_remove;
    for (const auto & [name, ttl_entry] : storage.getColumnTTLs())
    {
        /// If we read not all table columns. E.g. while mutation.
        if (!block.has(name))
            continue;

        const auto & old_ttl_info = old_ttl_infos.columns_ttl[name];
        auto & new_ttl_info = new_ttl_infos.columns_ttl[name];

        /// Nothing to do
        if (!force && !isTTLExpired(old_ttl_info.min))
            continue;

        /// Later drop full column
        if (isTTLExpired(old_ttl_info.max))
            continue;

        if (!block.has(ttl_entry.result_column))
        {
            columns_to_remove.push_back(ttl_entry.result_column);
            ttl_entry.expression->execute(block);
        }

        ColumnPtr default_column = nullptr;
        if (block_with_defaults.has(name))
            default_column = block_with_defaults.getByName(name).column->convertToFullColumnIfConst();

        auto & column_with_type = block.getByName(name);
        const IColumn * values_column = column_with_type.column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        const IColumn * ttl_column = block.getByName(ttl_entry.result_column).column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            if (isTTLExpired(cur_ttl))
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

    for (const String & column : columns_to_remove)
        block.erase(column);
}

void TTLBlockInputStream::updateMovesTTL(Block & block)
{
    std::vector<String> columns_to_remove;
    for (const auto & ttl_entry : storage.getMoveTTLs())
    {
        auto & new_ttl_info = new_ttl_infos.moves_ttl[ttl_entry.result_column];

        if (!block.has(ttl_entry.result_column))
        {
            columns_to_remove.push_back(ttl_entry.result_column);
            ttl_entry.expression->execute(block);
        }

        const IColumn * ttl_column = block.getByName(ttl_entry.result_column).column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            new_ttl_info.update(cur_ttl);
        }
    }

    for (const String & column : columns_to_remove)
        block.erase(column);
}

UInt32 TTLBlockInputStream::getTimestampByIndex(const IColumn * column, size_t ind)
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[ind]));
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[ind];
    else if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(column))
    {
        if (typeid_cast<const ColumnUInt16 *>(&column_const->getDataColumn()))
            return date_lut.fromDayNum(DayNum(column_const->getValue<UInt16>()));
        else if (typeid_cast<const ColumnUInt32 *>(&column_const->getDataColumn()))
            return column_const->getValue<UInt32>();
    }

    throw Exception("Unexpected type of result TTL column", ErrorCodes::LOGICAL_ERROR);
}

}
