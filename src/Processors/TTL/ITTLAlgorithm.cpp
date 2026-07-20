#include <Processors/TTL/ITTLAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/DateLUTImpl.h>
#include <Interpreters/ExpressionActions.h>

#include <Columns/ColumnsDateTime.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ITTLAlgorithm::ITTLAlgorithm(
    const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ttl_expressions(ttl_expressions_)
    , description(description_)
    , old_ttl_info(old_ttl_info_)
    , current_time(current_time_)
    , force(force_)
    , date_lut(DateLUT::instance())
{
}

bool ITTLAlgorithm::isTTLExpired(time_t ttl) const
{
    return (ttl && (ttl <= current_time));
}

ColumnPtr ITTLAlgorithm::executeExpressionAndGetColumn(
    const ExpressionActionsPtr & expression, const Block & block, const String & result_column)
{
    if (!expression)
        return nullptr;

    if (block.has(result_column))
        return block.getByName(result_column).column;

    Block block_copy;
    for (const auto & column_name : expression->getRequiredColumns())
        block_copy.insert(block.getByName(column_name));

    /// Keep number of rows for const expression.
    size_t num_rows = block.rows();
    expression->execute(block_copy, num_rows);

    return block_copy.getByName(result_column).column;
}

Int64 ITTLAlgorithm::getTimestampByIndex(const IColumn * column, size_t index) const
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[index]));
    if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[index];
    if (const ColumnInt32 * column_date_32 = typeid_cast<const ColumnInt32 *>(column))
        return date_lut.fromDayNum(ExtendedDayNum(column_date_32->getData()[index]));
    if (const ColumnDateTime64 * column_date_time_64 = typeid_cast<const ColumnDateTime64 *>(column))
        return column_date_time_64->getData()[index] / intExp10OfSize<Int64>(column_date_time_64->getScale());
    if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(column))
    {
        if (typeid_cast<const ColumnUInt16 *>(&column_const->getDataColumn()))
            return date_lut.fromDayNum(DayNum(column_const->getValue<UInt16>()));
        if (typeid_cast<const ColumnUInt32 *>(&column_const->getDataColumn()))
            return column_const->getValue<UInt32>();
        if (typeid_cast<const ColumnInt32 *>(&column_const->getDataColumn()))
            return date_lut.fromDayNum(ExtendedDayNum(column_const->getValue<Int32>()));
        if (const ColumnDateTime64 * column_dt64 = typeid_cast<const ColumnDateTime64 *>(&column_const->getDataColumn()))
            return column_const->getValue<DateTime64>() / intExp10OfSize<Int64>(column_dt64->getScale());
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of result TTL column");
}

}
