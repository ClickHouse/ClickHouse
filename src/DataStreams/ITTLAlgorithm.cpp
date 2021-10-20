#include <DataStreams/ITTLAlgorithm.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ITTLAlgorithm::ITTLAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : description(description_)
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

UInt32 ITTLAlgorithm::getTimestampByIndex(const IColumn * column, size_t index) const
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[index]));
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[index];
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
