#include <Processors/TTL/ITTLAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
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
        return block.getByName(result_column).column->convertToFullColumnIfSparse();

    Block block_copy;
    for (const auto & column_name : expression->getRequiredColumns())
        block_copy.insert(block.getColumnOrSubcolumnByName(column_name));

    /// Keep number of rows for const expression.
    size_t num_rows = block.rows();
    expression->execute(block_copy, num_rows);

    return block_copy.getByName(result_column).column->convertToFullColumnIfSparse();
}

/// The block-at-a-time half of the mapping from a TTL result column to Unix timestamps.
/// `getTimestampByIndex` below is the row-at-a-time half; the two must agree, so a new TTL result
/// type has to be added to both. They deliberately differ in one place - see the sparse branches.
void ITTLAlgorithm::extractTimestamps(
    const IColumn * column, size_t num_rows, const DateLUTImpl & date_lut, PaddedPODArray<Int64> & timestamps)
{
    const IColumn * ttl_column = column;
    timestamps.resize_exact(num_rows);

    /// Sparse columns must be converted to dense before type dispatch, since typeid_cast does not
    /// see through the ColumnSparse wrapper. Materializing once is the right trade here; the row-at-a-time
    /// version instead binary-searches per row, because materializing there would be O(N) per row.
    ColumnPtr dense;
    if (typeid_cast<const ColumnSparse *>(ttl_column))
    {
        dense = ttl_column->convertToFullColumnIfSparse();
        ttl_column = dense.get();
    }

    if (const auto * col_date = typeid_cast<const ColumnUInt16 *>(ttl_column))
    {
        const auto & data = col_date->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = date_lut.fromDayNum(DayNum(data[i]));
    }
    else if (const auto * col_datetime = typeid_cast<const ColumnUInt32 *>(ttl_column))
    {
        const auto & data = col_datetime->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = static_cast<Int64>(data[i]);
    }
    else if (const auto * col_date32 = typeid_cast<const ColumnInt32 *>(ttl_column))
    {
        const auto & data = col_date32->getData();
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = date_lut.fromDayNum(ExtendedDayNum(data[i]));
    }
    else if (const auto * col_datetime64 = typeid_cast<const ColumnDateTime64 *>(ttl_column))
    {
        const auto & data = col_datetime64->getData();
        const auto scale = intExp10OfSize<Int64>(col_datetime64->getScale());
        for (size_t i = 0; i < num_rows; ++i)
            timestamps[i] = data[i] / scale;
    }
    else if (const auto * col_const = typeid_cast<const ColumnConst *>(ttl_column))
    {
        /// Same inner-type dispatch as ITTLAlgorithm::getTimestampByIndex,
        /// but only executed once for the constant value.
        const auto & inner = col_const->getDataColumn();
        Int64 value = 0;
        if (typeid_cast<const ColumnUInt16 *>(&inner))
            value = date_lut.fromDayNum(DayNum(col_const->getValue<UInt16>()));
        else if (typeid_cast<const ColumnUInt32 *>(&inner))
            value = col_const->getValue<UInt32>();
        else if (typeid_cast<const ColumnInt32 *>(&inner))
            value = date_lut.fromDayNum(ExtendedDayNum(col_const->getValue<Int32>()));
        else if (const auto * inner_dt64 = typeid_cast<const ColumnDateTime64 *>(&inner))
            value = col_const->getValue<DateTime64>() / intExp10OfSize<Int64>(inner_dt64->getScale());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of inner column in constant TTL column");

        std::fill(timestamps.begin(), timestamps.end(), value);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of result TTL column");
    }
}

/// The row-at-a-time half; see `extractTimestamps` above. Fine for an algorithm that touches one row
/// at a time, but a loop over a whole block should call `extractTimestamps` instead - the type is
/// resolved once there rather than once per row.
Int64 ITTLAlgorithm::getTimestampByIndex(const IColumn * column, size_t index) const
{
    /// Sparse columns must be unwrapped before type dispatch, since
    /// typeid_cast does not see through the ColumnSparse wrapper.
    /// Use getValueIndex (binary-search, O(log N)) to avoid O(N²) full conversion.
    if (const auto * col_sparse = typeid_cast<const ColumnSparse *>(column))
        return getTimestampByIndex(&col_sparse->getValuesColumn(), col_sparse->getValueIndex(index));

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
