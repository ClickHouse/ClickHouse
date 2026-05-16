#include <Processors/TTL/ITTLAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Common/DateLUTImpl.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>

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

    /// `Date`/`DateTime` source columns are widened to `Date32`/`DateTime64` at TTL
    /// analysis time so the arithmetic in the expression cannot silently 16/32-bit wrap
    /// on overflow. The block here still holds the original narrow types, so cast each
    /// required input to the type the expression expects; for matching types this is a
    /// cheap no-op handled inside `castColumn`.
    Block block_copy;
    for (const auto & required : expression->getRequiredColumnsWithTypes())
    {
        auto block_col = block.getByName(required.name);
        if (!block_col.type->equals(*required.type))
        {
            block_col.column = castColumn(block_col, required.type);
            block_col.type = required.type;
        }
        block_copy.insert(std::move(block_col));
    }

    /// Keep number of rows for const expression.
    size_t num_rows = block.rows();
    expression->execute(block_copy, num_rows);

    return block_copy.getByName(result_column).column->convertToFullColumnIfSparse();
}

/// TODO: This per-row type dispatch is inefficient when called in a loop.
/// Callers should resolve the column type once and iterate over typed data directly.
/// See TTLDeleteFilterTransform::extractTimestamps for a batch-oriented approach.
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
