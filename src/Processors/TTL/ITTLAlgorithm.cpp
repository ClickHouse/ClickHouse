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
    {
        /// When the TTL expression is a bare source column (e.g. `TTL ts`), the block holds
        /// it in the original type, which may differ from the type the analyzed expression
        /// produces: the analysis widens `Date`/`DateTime` sources to `Date32`/`DateTime64`
        /// and looks through `LowCardinality`. Cast to the expression's result type so the
        /// consumers see the same column type in both paths (a no-op for matching types).
        const auto & ttl_column = block.getByName(result_column);
        const auto & expected_type = expression->getSampleBlock().getByName(result_column).type;
        if (!ttl_column.type->equals(*expected_type))
            return castColumn(ttl_column, expected_type)->convertToFullColumnIfSparse();
        return ttl_column.column->convertToFullColumnIfSparse();
    }

    /// `Date`/`DateTime` source columns are widened to `Date32`/`DateTime64` at TTL
    /// analysis time so the arithmetic in the expression cannot silently 16/32-bit wrap
    /// on overflow. The block here still holds the original narrow types, so cast each
    /// required input to the type the expression expects; for matching types this is a
    /// cheap no-op handled inside `castColumn`.
    Block block_copy;
    for (const auto & required : expression->getRequiredColumnsWithTypes())
    {
        auto block_col = block.getColumnOrSubcolumnByName(required.name);
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

/// Shared by every TTL algorithm and by `TTLDeleteFilterTransform`, so a new result type is added
/// once for all of them. Not shared with the insert path: `MergeTreeDataWriter::updateTTLInfo` and
/// `updateTTLInfoConst` fold the same types into `MergeTreeDataPartTTLInfo` without building an
/// array, and still need the new type too.
void ITTLAlgorithm::extractTimestamps(
    const IColumn * column, const DateLUTImpl & date_lut, PaddedPODArray<Int64> & timestamps)
{
    const IColumn * ttl_column = column;
    const size_t num_rows = column->size();
    timestamps.resize_exact(num_rows);

    /// Sparse columns must be converted to dense before type dispatch, since typeid_cast does not
    /// see through the ColumnSparse wrapper. Unreachable in practice - every caller gets its column
    /// from executeExpressionAndGetColumn, which converts - but kept so the mapping stays total.
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
        /// A constant column has one value for the whole block, so the inner type is
        /// resolved and converted once rather than per row.
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


}
