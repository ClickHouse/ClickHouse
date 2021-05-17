#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool reverse, T default_value)
{
    if (mask.size() < data.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int from = data.size() - 1;
    int index = mask.size() - 1;
    data.resize(mask.size());
    while (index >= 0)
    {
        if (mask[index] ^ reverse)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            data[index] = data[from];
            --from;
        }
        else
            data[index] = default_value;

        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);

}

/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(TYPE) \
template void expandDataByMask<TYPE>(PaddedPODArray<TYPE> &, const PaddedPODArray<UInt8> &, bool, TYPE);

INSTANTIATE(UInt8)
INSTANTIATE(UInt16)
INSTANTIATE(UInt32)
INSTANTIATE(UInt64)
INSTANTIATE(UInt128)
INSTANTIATE(UInt256)
INSTANTIATE(Int8)
INSTANTIATE(Int16)
INSTANTIATE(Int32)
INSTANTIATE(Int64)
INSTANTIATE(Int128)
INSTANTIATE(Int256)
INSTANTIATE(Float32)
INSTANTIATE(Float64)
INSTANTIATE(Decimal32)
INSTANTIATE(Decimal64)
INSTANTIATE(Decimal128)
INSTANTIATE(Decimal256)
INSTANTIATE(DateTime64)
INSTANTIATE(char *)
INSTANTIATE(UUID)

#undef INSTANTIATE

void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool reverse)
{
    if (mask.size() < offsets.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int index = mask.size() - 1;
    int from = offsets.size() - 1;
    offsets.resize(mask.size());
    UInt64 prev_offset = offsets[from];
    while (index >= 0)
    {
        if (mask[index] ^ reverse)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            offsets[index] = offsets[from];
            --from;
            prev_offset = offsets[from];
        }
        else
            offsets[index] = prev_offset;
        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);
}

void expandColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, bool reverse)
{
    column->assumeMutable()->expand(mask, reverse);
}

void getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool reverse,
    const PaddedPODArray<UInt8> * expanding_mask,
    UInt8 default_value,
    bool expanding_mask_reverse,
    const PaddedPODArray<UInt8> * null_bytemap,
    UInt8 null_value)
{
    if (const auto * col = checkAndGetColumn<ColumnNothing>(*column))
    {
        res.resize_fill(col->size(), reverse ? !null_value : null_value);
        return;
    }

    if (const auto * col = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = checkAndGetColumn<ColumnUInt8>(*col->getNullMapColumnPtr())->getData();
        return getMaskFromColumn(col->getNestedColumnPtr(), res, reverse, expanding_mask, default_value, expanding_mask_reverse, &null_map, null_value);
    }

    try
    {
        if (res.size() != column->size())
            res.resize(column->size());

        for (size_t i = 0; i != column->size(); ++i)
        {
            if (expanding_mask && (!(*expanding_mask)[i] ^ expanding_mask_reverse))
                res[i] = reverse ? !default_value : default_value;
            else if (null_bytemap && (*null_bytemap)[i])
                res[i] = reverse ? !null_value : null_value;
            else
                res[i] = reverse ? !column->getBool(i): column->getBool(i);
        }
    }
    catch (...)
    {
        throw Exception("Cannot convert column " + column.get()->getName() + " to mask", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

template <typename Op>
void binaryMasksOperationImpl(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2, Op operation)
{
    if (mask1.size() != mask2.size())
        throw Exception("Masks have different sizes", ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i != mask1.size(); ++i)
        mask1[i] = operation(mask1[i], mask2[i]);
}

void conjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2)
{
    binaryMasksOperationImpl(mask1, mask2, [](const auto & lhs, const auto & rhs){ return lhs & rhs; });
}

void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2)
{
    binaryMasksOperationImpl(mask1, mask2, [](const auto & lhs, const auto & rhs){ return lhs | rhs; });
}

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, bool reverse)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function || !column_function->isShortCircuitArgument())
        return;

    auto filtered = column_function->filter(mask, -1, reverse);
    auto result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce();
    expandColumnByMask(result.column, mask, reverse);
    column = std::move(result);
}

void executeColumnIfNeeded(ColumnWithTypeAndName & column)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function || !column_function->isShortCircuitArgument())
        return;

    column = typeid_cast<const ColumnFunction *>(column_function)->reduce();
}


int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments)
{
    int last_short_circuit_argument_index = -1;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        const auto * column_func = checkAndGetColumn<ColumnFunction>(*arguments[i].column);
        if (column_func && column_func->isShortCircuitArgument())
            last_short_circuit_argument_index = i;
    }

    return last_short_circuit_argument_index;
}

}
