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



template <typename ValueType>
bool tryExpandMaskColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8> & mask, bool reverse, UInt8 default_value_for_expanding_mask)
{
    if (const auto * col = checkAndGetColumn<ColumnVector<ValueType>>(*column))
    {
        expandDataByMask<ValueType>(const_cast<ColumnVector<ValueType> *>(col)->getData(), mask, reverse, default_value_for_expanding_mask);
        return true;
    }

    return false;
}

void expandMaskColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, bool reverse, UInt8 default_value_for_expanding_mask)
{
    if (const auto * col = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        expandMaskColumnByMask(col->getNullMapColumnPtr(), mask, reverse, 0);
        expandMaskColumnByMask(col->getNestedColumnPtr(), mask, reverse, default_value_for_expanding_mask);
        return;
    }

    if (!tryExpandMaskColumnByMask<Int8>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<Int16>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<Int32>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<Int64>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<UInt8>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<UInt16>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<UInt32>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<UInt64>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<Float32>(column, mask, reverse, default_value_for_expanding_mask) &&
        !tryExpandMaskColumnByMask<Float64>(column, mask, reverse, default_value_for_expanding_mask))
        throw Exception("Cannot convert column " + column.get()->getName() + " to mask", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void expandColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, bool reverse)
{
    column->assumeMutable()->expand(mask, reverse);
}

template <typename ValueType>
void copyMaskImpl(const PaddedPODArray<ValueType>& mask, PaddedPODArray<UInt8> & res, bool reverse, const PaddedPODArray<UInt8> * null_bytemap, UInt8 null_value)
{
    if (res.size() != mask.size())
        res.resize(mask.size());

    for (size_t i = 0; i != mask.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
            res[i] = reverse ? !null_value : null_value;
        else
            res[i] = reverse ? !mask[i]: !!mask[i];
    }
}

template <typename ValueType>
bool tryGetMaskFromColumn(const ColumnPtr column, PaddedPODArray<UInt8> & res, bool reverse, const PaddedPODArray<UInt8> * null_bytemap, UInt8 null_value)
{
    if (const auto * col = checkAndGetColumn<ColumnVector<ValueType>>(*column))
    {
        copyMaskImpl(col->getData(), res, reverse, null_bytemap, null_value);
        return true;
    }

    return false;
}

void getMaskFromColumn(const ColumnPtr & column, PaddedPODArray<UInt8> & res, bool reverse, const PaddedPODArray<UInt8> * null_bytemap, UInt8 null_value)
{
    if (const auto * col = checkAndGetColumn<ColumnConst>(*column))
    {
        getMaskFromColumn(col->convertToFullColumn(), res, reverse, null_bytemap, null_value);
        return;
    }

    if (const auto * col = checkAndGetColumn<ColumnNothing>(*column))
    {
        res.resize_fill(col->size(), reverse ? !null_value : null_value);
        return;
    }

    if (const auto * col = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = checkAndGetColumn<ColumnUInt8>(*col->getNullMapColumnPtr())->getData();
        return getMaskFromColumn(col->getNestedColumnPtr(), res, reverse, &null_map, null_value);
    }

    if (const auto * col = checkAndGetColumn<ColumnLowCardinality>(*column))
        return getMaskFromColumn(col->convertToFullColumn(), res, reverse, null_bytemap, null_value);

    if (!tryGetMaskFromColumn<Int8>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<Int16>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<Int32>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<Int64>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<UInt8>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<UInt16>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<UInt32>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<UInt64>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<Float32>(column, res, reverse, null_bytemap, null_value) &&
        !tryGetMaskFromColumn<Float64>(column, res, reverse, null_bytemap, null_value))
        throw Exception("Cannot convert column " + column.get()->getName() + " to mask", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, bool reverse, const UInt8 * default_value_for_expanding_mask)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function)
        return;

    auto filtered = column_function->filter(mask, -1, reverse);
    auto result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce(true);
    if (default_value_for_expanding_mask)
    {
        result.column = result.column->convertToFullColumnIfLowCardinality();
        result.column = result.column->convertToFullColumnIfConst();
        expandMaskColumnByMask(result.column, mask, reverse, *default_value_for_expanding_mask);
    }
    else
        expandColumnByMask(result.column, mask, reverse);
    column = std::move(result);
}

void executeColumnIfNeeded(ColumnWithTypeAndName & column)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function)
        return;

    column = typeid_cast<const ColumnFunction *>(column_function)->reduce(true);
}

bool checkArgumentsForColumnFunction(const ColumnsWithTypeAndName & arguments)
{
    for (const auto & arg : arguments)
    {
        if (const auto * col = checkAndGetColumn<ColumnFunction>(*arg.column))
            return true;
    }
    return false;
}

}
