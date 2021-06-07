#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsCommon.h>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool inverted)
{
    if (mask.size() < data.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int from = data.size() - 1;
    int index = mask.size() - 1;
    data.resize(mask.size());
    while (index >= 0)
    {
        if (mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            data[index] = data[from];
            --from;
        }

        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);
}

/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(TYPE) \
template void expandDataByMask<TYPE>(PaddedPODArray<TYPE> &, const PaddedPODArray<UInt8> &, bool);

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

void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool inverted)
{
    if (mask.size() < offsets.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int index = mask.size() - 1;
    int from = offsets.size() - 1;
    offsets.resize(mask.size());
    UInt64 prev_offset = offsets[from];
    while (index >= 0)
    {
        if (mask[index] ^ inverted)
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

MaskInfo getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool inverted,
    const PaddedPODArray<UInt8> * mask_used_in_expanding,
    UInt8 default_value_in_expanding,
    bool inverted_mask_used_in_expanding,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls)
{
    if (const auto * col = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = checkAndGetColumn<ColumnUInt8>(*col->getNullMapColumnPtr())->getData();
        return getMaskFromColumn(col->getNestedColumnPtr(), res, inverted, mask_used_in_expanding, default_value_in_expanding, inverted_mask_used_in_expanding, null_value, &null_map, nulls);
    }

    bool is_full_column = true;
    if (mask_used_in_expanding && mask_used_in_expanding->size() != column->size())
        is_full_column = false;

    size_t size = is_full_column ? column->size() : mask_used_in_expanding->size();
    res.resize(size);

    bool only_null = column->onlyNull();

    /// Some columns doesn't implement getBool() method and we cannot
    /// convert them to mask, throw an exception in this case.
    try
    {
        MaskInfo info;
        bool value;
        size_t column_index = 0;
        for (size_t i = 0; i != size; ++i)
        {
            bool use_value_from_expanding_mask = mask_used_in_expanding && (!(*mask_used_in_expanding)[i] ^ inverted_mask_used_in_expanding);
            if (use_value_from_expanding_mask)
                value = inverted ? !default_value_in_expanding : default_value_in_expanding;
            else if (only_null || (null_bytemap && (*null_bytemap)[i]))
            {
                value = inverted ? !null_value : null_value;
                if (nulls)
                    (*nulls)[i] = 1;
            }
            else
                value = inverted ? !column->getBool(column_index) : column->getBool(column_index);

            if (value)
                info.has_once = true;
            else
                info.has_zeros = true;

            if (is_full_column || !use_value_from_expanding_mask)
                ++column_index;

            res[i] = value;
        }

        return info;
    }
    catch (...)
    {
        throw Exception("Cannot convert column " + column.get()->getName() + " to mask", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

MaskInfo disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2)
{
    if (mask1.size() != mask2.size())
        throw Exception("Cannot make a disjunction of masks, they have different sizes", ErrorCodes::LOGICAL_ERROR);

    MaskInfo info;
    for (size_t i = 0; i != mask1.size(); ++i)
    {
        mask1[i] |= mask2[i];
        if (mask1[i])
            info.has_once = true;
        else
            info.has_zeros = true;
    }

    return info;
}

void inverseMask(PaddedPODArray<UInt8> & mask)
{
    std::transform(mask.begin(), mask.end(), mask.begin(), [](UInt8 val){ return !val; });
}

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, const MaskInfo & mask_info, bool inverted)
{
    const auto * column_function = checkAndGetShortCircuitArgument(column.column);
    if (!column_function)
        return;

    ColumnWithTypeAndName result;
    /// If mask contains only zeros, we can just create
    /// an empty column with the execution result type.
    if ((!inverted && !mask_info.has_once) || (inverted && !mask_info.has_zeros))
    {
        auto result_type = column_function->getResultType();
        auto empty_column = result_type->createColumn();
        result = {std::move(empty_column), result_type, ""};
    }
    /// Filter column only if mask contains zeros.
    else if ((!inverted && mask_info.has_zeros) || (inverted && mask_info.has_once))
    {
        auto filtered = column_function->filter(mask, -1, inverted);
        result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce();
    }
    else
        result = column_function->reduce();

    column = std::move(result);
}

void executeColumnIfNeeded(ColumnWithTypeAndName & column, bool empty)
{
    const auto * column_function = checkAndGetShortCircuitArgument(column.column);
    if (!column_function)
        return;

    if (!empty)
        column = column_function->reduce();
    else
        column.column = column_function->getResultType()->createColumn();
}

int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments)
{
    int last_short_circuit_argument_index = -1;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (const auto * column_function = checkAndGetShortCircuitArgument(arguments[i].column))
            last_short_circuit_argument_index = i;
    }

    return last_short_circuit_argument_index;
}

}
