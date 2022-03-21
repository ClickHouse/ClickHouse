#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
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
        if (!!mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            /// Copy only if it makes sense.
            if (index != from)
                data[index] = data[from];
            --from;
        }
        else
            data[index] = T();

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

template <bool inverted, bool column_is_short, typename Container>
size_t extractMaskNumericImpl(
    PaddedPODArray<UInt8> & mask,
    const Container & data,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls)
{
    size_t ones_count = 0;
    size_t data_index = 0;
    for (size_t i = 0; i != mask.size(); ++i)
    {
        // Change mask only where value is 1.
        if (!mask[i])
            continue;

        UInt8 value;
        size_t index;
        if constexpr (column_is_short)
        {
            index = data_index;
            ++data_index;
        }
        else
            index = i;

        if (null_bytemap && (*null_bytemap)[index])
        {
            value = null_value;
            if (nulls)
                (*nulls)[i] = 1;
        }
        else
            value = !!data[index];

        if constexpr (inverted)
            value = !value;

        if (value)
            ++ones_count;

        mask[i] = value;
    }
    return ones_count;
}

template <bool inverted, typename NumericType>
bool extractMaskNumeric(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls,
    MaskInfo & mask_info)
{
    const auto * numeric_column = checkAndGetColumn<ColumnVector<NumericType>>(column.get());
    if (!numeric_column)
        return false;

    const auto & data = numeric_column->getData();
    size_t ones_count;
    if (column->size() < mask.size())
        ones_count = extractMaskNumericImpl<inverted, true>(mask, data, null_value, null_bytemap, nulls);
    else
        ones_count = extractMaskNumericImpl<inverted, false>(mask, data, null_value, null_bytemap, nulls);

    mask_info.has_ones = ones_count > 0;
    mask_info.has_zeros = ones_count != mask.size();
    return true;
}

template <bool inverted>
MaskInfo extractMaskFromConstOrNull(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value,
    PaddedPODArray<UInt8> * nulls = nullptr)
{
    UInt8 value;
    if (column->onlyNull())
    {
        value = null_value;
        if (nulls)
            std::fill(nulls->begin(), nulls->end(), 1);
    }
    else
        value = column->getBool(0);

    if constexpr (inverted)
        value = !value;

    size_t ones_count = 0;
    if (value)
        ones_count = countBytesInFilter(mask);
    else
        std::fill(mask.begin(), mask.end(), 0);

    return {.has_ones = ones_count > 0, .has_zeros = ones_count != mask.size()};
}

template <bool inverted>
MaskInfo extractMaskImpl(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & col,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls = nullptr)
{
    auto column = col->convertToFullColumnIfLowCardinality();

    /// Special implementation for Null and Const columns.
    if (column->onlyNull() || checkAndGetColumn<ColumnConst>(*column))
        return extractMaskFromConstOrNull<inverted>(mask, column, null_value, nulls);

    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = nullable_column->getNullMapData();
        return extractMaskImpl<inverted>(mask, nullable_column->getNestedColumnPtr(), null_value, &null_map, nulls);
    }

    MaskInfo mask_info;

    if (!(extractMaskNumeric<inverted, UInt8>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt16>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt32>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt64>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int8>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int16>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int32>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int64>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Float32>(mask, column, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Float64>(mask, column, null_value, null_bytemap, nulls, mask_info)))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot convert column {} to mask.", column->getName());

    return mask_info;
}

MaskInfo extractMask(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value)
{
    return extractMaskImpl<false>(mask, column, null_value, nullptr);
}

MaskInfo extractInvertedMask(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value)
{
    return extractMaskImpl<true>(mask, column, null_value, nullptr);
}

MaskInfo extractMask(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> * nulls,
    UInt8 null_value)
{
    return extractMaskImpl<false>(mask, column, null_value, nullptr, nulls);
}

MaskInfo extractInvertedMask(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> * nulls,
    UInt8 null_value)
{
    return extractMaskImpl<true>(mask, column, null_value, nullptr, nulls);
}


void inverseMask(PaddedPODArray<UInt8> & mask, MaskInfo & mask_info)
{
    for (auto & byte : mask)
        byte = !byte;
    std::swap(mask_info.has_ones, mask_info.has_zeros);
}

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, const MaskInfo & mask_info)
{
    const auto * column_function = checkAndGetShortCircuitArgument(column.column);
    if (!column_function)
        return;

    ColumnWithTypeAndName result;
    /// If mask contains only zeros, we can just create
    /// an empty column with the execution result type.
    if (!mask_info.has_ones)
    {
        auto result_type = column_function->getResultType();
        auto empty_column = result_type->createColumn();
        result = {std::move(empty_column), result_type, ""};
    }
    /// Filter column only if mask contains zeros.
    else if (mask_info.has_zeros)
    {
        auto filtered = column_function->filter(mask, -1);
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

int checkShortCircuitArguments(const ColumnsWithTypeAndName & arguments)
{
    int last_short_circuit_argument_index = -1;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (checkAndGetShortCircuitArgument(arguments[i].column))
            last_short_circuit_argument_index = i;
    }

    return last_short_circuit_argument_index;
}

void copyMask(const PaddedPODArray<UInt8> & from, PaddedPODArray<UInt8> & to)
{
    if (from.size() != to.size())
        throw Exception("Cannot copy mask, because source and destination have different size", ErrorCodes::LOGICAL_ERROR);

    if (from.empty())
        return;

    memcpy(to.data(), from.data(), from.size() * sizeof(*from.data()));
}

}

