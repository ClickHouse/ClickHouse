#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
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
        if (mask[index] ^ inverted)
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
    const PaddedPODArray<UInt8> & mask,
    const Container & data,
    PaddedPODArray<UInt8> & result,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls)
{
    size_t ones_count = 0;
    size_t data_index = 0;
    for (size_t i = 0; i != mask.size(); ++i)
    {
        UInt8 value = 0;
        if (mask[i])
        {
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
        }

        if (value)
            ++ones_count;

        result[i] = value;
    }
    return ones_count;
}

template <bool inverted, typename NumericType>
bool extractMaskNumeric(
    const PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & result,
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
        ones_count = extractMaskNumericImpl<inverted, true>(mask, data, result, null_value, null_bytemap, nulls);
    else
        ones_count = extractMaskNumericImpl<inverted, false>(mask, data, result, null_value, null_bytemap, nulls);

    mask_info.has_ones = ones_count > 0;
    mask_info.has_zeros = ones_count != mask.size();
    return true;
}

template <bool inverted>
MaskInfo extractMaskFromConstOrNull(
    const PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & result,
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
    {
        /// Copy mask to result only if they are not the same.
        if (result != mask)
        {
            for (size_t i = 0; i != mask.size(); ++i)
            {
                result[i] = mask[i];
                if (result[i])
                    ++ones_count;
            }
        }
        else
            ones_count = countBytesInFilter(mask);
    }
    else
        std::fill(result.begin(), result.end(), 0);

    return {.has_ones = ones_count > 0, .has_zeros = ones_count != mask.size()};
}

template <bool inverted>
MaskInfo extractMaskImpl(
    const PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & result,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    PaddedPODArray<UInt8> * nulls = nullptr)
{
    /// Special implementation for Null and Const columns.
    if (column->onlyNull() || checkAndGetColumn<ColumnConst>(*column))
        return extractMaskFromConstOrNull<inverted>(mask, column, result, null_value, nulls);

    if (const auto * col = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = col->getNullMapData();
        return extractMaskImpl<inverted>(mask, col->getNestedColumnPtr(), result, null_value, &null_map, nulls);
    }

    MaskInfo mask_info;

    if (!(extractMaskNumeric<inverted, UInt8>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt16>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt32>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, UInt64>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int8>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int16>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int32>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Int64>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Float32>(mask, column, result, null_value, null_bytemap, nulls, mask_info)
          || extractMaskNumeric<inverted, Float64>(mask, column, result, null_value, null_bytemap, nulls, mask_info)))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot convert column {} to mask.", column->getName());

    return mask_info;
}

template <bool inverted>
MaskInfo extractMask(
    const PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & result,
    UInt8 null_value)
{
    return extractMaskImpl<inverted>(mask, column, result, null_value, nullptr);
}

template <bool inverted>
MaskInfo extractMaskInplace(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value)
{
    return extractMaskImpl<inverted>(mask, column, mask, null_value, nullptr);
}

template <bool inverted>
MaskInfo extractMaskInplaceWithNulls(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> * nulls,
    UInt8 null_value)
{
    return extractMaskImpl<inverted>(mask, column, mask, null_value, nullptr, nulls);
}

template MaskInfo extractMask<true>(const PaddedPODArray<UInt8> & mask, const ColumnPtr & column, PaddedPODArray<UInt8> & result, UInt8 null_value);
template MaskInfo extractMask<false>(const PaddedPODArray<UInt8> & mask, const ColumnPtr & column, PaddedPODArray<UInt8> & result, UInt8 null_value);
template MaskInfo extractMaskInplace<true>(PaddedPODArray<UInt8> & mask, const ColumnPtr & column, UInt8 null_value);
template MaskInfo extractMaskInplace<false>(PaddedPODArray<UInt8> & mask, const ColumnPtr & column, UInt8 null_value);
template MaskInfo extractMaskInplaceWithNulls<true>(PaddedPODArray<UInt8> & mask, const ColumnPtr & column, PaddedPODArray<UInt8> * nulls, UInt8 null_value);
template MaskInfo extractMaskInplaceWithNulls<false>(PaddedPODArray<UInt8> & mask, const ColumnPtr & column, PaddedPODArray<UInt8> * nulls, UInt8 null_value);


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
    if ((!inverted && !mask_info.has_ones) || (inverted && !mask_info.has_zeros))
    {
        auto result_type = column_function->getResultType();
        auto empty_column = result_type->createColumn();
        result = {std::move(empty_column), result_type, ""};
    }
    /// Filter column only if mask contains zeros.
    else if ((!inverted && mask_info.has_zeros) || (inverted && mask_info.has_ones))
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
        if (checkAndGetShortCircuitArgument(arguments[i].column))
            last_short_circuit_argument_index = i;
    }

    return last_short_circuit_argument_index;
}

}
