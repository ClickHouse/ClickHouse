#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnLowCardinality.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

ColumnPtr expandColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, Field * field, bool reverse)
{
    MutableColumnPtr res = column->cloneEmpty();
    res->reserve(mask.size());
    size_t index = 0;
    for (size_t i = 0; i != mask.size(); ++i)
    {
        if (reverse ^ mask[i])
        {
            if (index >= column->size())
                throw Exception("Too many bits in mask", ErrorCodes::LOGICAL_ERROR);

            res->insert((*column)[index]);
            ++index;
        }
        else if (field)
            res->insert(*field);
        else
            res->insertDefault();
    }

    if (index < column->size())
        throw Exception("Too less bits in mask", ErrorCodes::LOGICAL_ERROR);

    return res;
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

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, Field * default_value, bool reverse)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function)
        return;

    auto filtered = column_function->filter(mask, -1, reverse);
    auto result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce(true);
    result.column = expandColumnByMask(result.column, mask, default_value, reverse);
    column = std::move(result);
}

void executeColumnIfNeeded(ColumnWithTypeAndName & column)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function)
        return;

    column = typeid_cast<const ColumnFunction *>(column_function)->reduce(true);
}

}
