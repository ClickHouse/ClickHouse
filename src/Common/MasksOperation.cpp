#include <Common/MasksOperation.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

ColumnPtr expandColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, Field * field)
{
    MutableColumnPtr res = column->cloneEmpty();
    res->reserve(mask.size());
    size_t index = 0;
    for (size_t i = 0; i != mask.size(); ++i)
    {
        if (mask[i])
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

PaddedPODArray<UInt8> copyMaskImpl(const PaddedPODArray<UInt8>& mask, bool reverse, const PaddedPODArray<UInt8> * null_bytemap, UInt8 null_value)
{
    PaddedPODArray<UInt8> res;
    res.reserve(mask.size());
    for (size_t i = 0; i != mask.size(); ++i)
    {
        if (null_bytemap && (*null_bytemap)[i])
            res.push_back(null_value);
        else
            res.push_back(reverse ? !mask[i] : mask[i]);
    }

    return res;
}

PaddedPODArray<UInt8> reverseMask(const PaddedPODArray<UInt8> & mask)
{
    return copyMaskImpl(mask, true, nullptr, 1);
}

PaddedPODArray<UInt8> getMaskFromColumn(const ColumnPtr & column, bool reverse, const PaddedPODArray<UInt8> * null_bytemap, UInt8 null_value)
{
    if (const auto * col = typeid_cast<const ColumnConst *>(column.get()))
        return getMaskFromColumn(col->convertToFullColumn(), reverse, null_bytemap, null_value);

    if (const auto * col = typeid_cast<const ColumnNothing *>(column.get()))
        return PaddedPODArray<UInt8>(col->size(), null_value);

    if (const auto * col = typeid_cast<const ColumnNullable *>(column.get()))
    {
        const PaddedPODArray<UInt8> & null_map = typeid_cast<const ColumnUInt8 *>(col->getNullMapColumnPtr().get())->getData();
        return getMaskFromColumn(col->getNestedColumnPtr(), reverse, &null_map, null_value);
    }

    if (const auto * col = typeid_cast<const ColumnUInt8 *>(column.get()))
        return copyMaskImpl(col->getData(), reverse, null_bytemap, null_value);

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

ColumnWithTypeAndName maskedExecute(const ColumnWithTypeAndName & column, const PaddedPODArray<UInt8>& mask, Field * default_value)
{
    const auto * column_function = typeid_cast<const ColumnFunction *>(column.column.get());
    if (!column_function)
        return column;

    auto filtered = column_function->filter(mask, -1);
    auto result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce(true);
    result.column = expandColumnByMask(result.column, mask, default_value);
    return result;
}

}
