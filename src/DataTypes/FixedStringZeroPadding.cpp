#include <DataTypes/FixedStringZeroPadding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>

namespace DB
{

bool zeroPaddedStringComparison(const DataTypePtr & left, const DataTypePtr & right)
{
    if (!left || !right)
        return false;

    auto left_decayed = removeNullable(removeLowCardinality(left));
    auto right_decayed = removeNullable(removeLowCardinality(right));

    const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_decayed.get());
    const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_decayed.get());
    if (left_tuple && right_tuple)
    {
        const auto & left_elements = left_tuple->getElements();
        const auto & right_elements = right_tuple->getElements();
        if (left_elements.size() != right_elements.size())
            return false;

        for (size_t i = 0; i < left_elements.size(); ++i)
            if (zeroPaddedStringComparison(left_elements[i], right_elements[i]))
                return true;

        return false;
    }

    /// Deliberately not recursed into. `equals` compares an `Array` or a `Map` through a cast to
    /// their common type, which drops the padding of only the operand it converts, so it does not
    /// apply the rule to their elements: `[toFixedString('V0', 3)] = ['V0\0']` is 0 while
    /// `toFixedString('V0', 3) = 'V0\0'` is 1. Recursing here would make these functions disagree
    /// with `equals` in the opposite direction. `Tuple` above is different: `equals` decomposes it
    /// element-wise with the element types intact, so the rule does reach a `FixedString` inside it.
    if (typeid_cast<const DataTypeArray *>(left_decayed.get()) || typeid_cast<const DataTypeMap *>(left_decayed.get()))
        return false;

    return isStringOrFixedString(left_decayed) && isStringOrFixedString(right_decayed)
        && (isFixedString(left_decayed) || isFixedString(right_decayed));
}

bool zeroPaddedStringConstant(const DataTypePtr & type)
{
    if (!type)
        return false;

    auto decayed = removeNullable(removeLowCardinality(type));

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(decayed.get()))
        return zeroPaddedStringConstant(type_array->getNestedType());

    return isFixedString(decayed);
}

ColumnPtr stripTrailingZerosInStrings(const ColumnPtr & column, const DataTypePtr & type)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(
            stripTrailingZerosInStrings(column_const->getDataColumnPtr(), type), column_const->size());

    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & column_nullable = assert_cast<const ColumnNullable &>(*column);
        return ColumnNullable::create(
            stripTrailingZerosInStrings(column_nullable.getNestedColumnPtr(), type_nullable->getNestedType()),
            column_nullable.getNullMapColumnPtr());
    }

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & column_tuple = assert_cast<const ColumnTuple &>(*column);
        const auto & element_types = type_tuple->getElements();
        Columns elements(element_types.size());
        for (size_t i = 0; i < element_types.size(); ++i)
            elements[i] = stripTrailingZerosInStrings(column_tuple.getColumnPtr(i), element_types[i]);
        return ColumnTuple::create(std::move(elements));
    }

    if (isString(type))
    {
        const auto & column_string = assert_cast<const ColumnString &>(*column);
        const size_t size = column_string.size();
        auto result = ColumnString::create();
        result->reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto value = stripTrailingZeros(column_string.getDataAt(i));
            result->insertData(value.data(), value.size());
        }
        return result;
    }

    /// No `String` values are reachable in this type, so there is nothing to canonicalise.
    return column;
}

ColumnPtr stripTrailingZerosInArrayElements(const ColumnPtr & column, const DataTypePtr & element_type)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(
            stripTrailingZerosInArrayElements(column_const->getDataColumnPtr(), element_type), column_const->size());

    const auto & column_array = assert_cast<const ColumnArray &>(*column);
    return ColumnArray::create(
        stripTrailingZerosInStrings(column_array.getDataPtr(), element_type), column_array.getOffsetsPtr());
}

}
