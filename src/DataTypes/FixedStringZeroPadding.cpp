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

    auto left_decayed = removeLowCardinalityAndNullable(left);
    auto right_decayed = removeLowCardinalityAndNullable(right);

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
    /// TODO: fix the weird equals behavior
    if (typeid_cast<const DataTypeArray *>(left_decayed.get()) || typeid_cast<const DataTypeMap *>(left_decayed.get()))
        return false;

    return isStringOrFixedString(left_decayed) && isStringOrFixedString(right_decayed)
        && (isFixedString(left_decayed) || isFixedString(right_decayed));
}

static bool containsFixedString(const DataTypePtr & type)
{
    auto decayed = removeLowCardinalityAndNullable(type);

    if (isFixedString(decayed))
        return true;

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(decayed.get()))
        return containsFixedString(type_array->getNestedType());

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(decayed.get()))
        return containsFixedString(type_map->getKeyType()) || containsFixedString(type_map->getValueType());

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(decayed.get()))
    {
        for (const auto & element : type_tuple->getElements())
            if (containsFixedString(element))
                return true;
    }

    return false;
}

bool fixedStringPaddingInsideContainer(const DataTypePtr & left, const DataTypePtr & right)
{
    if (!left || !right)
        return false;

    auto left_decayed = removeLowCardinalityAndNullable(left);
    auto right_decayed = removeLowCardinalityAndNullable(right);

    const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_decayed.get());
    const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_decayed.get());
    if (left_tuple && right_tuple)
    {
        const auto & left_elements = left_tuple->getElements();
        const auto & right_elements = right_tuple->getElements();
        if (left_elements.size() != right_elements.size())
            return false;

        for (size_t i = 0; i < left_elements.size(); ++i)
            if (fixedStringPaddingInsideContainer(left_elements[i], right_elements[i]))
                return true;

        return false;
    }

    const bool inside_container = typeid_cast<const DataTypeArray *>(left_decayed.get())
        || typeid_cast<const DataTypeMap *>(left_decayed.get()) || typeid_cast<const DataTypeArray *>(right_decayed.get())
        || typeid_cast<const DataTypeMap *>(right_decayed.get());

    return inside_container && (containsFixedString(left_decayed) || containsFixedString(right_decayed));
}

bool zeroPaddedStringConstant(const DataTypePtr & type)
{
    if (!type)
        return false;

    auto decayed = removeLowCardinalityAndNullable(type);

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(decayed.get()))
        return zeroPaddedStringConstant(type_array->getNestedType());

    return isFixedString(decayed);
}

Field stripFixedStringPaddingForTerms(const Field & field, const DataTypePtr & type)
{
    auto inner_type = removeLowCardinalityAndNullable(type);

    if (isFixedString(inner_type) && field.getType() == Field::Types::String)
        return Field(String(stripTrailingZeros(field.safeGet<String>())));

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(inner_type.get());
        array_type && field.getType() == Field::Types::Array)
    {
        Array stripped;
        const auto & elements = field.safeGet<Array>();
        stripped.reserve(elements.size());
        for (const auto & element : elements)
            stripped.push_back(stripFixedStringPaddingForTerms(element, array_type->getNestedType()));
        return Field(std::move(stripped));
    }

    return field;
}

DataTypePtr indexedElementType(const DataTypePtr & type)
{
    auto decayed = removeLowCardinalityAndNullable(type);
    if (const auto * type_array = typeid_cast<const DataTypeArray *>(decayed.get()))
        return removeLowCardinalityAndNullable(type_array->getNestedType());
    return decayed;
}

ColumnPtr stripTrailingZerosInStrings(const ColumnPtr & column, const DataTypePtr & left, const DataTypePtr & right)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(
            stripTrailingZerosInStrings(column_const->getDataColumnPtr(), left, right), column_const->size());

    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        return ColumnNullable::create(
            stripTrailingZerosInStrings(column_nullable->getNestedColumnPtr(), left, right),
            column_nullable->getNullMapColumnPtr());

    auto left_decayed = removeLowCardinalityAndNullable(left);
    auto right_decayed = removeLowCardinalityAndNullable(right);

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        const auto & left_elements = assert_cast<const DataTypeTuple &>(*left_decayed).getElements();
        const auto & right_elements = assert_cast<const DataTypeTuple &>(*right_decayed).getElements();
        const size_t size = column_tuple->tupleSize();
        chassert(left_elements.size() == size && right_elements.size() == size);

        Columns elements(size);
        for (size_t i = 0; i < size; ++i)
            elements[i] = stripTrailingZerosInStrings(column_tuple->getColumnPtr(i), left_elements[i], right_elements[i]);
        return ColumnTuple::create(std::move(elements));
    }

    if (const auto * column_string = typeid_cast<const ColumnString *>(column.get());
        column_string && zeroPaddedStringComparison(left_decayed, right_decayed))
    {
        const size_t size = column_string->size();
        auto result = ColumnString::create();
        result->reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto value = stripTrailingZeros(column_string->getDataAt(i));
            result->insertData(value.data(), value.size());
        }
        return result;
    }

    /// Either the rule does not apply to this pair of types, or the values already have one
    /// canonical spelling: two `FixedString`s share a common `FixedString` type only when their
    /// widths are equal, and there the rule and an exact comparison give the same answer.
    return column;
}

ColumnPtr
stripTrailingZerosInArrayElements(const ColumnPtr & column, const DataTypePtr & left_element, const DataTypePtr & right_element)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(
            stripTrailingZerosInArrayElements(column_const->getDataColumnPtr(), left_element, right_element),
            column_const->size());

    const auto & column_array = assert_cast<const ColumnArray &>(*column);
    return ColumnArray::create(
        stripTrailingZerosInStrings(column_array.getDataPtr(), left_element, right_element), column_array.getOffsetsPtr());
}

}
