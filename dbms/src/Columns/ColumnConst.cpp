#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <ext/enumerate.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


template <>
ColumnPtr ColumnConst<Null>::convertToFullColumn() const
{
    /// We basically create a column whose rows have NULL values.

    ColumnPtr nested_col;

    if (data_type)
    {
        const IDataType & nested_data_type = *typeid_cast<const DataTypeNullable &>(*data_type).getNestedType();
        nested_col = nested_data_type.createConstColumn(s, nested_data_type.getDefault())->convertToFullColumnIfConst();
    }
    else
        nested_col = std::make_shared<ColumnUInt8>(s, 0);

    ColumnPtr null_map = std::make_shared<ColumnUInt8>(s, 1);

    return std::make_shared<ColumnNullable>(nested_col, null_map);
}


template <> ColumnPtr ColumnConst<String>::convertToFullColumn() const
{
    if (!data_type || typeid_cast<const DataTypeString *>(&*data_type))
    {
        auto res = std::make_shared<ColumnString>();
        ColumnString::Offsets_t & offsets = res->getOffsets();
        ColumnString::Chars_t & vec = res->getChars();

        size_t string_size = data.size() + 1;
        size_t offset = 0;
        offsets.resize(s);
        vec.resize(s * string_size);

        for (size_t i = 0; i < s; ++i)
        {
            memcpy(&vec[offset], data.data(), string_size);
            offset += string_size;
            offsets[i] = offset;
        }

        return res;
    }
    else if (const DataTypeFixedString * type = typeid_cast<const DataTypeFixedString *>(&*data_type))
    {
        size_t n = type->getN();

        if (data.size() > n)
            throw Exception("Too long value for " + type->getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);

        auto res = std::make_shared<ColumnFixedString>(n);
        ColumnFixedString::Chars_t & vec = res->getChars();

        vec.resize_fill(n * s);
        size_t offset = 0;

        for (size_t i = 0; i < s; ++i)
        {
            memcpy(&vec[offset], data.data(), data.size());
            offset += n;
        }

        return res;
    }
    else
        throw Exception("Invalid data type in ColumnConstString: " + data_type->getName(), ErrorCodes::LOGICAL_ERROR);
}


ColumnPtr ColumnConst<Array>::convertToFullColumn() const
{
    if (!data_type)
        throw Exception("No data type specified for ColumnConstArray", ErrorCodes::LOGICAL_ERROR);

    const DataTypeArray * type = typeid_cast<const DataTypeArray *>(&*data_type);
    if (!type)
        throw Exception("Non-array data type specified for ColumnConstArray", ErrorCodes::LOGICAL_ERROR);

    const Array & array = getDataFromHolderImpl();
    size_t array_size = array.size();

    const auto & nested_type = type->getNestedType();
    ColumnPtr nested_column;

    if (nested_type->isNull())
    {
        /// Special case: an array of Null is actually an array of Nullable(UInt8).
        nested_column = std::make_shared<ColumnNullable>(
            std::make_shared<ColumnUInt8>(), std::make_shared<ColumnUInt8>());
    }
    else
        nested_column = type->getNestedType()->createColumn();

    auto res = std::make_shared<ColumnArray>(nested_column);
    ColumnArray::Offsets_t & offsets = res->getOffsets();

    offsets.resize(s);
    for (size_t i = 0; i < s; ++i)
    {
        offsets[i] = (i + 1) * array_size;
        for (size_t j = 0; j < array_size; ++j)
            nested_column->insert(array[j]);
    }

    return res;
}


StringRef ColumnConst<Array>::getDataAt(size_t n) const
{
    throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

UInt64 ColumnConst<Array>::get64(size_t n) const
{
    throw Exception("Method get64 is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnConst<Array>::getDataAtWithTerminatingZero(size_t n) const
{
    throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}


ColumnPtr ColumnConst<Tuple>::convertToFullColumn() const
{
    return convertToTupleOfConstants()->convertToFullColumnIfConst();
}

ColumnPtr ColumnConst<Tuple>::convertToTupleOfConstants() const
{
    if (!data_type)
        throw Exception("No data type specified for ColumnConstTuple", ErrorCodes::LOGICAL_ERROR);

    const DataTypeTuple * type = typeid_cast<const DataTypeTuple *>(&*data_type);
    if (!type)
        throw Exception("Non-Tuple data type specified for ColumnConstTuple", ErrorCodes::LOGICAL_ERROR);

    /// Create columns for each element and convert to full columns.
    const DataTypes & element_types = type->getElements();
    size_t tuple_size = element_types.size();
    Block block;

    for (size_t i = 0; i < tuple_size; ++i)
        block.insert(ColumnWithTypeAndName{
            element_types[i]->createConstColumn(s, static_cast<const TupleBackend &>(*data)[i]),
            element_types[i],
            ""});

    return std::make_shared<ColumnTuple>(block);
}

void ColumnConst<Tuple>::getExtremes(Field & min, Field & max) const
{
    min = *data;
    max = *data;
}

StringRef ColumnConst<Tuple>::getDataAt(size_t n) const
{
    throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

UInt64 ColumnConst<Tuple>::get64(size_t n) const
{
    throw Exception("Method get64 is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnConst<Tuple>::getDataAtWithTerminatingZero(size_t n) const
{
    throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}


}
