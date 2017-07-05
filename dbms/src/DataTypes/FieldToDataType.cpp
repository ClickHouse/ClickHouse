#include <Core/FieldVisitors.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNull.h>
#include <Common/Exception.h>
#include <ext/size.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypePtr FieldToDataType::operator() (Null & x) const
{
    return std::make_shared<DataTypeNull>();
}

DataTypePtr FieldToDataType::operator() (UInt64 & x) const
{
    if (x <= std::numeric_limits<UInt8>::max())        return std::make_shared<DataTypeUInt8>();
    if (x <= std::numeric_limits<UInt16>::max())    return std::make_shared<DataTypeUInt16>();
    if (x <= std::numeric_limits<UInt32>::max())    return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}

DataTypePtr FieldToDataType::operator() (Int64 & x) const
{
    if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())        return std::make_shared<DataTypeInt8>();
    if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min())    return std::make_shared<DataTypeInt16>();
    if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min())    return std::make_shared<DataTypeInt32>();
    return std::make_shared<DataTypeInt64>();
}

DataTypePtr FieldToDataType::operator() (Float64 & x) const
{
    return std::make_shared<DataTypeFloat64>();
}

DataTypePtr FieldToDataType::operator() (String & x) const
{
    return std::make_shared<DataTypeString>();
}


template <typename T>
static void convertArrayToCommonType(Array & arr)
{
    for (auto & elem : arr)
    {
        if (!elem.isNull())
            elem = applyVisitor(FieldVisitorConvertToNumber<T>(), elem);
    }
}


DataTypePtr FieldToDataType::operator() (Array & x) const
{
    if (x.empty())
        throw Exception("Cannot infer type of empty array", ErrorCodes::EMPTY_DATA_PASSED);

    /** The type of the array should be determined by the type of its elements.
      * If the elements are numbers, then select the smallest common type, if any,
      *  or throw an exception.
      * The code is similar to NumberTraits::ResultOfIf, but it's hard to use this code directly.
      *
      * Also notice that Float32 is not output, only Float64 is used instead.
      * This is done because Float32 type literals do not exist in the query.
      */

    bool has_string = false;
    bool has_array = false;
    bool has_float = false;
    bool has_tuple = false;
    bool has_null = false;
    bool has_uint128 = false;
    int max_bits = 0;
    int max_signed_bits = 0;
    int max_unsigned_bits = 0;

    /// Wrap the specified type into an array type. If at least one element of
    /// the array is nullable, first turn the input argument into a nullable type.
    auto wrap_into_array = [&has_null](const DataTypePtr & type)
    {
        return std::make_shared<DataTypeArray>(
            has_null ? std::make_shared<DataTypeNullable>(type) : type);
    };

    for (const Field & elem : x)
    {
        switch (elem.getType())
        {
            case Field::Types::UInt64:
            {
                UInt64 num = elem.get<UInt64>();
                if (num <= std::numeric_limits<UInt8>::max())
                    max_unsigned_bits = std::max(8, max_unsigned_bits);
                else if (num <= std::numeric_limits<UInt16>::max())
                    max_unsigned_bits = std::max(16, max_unsigned_bits);
                else if (num <= std::numeric_limits<UInt32>::max())
                    max_unsigned_bits = std::max(32, max_unsigned_bits);
                else
                    max_unsigned_bits = 64;
                max_bits = std::max(max_unsigned_bits, max_bits);
                break;
            }
            case Field::Types::Int64:
            {
                Int64 num = elem.get<Int64>();
                if (num <= std::numeric_limits<Int8>::max() && num >= std::numeric_limits<Int8>::min())
                    max_signed_bits = std::max(8, max_signed_bits);
                else if (num <= std::numeric_limits<Int16>::max() && num >= std::numeric_limits<Int16>::min())
                    max_signed_bits = std::max(16, max_signed_bits);
                else if (num <= std::numeric_limits<Int32>::max() && num >= std::numeric_limits<Int32>::min())
                    max_signed_bits = std::max(32, max_signed_bits);
                else
                    max_signed_bits = 64;
                max_bits = std::max(max_signed_bits, max_bits);
                break;
            }
            case Field::Types::UInt128:
            {
                has_uint128 = true;
                break;
            }
            case Field::Types::Float64:
            {
                has_float = true;
                break;
            }
            case Field::Types::String:
            {
                has_string = true;
                break;
            }
            case Field::Types::Array:
            {
                has_array = true;
                break;
            }
            case Field::Types::Tuple:
            {
                has_tuple = true;
                break;
            }
            case Field::Types::Null:
            {
                has_null = true;
                break;
            }
        }
    }

    if ((has_string + has_array + (max_bits > 0)) > 1)
        throw Exception("Incompatible types of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (has_array)
        throw Exception("Type inference of multidimensional arrays is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (has_tuple)
        throw Exception("Type inference of array of tuples is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (has_string)
        return wrap_into_array(std::make_shared<DataTypeString>());

    if (has_uint128)
        return wrap_into_array(std::make_shared<DataTypeUInt128>());

    if (has_float && max_bits == 64)
        throw Exception("Incompatible types Float64 and UInt64/Int64 of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (has_float)
    {
        convertArrayToCommonType<Float64>(x);
        return wrap_into_array(std::make_shared<DataTypeFloat64>());
    }

    if (max_signed_bits == 64 && max_unsigned_bits == 64)
        throw Exception("Incompatible types UInt64 and Int64 of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (max_signed_bits && !max_unsigned_bits)
    {
        if (max_signed_bits == 8)
            return wrap_into_array(std::make_shared<DataTypeInt8>());
        if (max_signed_bits == 16)
            return wrap_into_array(std::make_shared<DataTypeInt16>());
        if (max_signed_bits == 32)
            return wrap_into_array(std::make_shared<DataTypeInt32>());
        if (max_signed_bits == 64)
            return wrap_into_array(std::make_shared<DataTypeInt64>());
    }

    if (!max_signed_bits && max_unsigned_bits)
    {
        if (max_unsigned_bits == 8)
            return wrap_into_array(std::make_shared<DataTypeUInt8>());
        if (max_unsigned_bits == 16)
            return wrap_into_array(std::make_shared<DataTypeUInt16>());
        if (max_unsigned_bits == 32)
            return wrap_into_array(std::make_shared<DataTypeUInt32>());
        if (max_unsigned_bits == 64)
            return wrap_into_array(std::make_shared<DataTypeUInt64>());
    }

    if (max_signed_bits && max_unsigned_bits)
    {
        convertArrayToCommonType<Int64>(x);

        if (max_unsigned_bits >= max_signed_bits)
        {
            /// An unsigned type does not fit into a signed type. It is necessary to increase the number of bits.
            if (max_bits == 8)
                return wrap_into_array(std::make_shared<DataTypeInt16>());
            if (max_bits == 16)
                return wrap_into_array(std::make_shared<DataTypeInt32>());
            if (max_bits == 32)
                return wrap_into_array(std::make_shared<DataTypeInt64>());
            else
                throw Exception("Incompatible types UInt64 and signed integer of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
        {
            /// An unsigned type is placed in a signed type.
            if (max_bits == 8)
                return wrap_into_array(std::make_shared<DataTypeInt8>());
            if (max_bits == 16)
                return wrap_into_array(std::make_shared<DataTypeInt16>());
            if (max_bits == 32)
                return wrap_into_array(std::make_shared<DataTypeInt32>());
            if (max_bits == 64)
                return wrap_into_array(std::make_shared<DataTypeInt64>());
        }
    }

    if (has_null)
    {
        /// Special case: an array of NULLs is represented as an array
        /// of Nullable(UInt8) because ColumnNull is actually ColumnConst<Null>.
        return wrap_into_array(std::make_shared<DataTypeUInt8>());
    }

    throw Exception("Incompatible types of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


DataTypePtr FieldToDataType::operator() (Tuple & x) const
{
    auto & tuple = static_cast<TupleBackend &>(x);
    if (tuple.empty())
        throw Exception("Cannot infer type of an empty tuple", ErrorCodes::EMPTY_DATA_PASSED);

    DataTypes element_types;
    element_types.reserve(ext::size(tuple));

    for (auto & element : tuple)
        element_types.push_back(applyVisitor(FieldToDataType{}, element));

    return std::make_shared<DataTypeTuple>(element_types);
}


}
