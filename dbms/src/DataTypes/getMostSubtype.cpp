#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/typeid_cast.h>

#include <DataTypes/getMostSubtype.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_COMMON_TYPE;
}

namespace
{
String getExceptionMessagePrefix(const DataTypes & types)
{
    WriteBufferFromOwnString res;
    res << "There is no subtype for types ";

    bool first = true;
    for (const auto & type : types)
    {
        if (!first)
            res << ", ";
        first = false;

        res << type->getName();
    }

    return res.str();
}

}


DataTypePtr getMostSubtype(const DataTypes & types, bool throw_if_result_is_nothing, bool force_support_conversion)
{

    auto getNothingOrThrow = [throw_if_result_is_nothing, & types](const std::string & reason)
    {
        if (throw_if_result_is_nothing)
            throw Exception(getExceptionMessagePrefix(types) + reason, ErrorCodes::NO_COMMON_TYPE);
        return std::make_shared<DataTypeNothing>();
    };

    /// Trivial cases

    if (types.empty())
    {
        if (throw_if_result_is_nothing)
            throw Exception("There is no common type for empty type list", ErrorCodes::NO_COMMON_TYPE);
        return std::make_shared<DataTypeNothing>();
    }

    if (types.size() == 1)
    {
        if (throw_if_result_is_nothing && typeid_cast<const DataTypeNothing *>(types[0].get()))
            throw Exception("There is no common type for type Nothing", ErrorCodes::NO_COMMON_TYPE);
        return types[0];
    }

    /// All types are equal
    {
        bool all_equal = true;
        for (size_t i = 1, size = types.size(); i < size; ++i)
        {
            if (!types[i]->equals(*types[0]))
            {
                all_equal = false;
                break;
            }
        }

        if (all_equal)
            return types[0];
    }

    /// Recursive rules

    /// If there are Nothing types, result is Nothing
    {
        for (const auto & type : types)
            if (typeid_cast<const DataTypeNothing *>(type.get()))
                return getNothingOrThrow(" because some of them are Nothing");
    }

    /// For Arrays
    {
        bool have_array = false;
        bool all_arrays = true;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const auto type_array = typeid_cast<const DataTypeArray *>(type.get()))
            {
                have_array = true;
                nested_types.emplace_back(type_array->getNestedType());
            }
            else
                all_arrays = false;
        }

        if (have_array)
        {
            if (!all_arrays)
                return getNothingOrThrow(" because some of them are Array and some of them are not");

            return std::make_shared<DataTypeArray>(getMostSubtype(nested_types, false, force_support_conversion));
        }
    }

    /// For tuples
    {
        bool have_tuple = false;
        bool all_tuples = true;
        size_t tuple_size = 0;

        std::vector<DataTypes> nested_types;

        for (const auto & type : types)
        {
            if (const auto type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                if (!have_tuple)
                {
                    tuple_size = type_tuple->getElements().size();
                    nested_types.resize(tuple_size);
                    for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                        nested_types[elem_idx].reserve(types.size());
                }
                else if (tuple_size != type_tuple->getElements().size())
                    return getNothingOrThrow(" because Tuples have different sizes");

                have_tuple = true;

                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                    nested_types[elem_idx].emplace_back(type_tuple->getElements()[elem_idx]);
            }
            else
                all_tuples = false;
        }

        if (have_tuple)
        {
            if (!all_tuples)
                return getNothingOrThrow(" because some of them are Tuple and some of them are not");

            DataTypes common_tuple_types(tuple_size);
            for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                common_tuple_types[elem_idx] =
                        getMostSubtype(nested_types[elem_idx], throw_if_result_is_nothing, force_support_conversion);

            return std::make_shared<DataTypeTuple>(common_tuple_types);
        }
    }

    /// For Nullable
    {
        bool all_nullable = true;
        bool have_nullable = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const auto type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            {
                have_nullable = true;
                nested_types.emplace_back(type_nullable->getNestedType());
            }
            else
            {
                all_nullable = false;
                nested_types.emplace_back(type);
            }
        }

        if (have_nullable)
        {
            if (all_nullable || force_support_conversion)
                return std::make_shared<DataTypeNullable>(getMostSubtype(nested_types, false, force_support_conversion));

            return getMostSubtype(nested_types, throw_if_result_is_nothing, force_support_conversion);
        }
    }

    /// Non-recursive rules

    /// For String and FixedString, the common type is FixedString.
    /// For different FixedStrings, the common type is Nothing.
    /// No other types are compatible with Strings. TODO Enums?
    {
        bool have_string = false;
        bool all_strings = true;

        DataTypePtr fixed_string_type = nullptr;

        for (const auto & type : types)
        {
            if (type->isFixedString())
            {
                have_string = true;
                if (!fixed_string_type)
                    fixed_string_type = type;
                else if (!type->equals(*fixed_string_type))
                    return getNothingOrThrow(" because some of them are FixedStrings with different length");
            }
            else if (type->isString())
                have_string = true;
            else
                all_strings = false;
        }

        if (have_string)
        {
            if (!all_strings)
                return getNothingOrThrow(" because some of them are String/FixedString and some of them are not");

            return fixed_string_type ? fixed_string_type : std::make_shared<DataTypeString>();
        }
    }

    /// For Date and DateTime, the common type is Date. No other types are compatible.
    {
        bool have_date_or_datetime = false;
        bool all_date_or_datetime = true;

        for (const auto & type : types)
        {
            if (type->isDateOrDateTime())
                have_date_or_datetime = true;
            else
                all_date_or_datetime = false;
        }

        if (have_date_or_datetime)
        {
            if (!all_date_or_datetime)
                return getNothingOrThrow(" because some of them are Date/DateTime and some of them are not");

            return std::make_shared<DataTypeDate>();
        }
    }

    /// For numeric types, the most complicated part.
    {
        bool all_numbers = true;

        size_t min_bits_of_signed_integer = 0;
        size_t min_bits_of_unsigned_integer = 0;
        size_t min_mantissa_bits_of_floating = 0;

        auto minimize = [](size_t & what, size_t value)
        {
            if (what == 0 || value < what)
                what = value;
        };

        for (const auto & type : types)
        {
            if (typeid_cast<const DataTypeUInt8 *>(type.get()))
                minimize(min_bits_of_unsigned_integer, 8);
            else if (typeid_cast<const DataTypeUInt16 *>(type.get()))
                minimize(min_bits_of_unsigned_integer, 16);
            else if (typeid_cast<const DataTypeUInt32 *>(type.get()))
                minimize(min_bits_of_unsigned_integer, 32);
            else if (typeid_cast<const DataTypeUInt64 *>(type.get()))
                minimize(min_bits_of_unsigned_integer, 64);
            else if (typeid_cast<const DataTypeInt8 *>(type.get()))
                minimize(min_bits_of_signed_integer, 8);
            else if (typeid_cast<const DataTypeInt16 *>(type.get()))
                minimize(min_bits_of_signed_integer, 16);
            else if (typeid_cast<const DataTypeInt32 *>(type.get()))
                minimize(min_bits_of_signed_integer, 32);
            else if (typeid_cast<const DataTypeInt64 *>(type.get()))
                minimize(min_bits_of_signed_integer, 64);
            else if (typeid_cast<const DataTypeFloat32 *>(type.get()))
                minimize(min_mantissa_bits_of_floating, 24);
            else if (typeid_cast<const DataTypeFloat64 *>(type.get()))
                minimize(min_mantissa_bits_of_floating, 53);
            else
                all_numbers = false;
        }

        if (min_bits_of_signed_integer || min_bits_of_unsigned_integer || min_mantissa_bits_of_floating)
        {
            if (!all_numbers)
                return getNothingOrThrow(" because some of them are numbers and some of them are not");

            /// If the result must be floating.
            if (!min_bits_of_signed_integer && !min_bits_of_unsigned_integer)
            {
                if (min_mantissa_bits_of_floating <= 24)
                    return std::make_shared<DataTypeFloat32>();
                else if (min_mantissa_bits_of_floating <= 53)
                    return std::make_shared<DataTypeFloat64>();
                else
                    throw Exception("Logical error: " + getExceptionMessagePrefix(types)
                                    + " but as all data types are floats, we must have found maximum float type", ErrorCodes::NO_COMMON_TYPE);
            }

            /// If there are signed and unsigned types of same bit-width, the result must be unsigned number.
            if (min_bits_of_unsigned_integer &&
                (min_bits_of_signed_integer == 0 || min_bits_of_unsigned_integer <= min_bits_of_signed_integer))
            {
                if (min_bits_of_unsigned_integer <= 8)
                    return std::make_shared<DataTypeUInt8>();
                else if (min_bits_of_unsigned_integer <= 16)
                    return std::make_shared<DataTypeUInt16>();
                else if (min_bits_of_unsigned_integer <= 32)
                    return std::make_shared<DataTypeUInt32>();
                else if (min_bits_of_unsigned_integer <= 64)
                    return std::make_shared<DataTypeUInt64>();
                else
                    throw Exception("Logical error: " + getExceptionMessagePrefix(types)
                                    + " but as all data types are integers, we must have found maximum unsigned integer type", ErrorCodes::NO_COMMON_TYPE);
            }

            /// All signed.
            {
                if (min_bits_of_signed_integer <= 8)
                    return std::make_shared<DataTypeInt8>();
                else if (min_bits_of_signed_integer <= 16)
                    return std::make_shared<DataTypeInt16>();
                else if (min_bits_of_signed_integer <= 32)
                    return std::make_shared<DataTypeInt32>();
                else if (min_bits_of_signed_integer <= 64)
                    return std::make_shared<DataTypeInt64>();
                else
                    throw Exception("Logical error: " + getExceptionMessagePrefix(types)
                                    + " but as all data types are integers, we must have found maximum signed integer type", ErrorCodes::NO_COMMON_TYPE);
            }
        }
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    return getNothingOrThrow("");
}

}
