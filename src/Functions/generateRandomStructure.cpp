#include "config.h"

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

class FunctionGenerateRandomStructure : public IFunction
{
private:
    enum class Type
    {
        Int8,
        UInt8,
        Bool,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Float32,
        Float64,
        DateTime64,
        Decimal32,
        Decimal64,
        Date,
        Date32,
        DateTime,
        String,
        FixedString,
        IPv4,
        IPv6,
        Int128,
        UInt128,
        Int256,
        UInt256,
        Decimal128,
        Decimal256,
        Enum8,
        Enum16,
        Nullable,
        LowCardinality,
        Array,
        Tuple,
        Map,
        Nested,
    };

    static constexpr std::array<Type, 16> simple_types
    {
        Type::Int8,
        Type::UInt8,
        Type::Bool,
        Type::Int16,
        Type::UInt16,
        Type::Int32,
        Type::UInt32,
        Type::Int64,
        Type::UInt64,
        Type::Float32,
        Type::Float64,
        Type::Date,
        Type::Date32,
        Type::DateTime,
        Type::String,
        Type::FixedString,
    };

    static constexpr std::array<Type, 4> big_integer_types
    {
        Type::Int128,
        Type::UInt128,
        Type::Int256,
        Type::UInt256,
    };

    static constexpr std::array<Type, 3> decimal_types
    {
        Type::DateTime64,
        Type::Decimal32,
        Type::Decimal64,
    };

    static constexpr std::array<Type, 2> big_decimal_types
    {
        Type::Decimal128,
        Type::Decimal256,
    };

    static constexpr std::array<Type, 2> enum_types
    {
        Type::Enum8,
        Type::Enum16,
    };

    static constexpr std::array<Type, 2> ip_types
    {
        Type::IPv4,
        Type::IPv6,
    };

    static constexpr std::array<Type, 6> complex_types
    {
        Type::Nullable,
        Type::LowCardinality,
        Type::Array,
        Type::Tuple,
        Type::Map,
        Type::Nested,
    };

    static constexpr std::array<Type, 14> map_key_types
    {
        Type::Int8,
        Type::UInt8,
        Type::Bool,
        Type::Int16,
        Type::UInt16,
        Type::Int32,
        Type::UInt32,
        Type::Int64,
        Type::UInt64,
        Type::Date,
        Type::Date32,
        Type::DateTime,
        Type::String,
        Type::FixedString,
    };

    static constexpr std::array<Type, 2> map_key_string_types
    {
        Type::String,
        Type::FixedString
    };

    static constexpr size_t MAX_NUMBER_OF_COLUMNS = 128;
    static constexpr size_t MAX_TUPLE_ELEMENTS = 16;
    static constexpr size_t MAX_DATETIME64_PRECISION = 9;
    static constexpr size_t MAX_DECIMAL32_PRECISION = 9;
    static constexpr size_t MAX_DECIMAL64_PRECISION = 18;
    static constexpr size_t MAX_DECIMAL128_PRECISION = 38;
    static constexpr size_t MAX_DECIMAL256_PRECISION = 76;
    static constexpr size_t MAX_DEPTH = 32;

public:
    static constexpr auto name = "generateRandomStructure";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionGenerateRandomStructure>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1, 2, 3, 4, 5, 6}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 7)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected from 0 to 7",
                getName(), arguments.size());

        for (size_t i = 0; i != 2; ++i)
        {
            if (arguments.size() == i)
                break;

            if (!isUnsignedInteger(arguments[i]) && !arguments[i]->onlyNull())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of the {} argument of function {}, expected unsigned integer or Null",
                    i + 1,
                    arguments[i]->getName(),
                    getName());
            }
        }

        for (size_t i = 2; i != 7; ++i)
        {
            if (arguments.size() <= i)
                break;

            if (!isUInt8(arguments[i]))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of the {} argument of function {}, expected UInt8",
                    i + 1,
                    arguments[i]->getName(),
                    getName());
            }
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t seed = randomSeed();
        size_t number_of_columns = 0;

        if (!arguments.empty() && !arguments[0].column->onlyNull())
        {
            number_of_columns = arguments[0].column->getUInt(0);
            if (number_of_columns > MAX_NUMBER_OF_COLUMNS)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Maximum allowed number of columns is {}, got {}",
                    MAX_NUMBER_OF_COLUMNS,
                    number_of_columns);
        }

        if (arguments.size() > 1 && !arguments[1].column->onlyNull())
            seed = arguments[1].column->getUInt(0);

        bool allow_big_numbers = true;
        if (arguments.size() > 2)
            allow_big_numbers = arguments[2].column->getBool(0);

        bool allow_enums = true;
        if (arguments.size() > 3)
            allow_enums = arguments[3].column->getBool(0);

        bool allow_decimals = true;
        if (arguments.size() > 4)
            allow_decimals = arguments[4].column->getBool(0);

        bool allow_ip = true;
        if (arguments.size() > 5)
            allow_ip = arguments[5].column->getBool(0);

        bool only_string_map_key = false;
        if (arguments.size() > 6)
            only_string_map_key = arguments[6].column->getBool(0);

        pcg64 rng(seed);
        if (number_of_columns == 0)
            number_of_columns = generateNumberOfColumns(rng);

        auto col_res = ColumnString::create();
        String generated_structure;
        for (size_t i = 0; i != number_of_columns; ++i)
        {
            if (i != 0)
                generated_structure += ", ";
            String column_name = "c" + std::to_string(i + 1);
            auto type = generateRandomType(column_name, rng, allow_big_numbers, allow_enums, allow_decimals, allow_ip, only_string_map_key);
            generated_structure += column_name + " " + type;
        }
        col_res->insert(generated_structure);
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:

    size_t generateNumberOfColumns(pcg64 & rng) const
    {
        return rng() % MAX_NUMBER_OF_COLUMNS + 1;
    }

    /// Helper struct to call generateRandomTypeImpl with lots of bool template arguments without writing big if/else over all bool variables.
    template<bool ...Args>
    struct Dispatcher
    {
        static auto call(const FunctionGenerateRandomStructure * f, const String & column_name, pcg64 & rng)
        {
            return f->generateRandomTypeImpl<Args...>(column_name, rng);
        }

        template<class ...Args1>
        static auto call(const FunctionGenerateRandomStructure * f, const String & column_name, pcg64 & rng, bool b, Args1... ar1)
        {
            if (b)
                return Dispatcher<Args..., true>::call(f, column_name, rng, ar1...);
            else
                return Dispatcher<Args..., false>::call(f, column_name, rng, ar1...);
        }

        friend FunctionGenerateRandomStructure;
    };

    String generateRandomType(const String & column_name, pcg64 & rng, bool allow_big_numbers, bool allow_enums, bool allow_decimals, bool allow_ip, bool allow_only_string_map_keys) const
    {
        return Dispatcher<>::call(this, column_name, rng, allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, true);
    }

    template <bool allow_big_numbers, bool allow_enums, bool allow_decimals, bool allow_ip, bool allow_only_string_map_keys, bool allow_complex_types>
    String generateRandomTypeImpl(const String & column_name, pcg64 & rng, size_t depth = 0) const
    {
        constexpr auto all_types = getAllTypes<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_complex_types>();
        auto type = all_types[rng() % all_types.size()];

        switch (type)
        {
            case Type::FixedString:
                return "FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")";
            case Type::DateTime64:
                return "DateTime64(" + std::to_string(rng() % MAX_DATETIME64_PRECISION) + ")";
            case Type::Decimal32:
                return "Decimal32(" + std::to_string(rng() % MAX_DECIMAL32_PRECISION) + ")";
            case Type::Decimal64:
                return "Decimal64(" + std::to_string(rng() % MAX_DECIMAL64_PRECISION) + ")";
            case Type::Decimal128:
                return "Decimal128(" + std::to_string(rng() % MAX_DECIMAL128_PRECISION) + ")";
            case Type::Decimal256:
                return "Decimal256(" + std::to_string(rng() % MAX_DECIMAL256_PRECISION) + ")";
            case Type::Enum8:
                return "Enum8(" + generateEnumValues(column_name, rng) + ")";
            case Type::Enum16:
                return "Enum16(" + generateEnumValues(column_name, rng) + ")";
            case Type::LowCardinality:
                return "LowCardinality(" + generateLowCardinalityNestedType(rng) + ")";
            case Type::Nullable:
            {
                auto nested_type = generateRandomTypeImpl<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, false>(column_name, rng, depth + 1);
                return "Nullable(" + nested_type + ")";
            }
            case Type::Array:
            {
                auto nested_type = generateRandomTypeImpl<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, true>(column_name, rng, depth + 1);
                return "Array(" + nested_type + ")";
            }
            case Type::Map:
            {
                auto key_type = generateMapKeyType<allow_only_string_map_keys>(rng);
                auto value_type = generateRandomTypeImpl<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, true>(column_name, rng, depth + 1);
                return "Map(" + key_type + ", " + value_type + ")";
            }
            case Type::Tuple:
            {
                size_t elements = rng() % MAX_TUPLE_ELEMENTS + 1;
                bool named_tuple = rng() % 2;
                String tuple_type = "Tuple(";
                for (size_t i = 0; i != elements; ++i)
                {
                    if (i != 0)
                        tuple_type += ", ";

                    String element_name = "e" + std::to_string(i + 1);
                    if (named_tuple)
                        tuple_type += element_name + " ";
                    tuple_type += generateRandomTypeImpl<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, true>(element_name, rng, depth + 1);
                }
                return tuple_type + ")";
            }
            case Type::Nested:
            {
                size_t elements = rng() % MAX_TUPLE_ELEMENTS + 1;
                String nested_type = "Nested(";
                for (size_t i = 0; i != elements; ++i)
                {
                    if (i != 0)
                        nested_type += ", ";
                    String element_name = "e" + std::to_string(i + 1);
                    auto element_type = generateRandomTypeImpl<allow_big_numbers, allow_enums, allow_decimals, allow_ip, allow_only_string_map_keys, true>(element_name, rng, depth + 1);
                    nested_type += element_name + " " + element_type;
                }
                return nested_type + ")";
            }
            default:
                return String(magic_enum::enum_name<Type>(type));
        }
    }

    template <bool allow_only_string_map_keys>
    String generateMapKeyType(pcg64 & rng) const
    {
        Type type;
        if constexpr (allow_only_string_map_keys)
            type = map_key_string_types[rng() % map_key_string_types.size()];
        else
            type = map_key_types[rng() % map_key_types.size()];

        if (type == Type::FixedString)
            return "FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")";
        return String(magic_enum::enum_name<Type>(type));
    }

    String generateLowCardinalityNestedType(pcg64 & rng) const
    {
        /// Support only String and FixedString.
        String nested_type;
        if (rng() % 2)
            nested_type = "String";
        else
            nested_type = "FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")";
        return rng() % 2 ? nested_type : "Nullable(" + nested_type + ")";
    }

    String generateEnumValues(const String & column_name, pcg64 & rng) const
    {
        /// Don't generate big enums, because it will lead to really big strings
        /// and slowness of this function, and it can lead to `Max query size exceeded`
        /// while using this function with generateRandom.
        ssize_t num_values = rng() % 16 + 1;
        String result;
        for (ssize_t i = 0; i != num_values; ++i)
        {
            if (i != 0)
                result += ", ";
            result += "'" + column_name + "V" + std::to_string(i) + "' = " + std::to_string(i);
        }
        return result;
    }

    template <bool allow_big_numbers, bool allow_enums, bool allow_decimals, bool allow_ip, bool allow_complex_types>
    static constexpr auto getAllTypes()
    {
        constexpr size_t big_integer_types_size = big_integer_types.size() * allow_big_numbers;
        constexpr size_t enum_types_size = enum_types.size() * allow_enums;
        constexpr size_t decimal_types_size = decimal_types.size() * allow_decimals;
        constexpr size_t big_decimal_types_size = big_decimal_types.size() * allow_big_numbers * allow_decimals;
        constexpr size_t ip_types_size = ip_types.size() * allow_ip;
        constexpr size_t complex_types_size = complex_types.size() * allow_complex_types;

        constexpr size_t result_size = simple_types.size() + big_integer_types_size + enum_types_size + decimal_types_size
            + big_decimal_types_size + ip_types_size + complex_types_size;
        std::array<Type, result_size> result;
        size_t index = 0;

        for (size_t i = 0; i != simple_types.size(); ++i, ++index)
            result[index] = simple_types[i];

        for (size_t i = 0; i != big_integer_types_size; ++i, ++index)
            result[index] = big_integer_types[i];

        for (size_t i = 0; i != enum_types_size; ++i, ++index)
            result[index] = enum_types[i];

        for (size_t i = 0; i != decimal_types_size; ++i, ++index)
            result[index] = decimal_types[i];

        for (size_t i = 0; i != big_decimal_types_size; ++i, ++index)
            result[index] = big_decimal_types[i];

        for (size_t i = 0; i != ip_types_size; ++i, ++index)
            result[index] = ip_types[i];

        for (size_t i = 0; i != complex_types_size; ++i, ++index)
            result[index] = complex_types[i];

        return result;
    }
};


REGISTER_FUNCTION(GenerateRandomStructure)
{
    factory.registerFunction<FunctionGenerateRandomStructure>(
        {
            R"(
Generates a random table structure.
This function takes 4 optional constant arguments:
1) the number of column in the result structure (random by default)
2) random seed (random by default)
3) flag that indicates if big number types can be used (true by default)
4) flag that indicates if enum types can be used (true by default)
5) flag that indicates if decimal types can be used (true by default)
6) flag that indicates if ip types (IPv4, IPv6) can be used (true by default)
7) flag that indicates if map keys should be only String or FixedString (false by default)
The maximum number of columns is 128.
The function returns a value of type String.
)",
            Documentation::Examples{
                {"random", "SELECT generateRandomStructure()"},
                {"with specified number of arguments", "SELECT generateRandomStructure(10)"},
                {"with specified seed", "SELECT generateRandomStructure(10, 42)"},
                {"without big number types", "SELECT generateRandomStructure(10, NULL, false)"},
                {"without enum types", "SELECT generateRandomStructure(10, NULL, true, false)"},
                {"without decimal types", "SELECT generateRandomStructure(10, NULL, true, true, false)"},
                {"without ip types", "SELECT generateRandomStructure(10, NULL, true, true, true, false)"},
                {"with only string mak key types", "SELECT generateRandomStructure(10, NULL, true, true, true, true, true)"},
            },
            Documentation::Categories{"Random"}
        },
        FunctionFactory::CaseSensitive);
}

}
