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
    
    static constexpr std::array<Type, 21> simple_types
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
        Type::DateTime64,
        Type::Decimal32,
        Type::Decimal64,
        Type::Date,
        Type::Date32,
        Type::DateTime,
        Type::String,
        Type::FixedString,
        Type::IPv4,
        Type::IPv6,
    };

    static constexpr std::array<Type, 6> big_number_types
    {
        Type::Int128,
        Type::UInt128,
        Type::Int256,
        Type::UInt256,
        Type::Decimal128,
        Type::Decimal256,
    };

    static constexpr std::array<Type, 2> enum_types
    {
        Type::Enum8,
        Type::Enum16,
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1, 2, 3}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected from 0 to 4",
                getName(), arguments.size());

        if (!arguments.empty() && !isUnsignedInteger(arguments[0]) && !arguments[0]->onlyNull())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the first argument of function {}, expected unsigned integer or Null",
                arguments[0]->getName(),
                getName());
        }

        if (arguments.size() > 1 && !isUnsignedInteger(arguments[1]) && !arguments[1]->onlyNull())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the second argument of function {}, expected unsigned integer or Null",
                arguments[1]->getName(),
                getName());
        }

        if (arguments.size() > 2 && !isUInt8(arguments[2]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the third argument of function {}, expected UInt8",
                arguments[2]->getName(),
                getName());
        }

        if (arguments.size() > 3 && !isUInt8(arguments[3]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the fourth argument of function {}, expected UInt8",
                arguments[3]->getName(),
                getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t seed = randomSeed();
        size_t number_of_columns = 0;

        if (!arguments.empty() && !arguments[0].column->onlyNull())
        {
            const auto & first_arg = arguments[0];
            number_of_columns = first_arg.column->getUInt(0);
            if (number_of_columns > MAX_NUMBER_OF_COLUMNS)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Maximum allowed number of columns is {}, got {}",
                    MAX_NUMBER_OF_COLUMNS,
                    number_of_columns);
        }

        if (arguments.size() > 1 && !arguments[1].column->onlyNull())
        {
            const auto & second_arg = arguments[1];
            seed = second_arg.column->getUInt(0);
        }

        bool allow_big_numbers = true;
        if (arguments.size() > 2)
        {
            const auto & third_arg = arguments[2];
            allow_big_numbers = third_arg.column->getBool(0);
        }

        bool allow_enums = true;
        if (arguments.size() > 3)
        {
            const auto & fourth_arg = arguments[3];
            allow_enums = fourth_arg.column->getBool(0);
        }

        pcg64 rng(seed);
        if (number_of_columns == 0)
            number_of_columns = generateNumberOfColumns(rng);

        auto col_res = ColumnString::create();
        String generated_structure = "";
        for (size_t i = 0; i != number_of_columns; ++i)
        {
            if (i != 0)
                generated_structure += ", ";
            auto type = generateRandomType(rng, allow_big_numbers, allow_enums);
            generated_structure += "c" + std::to_string(i + 1) + " " + type;
        }
        col_res->insert(generated_structure);
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:

    size_t generateNumberOfColumns(pcg64 & rng) const
    {
        return rng() % MAX_NUMBER_OF_COLUMNS + 1;
    }
    
    String generateRandomType(pcg64 & rng, bool allow_big_numbers, bool allow_enums) const
    {
        if (allow_big_numbers)
        {
            if (allow_enums)
                return generateRandomTypeImpl<true, true, true>(rng);
            return generateRandomTypeImpl<true, false, true>(rng);
        }

        if (allow_enums)
            return generateRandomTypeImpl<false, true, true>(rng);
        return generateRandomTypeImpl<false, false, true>(rng);
    }
        

    template <bool allow_big_numbers, bool allow_enums, bool allow_complex_types>
    String generateRandomTypeImpl(pcg64 & rng, size_t depth = 0) const
    {
        constexpr auto all_types = getAllTypes<allow_big_numbers, allow_enums, allow_complex_types>();
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
                return "Enum8(" + generateEnumValues(rng) + ")";
            case Type::Enum16:
                return "Enum16(" + generateEnumValues(rng) + ")";
            case Type::LowCardinality:
                return "LowCardinality(" + generateLowCardinalityNestedType(rng) + ")";
            case Type::Nullable:
                return "Nullable(" +  generateRandomTypeImpl<allow_big_numbers, allow_enums, false>(rng, depth + 1) + ")";
            case Type::Array:
                return "Array(" + generateRandomTypeImpl<allow_big_numbers, allow_enums, true>(rng, depth + 1) + ")";
            case Type::Map:
                return "Map(" + generateMapKeyType(rng) + ", " + generateRandomTypeImpl<allow_big_numbers, allow_enums, true>(rng, depth + 1) + ")";
            case Type::Tuple:
            {
                size_t elements = rng() % MAX_TUPLE_ELEMENTS + 1;
                bool named_tuple = rng() % 2;
                String tuple_type = "Tuple(";
                for (size_t i = 0; i != elements; ++i)
                {
                    if (i != 0)
                        tuple_type += ", ";
                    if (named_tuple)
                        tuple_type += "e" + std::to_string(i + 1) + " ";
                    tuple_type += generateRandomTypeImpl<allow_big_numbers, allow_enums, true>(rng, depth + 1);
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
                    nested_type += "e" + std::to_string(i + 1) + " " + generateRandomTypeImpl<allow_big_numbers, allow_enums, true>(rng, depth + 1);
                }
                return nested_type + ")";
            }
            default:
                return String(magic_enum::enum_name<Type>(type));
        }
    }

    String generateMapKeyType(pcg64 & rng) const
    {
        auto type = map_key_types[rng() % map_key_types.size()];
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

    String generateEnumValues(pcg64 & rng) const
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
            result += "'v" + std::to_string(i) + "' = " + std::to_string(i);
        }
        return result;
    }

    template <bool allow_big_numbers, bool allow_enums, bool allow_complex_types>
    static constexpr auto getAllTypes()
    {
        constexpr size_t result_size = simple_types.size() + big_number_types.size() * allow_big_numbers + enum_types.size() * allow_enums + complex_types.size() * allow_complex_types;
        std::array<Type, result_size> result;
        size_t index = 0;
        for (size_t i = 0; i != simple_types.size(); ++i, ++index)
            result[index] = simple_types[i];
        
        if constexpr (allow_big_numbers)
        {
            for (size_t i = 0; i != big_number_types.size(); ++i, ++index)
                result[index] = big_number_types[i];
        }
        
        if constexpr (allow_enums)
        {
            for (size_t i = 0; i != enum_types.size(); ++i, ++index)
                result[index] = enum_types[i];
        }
        
        if constexpr (allow_complex_types)
        {
            for (size_t i = 0; i != complex_types.size(); ++i, ++index)
                result[index] = complex_types[i];
        }   
        
        return result;
    }
};


REGISTER_FUNCTION(GenerateRandomStructure)
{
    factory.registerFunction<FunctionGenerateRandomStructure>(
        {
            R"(
Generates a random table structure.
This function takes an optional constant argument, the number of column in the result structure.
If argument is now specified, the number of columns is random. The maximum number of columns is 1024.
The function returns a value of type String.
)",
            Documentation::Examples{
                {"random", "SELECT generateRandomStructure()"},
                {"with specified number of arguments", "SELECT generateRandomStructure(10)"}},
            Documentation::Categories{"Random"}
        },
        FunctionFactory::CaseSensitive);
}

}
