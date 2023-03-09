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
    enum class SimpleTypes
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
        Int128,
        UInt128,
        Int256,
        UInt256,
        Float32,
        Float64,
        DateTime64,
        Decimal32,
        Decimal64,
        Decimal128,
        Decimal256,
        Date,
        Date32,
        DateTime,
        String,
        FixedString,
        Enum8,
        Enum16,
        IPv4,
        IPv6,
    };

    enum class ComplexTypes
    {
        Nullable,
        LowCardinality,
        Array,
        Tuple,
        Map,
        Nested,
    };

    enum class MapKeyTypes
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
        Int128,
        UInt128,
        Int256,
        UInt256,
        Date,
        Date32,
        DateTime,
        String,
        FixedString,
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0, 1 or 2.",
                getName(), arguments.size());

        if (arguments.size() > 1 && !isUnsignedInteger(arguments[0]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the first argument of function {}, expected unsigned integer",
                arguments[0]->getName(),
                getName());
        }

        if (arguments.size() > 2 && !isUnsignedInteger(arguments[1]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the second argument of function {}, expected unsigned integer",
                arguments[1]->getName(),
                getName());
        }

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t seed = randomSeed();
        size_t number_of_columns = 0;

        if (!arguments.empty())
        {
            const auto & first_arg = arguments[0];

            if (!isUnsignedInteger(first_arg.type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of the first argument of function {}, expected unsigned integer",
                    first_arg.type->getName(),
                    getName());

            number_of_columns = first_arg.column->getUInt(0);
            if (number_of_columns > MAX_NUMBER_OF_COLUMNS)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Maximum allowed number of columns is {}, got {}", MAX_NUMBER_OF_COLUMNS, number_of_columns);

            if (arguments.size() == 2)
            {
                const auto & second_arg = arguments[1];

                if (!isUnsignedInteger(second_arg.type))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of the second argument of function {}, expected unsigned integer",
                        second_arg.type->getName(),
                        getName());

                seed = second_arg.column->getUInt(0);
            }
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
            auto type = generateRandomType(rng);
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

    String generateRandomType(pcg64 & rng, bool allow_complex_types = true, size_t depth = 0) const
    {
        constexpr size_t simple_types_size = magic_enum::enum_count<SimpleTypes>();
        constexpr size_t complex_types_size = magic_enum::enum_count<ComplexTypes>();
        size_t type_index;
        if (allow_complex_types)
            type_index = rng() % (simple_types_size + complex_types_size);
        else
            type_index = rng() % simple_types_size;

        if (type_index < simple_types_size)
        {
            auto type = magic_enum::enum_value<SimpleTypes>(type_index);
            switch (type)
            {
                case SimpleTypes::FixedString:
                    return "FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")";
                case SimpleTypes::DateTime64:
                    return "DateTime64(" + std::to_string(rng() % MAX_DATETIME64_PRECISION) + ")";
                case SimpleTypes::Decimal32:
                    return "Decimal32(" + std::to_string(rng() % MAX_DECIMAL32_PRECISION) + ")";
                case SimpleTypes::Decimal64:
                    return "Decimal64(" + std::to_string(rng() % MAX_DECIMAL64_PRECISION) + ")";
                case SimpleTypes::Decimal128:
                    return "Decimal128(" + std::to_string(rng() % MAX_DECIMAL128_PRECISION) + ")";
                case SimpleTypes::Decimal256:
                    return "Decimal256(" + std::to_string(rng() % MAX_DECIMAL256_PRECISION) + ")";
                case SimpleTypes::Enum8:
                    return "Enum8(" + generateEnumValues(rng) + ")";
                case SimpleTypes::Enum16:
                    return "Enum16(" + generateEnumValues(rng) + ")";
                default:
                    return String(magic_enum::enum_name<SimpleTypes>(type));
            }
        }

        auto complex_type = magic_enum::enum_value<ComplexTypes>(type_index - simple_types_size);
        switch (complex_type)
        {
            case ComplexTypes::LowCardinality:
                return "LowCardinality(" + generateLowCardinalityNestedType(rng) + ")";
            case ComplexTypes::Nullable:
                return "Nullable(" +  generateRandomType(rng, false, depth + 1) + ")";
            case ComplexTypes::Array:
                return "Array(" + generateRandomType(rng, true, depth + 1) + ")";
            case ComplexTypes::Map:
                return "Map(" + generateMapKeyType(rng) + ", " + generateRandomType(rng, true, depth + 1) + ")";
            case ComplexTypes::Tuple:
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
                    tuple_type += generateRandomType(rng, true, depth + 1);
                }
                return tuple_type + ")";
            }
            case ComplexTypes::Nested:
            {
                size_t elements = rng() % MAX_TUPLE_ELEMENTS + 1;
                String nested_type = "Nested(";
                for (size_t i = 0; i != elements; ++i)
                {
                    if (i != 0)
                        nested_type += ", ";
                    nested_type += "e" + std::to_string(i + 1) + " " + generateRandomType(rng, true, depth + 1);
                }
                return nested_type + ")";
            }
        }
    }

    String generateMapKeyType(pcg64 & rng) const
    {
        constexpr size_t map_keys_types_size = magic_enum::enum_count<MapKeyTypes>();
        auto type = magic_enum::enum_value<MapKeyTypes>(rng() % map_keys_types_size);
        if (type == MapKeyTypes::FixedString)
            return "FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")";
        return String(magic_enum::enum_name<MapKeyTypes>(type));
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
