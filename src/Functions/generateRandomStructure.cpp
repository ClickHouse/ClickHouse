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
    static constexpr std::array<TypeIndex, 29> simple_types
    {
        TypeIndex::Int8,
        TypeIndex::UInt8,
        TypeIndex::Int16,
        TypeIndex::UInt16,
        TypeIndex::Int32,
        TypeIndex::UInt32,
        TypeIndex::Int64,
        TypeIndex::UInt64,
        TypeIndex::Int128,
        TypeIndex::UInt128,
        TypeIndex::Int256,
        TypeIndex::UInt256,
        TypeIndex::Float32,
        TypeIndex::Float64,
        TypeIndex::Decimal32,
        TypeIndex::Decimal64,
        TypeIndex::Decimal128,
        TypeIndex::Decimal256,
        TypeIndex::Date,
        TypeIndex::Date32,
        TypeIndex::DateTime,
        TypeIndex::DateTime64,
        TypeIndex::String,
        TypeIndex::FixedString,
        TypeIndex::Enum8,
        TypeIndex::Enum16,
        TypeIndex::IPv4,
        TypeIndex::IPv6,
        TypeIndex::UUID,
    };

    static constexpr std::array<TypeIndex, 5> complex_types
    {
        TypeIndex::Nullable,
        TypeIndex::LowCardinality,
        TypeIndex::Array,
        TypeIndex::Tuple,
        TypeIndex::Map,
    };

    static constexpr std::array<TypeIndex, 19> map_key_types
    {
        TypeIndex::Int8,
        TypeIndex::UInt8,
        TypeIndex::Int16,
        TypeIndex::UInt16,
        TypeIndex::Int32,
        TypeIndex::UInt32,
        TypeIndex::Int64,
        TypeIndex::UInt64,
        TypeIndex::Int128,
        TypeIndex::UInt128,
        TypeIndex::Int256,
        TypeIndex::UInt256,
        TypeIndex::Date,
        TypeIndex::Date32,
        TypeIndex::DateTime,
        TypeIndex::String,
        TypeIndex::FixedString,
        TypeIndex::IPv4,
        TypeIndex::UUID,
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
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected from 0 to 2",
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

        pcg64 rng(seed);
        if (number_of_columns == 0)
            number_of_columns = generateNumberOfColumns(rng);

        auto col_res = ColumnString::create();
        auto & string_column = assert_cast<ColumnString &>(*col_res);
        auto & chars = string_column.getChars();
        WriteBufferFromVector buf(chars);
        for (size_t i = 0; i != number_of_columns; ++i)
        {
            if (i != 0)
                writeCString(", ", buf);
            String column_name = "c" + std::to_string(i + 1);
            writeString(column_name, buf);
            writeChar(' ', buf);
            writeRandomType(column_name, rng, buf);
        }

        buf.finalize();
        chars.push_back(0);
        string_column.getOffsets().push_back(chars.size());
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:

    size_t generateNumberOfColumns(pcg64 & rng) const
    {
        return rng() % MAX_NUMBER_OF_COLUMNS + 1;
    }

    template <bool allow_complex_types = true>
    void writeRandomType(const String & column_name, pcg64 & rng, WriteBuffer & buf, size_t depth = 0) const
    {
        constexpr auto all_types = getAllTypes<allow_complex_types>();
        auto type = all_types[rng() % all_types.size()];

        switch (type)
        {
            case TypeIndex::UInt8:
                if (rng() % 2)
                    writeCString("UInt8", buf);
                else
                    writeCString("Bool", buf);
                return;
            case TypeIndex::FixedString:
                writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
                return;
            case TypeIndex::DateTime64:
                writeString("DateTime64(" + std::to_string(rng() % MAX_DATETIME64_PRECISION) + ")", buf);
                return;
            case TypeIndex::Decimal32:
                writeString("Decimal32(" + std::to_string(rng() % MAX_DECIMAL32_PRECISION) + ")", buf);
                return;
            case TypeIndex::Decimal64:
                writeString("Decimal64(" + std::to_string(rng() % MAX_DECIMAL64_PRECISION) + ")", buf);
                return;
            case TypeIndex::Decimal128:
                writeString("Decimal128(" + std::to_string(rng() % MAX_DECIMAL128_PRECISION) + ")", buf);
                return;
            case TypeIndex::Decimal256:
                writeString("Decimal256(" + std::to_string(rng() % MAX_DECIMAL256_PRECISION) + ")", buf);
                return;
            case TypeIndex::Enum8:
                writeCString("Enum8(", buf);
                writeEnumValues(column_name, rng, buf);
                writeChar(')', buf);
                return;
            case TypeIndex::Enum16:
                writeCString("Enum16(", buf);
                writeEnumValues(column_name, rng, buf);
                writeChar(')', buf);
                return;
            case TypeIndex::LowCardinality:
                writeCString("LowCardinality(", buf);
                writeLowCardinalityNestedType(rng, buf);
                writeChar(')', buf);
                return;
            case TypeIndex::Nullable:
            {
                writeCString("Nullable(", buf);
                writeRandomType<false>(column_name, rng, buf, depth + 1);
                writeChar(')', buf);
                return;
            }
            case TypeIndex::Array:
            {
                writeCString("Array(", buf);
                writeRandomType(column_name, rng, buf, depth + 1);
                writeChar(')', buf);
                return;
            }
            case TypeIndex::Map:
            {
                writeCString("Map(", buf);
                writeMapKeyType(rng, buf);
                writeCString(", ", buf);
                writeRandomType(column_name, rng, buf, depth + 1);
                writeChar(')', buf);
                return;
            }
            case TypeIndex::Tuple:
            {
                size_t elements = rng() % MAX_TUPLE_ELEMENTS + 1;
                bool generate_nested = rng() % 2;
                bool generate_named_tuple = rng() % 2;
                if (generate_nested)
                    writeCString("Nested(", buf);
                else
                    writeCString("Tuple(", buf);

                for (size_t i = 0; i != elements; ++i)
                {
                    if (i != 0)
                        writeCString(", ", buf);

                    String element_name = "e" + std::to_string(i + 1);
                    if (generate_named_tuple || generate_nested)
                    {
                        writeString(element_name, buf);
                        writeChar(' ', buf);
                    }
                    writeRandomType(element_name, rng, buf, depth + 1);
                }
                writeChar(')', buf);
                return;
            }
            default:
                writeString(magic_enum::enum_name<TypeIndex>(type), buf);
                return;
        }
    }

    void writeMapKeyType(pcg64 & rng, WriteBuffer & buf) const
    {
        TypeIndex type = map_key_types[rng() % map_key_types.size()];
        if (type == TypeIndex::FixedString)
            writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
        else
            writeString(magic_enum::enum_name<TypeIndex>(type), buf);
    }

    void writeLowCardinalityNestedType(pcg64 & rng, WriteBuffer & buf) const
    {
        /// Support only String and FixedString (maybe Nullable).
        String nested_type;
        bool make_nullable = rng() % 2;
        if (make_nullable)
            writeCString("Nullable(", buf);

        if (rng() % 2)
            writeCString("String", buf);
        else
            writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);

        if (make_nullable)
            writeChar(')', buf);
    }

    void writeEnumValues(const String & column_name, pcg64 & rng, WriteBuffer & buf) const
    {
        /// Don't generate big enums, because it will lead to really big result
        /// and slowness of this function, and it can lead to `Max query size exceeded`
        /// while using this function with generateRandom.
        ssize_t num_values = rng() % 16 + 1;
        for (ssize_t i = 0; i != num_values; ++i)
        {
            if (i != 0)
                writeCString(", ", buf);
            writeString("'" + column_name + "V" + std::to_string(i) + "' = " + std::to_string(i), buf);
        }
    }

    template <bool allow_complex_types>
    static constexpr auto getAllTypes()
    {
        constexpr size_t complex_types_size = complex_types.size() * allow_complex_types;
        constexpr size_t result_size = simple_types.size() + complex_types_size;
        std::array<TypeIndex, result_size> result;
        size_t index = 0;

        for (size_t i = 0; i != simple_types.size(); ++i, ++index)
            result[index] = simple_types[i];

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
This function takes 2 optional constant arguments:
the number of columns in the result structure (random by default) and random seed (random by default)
The maximum number of columns is 128.
The function returns a value of type String.
)",
            Documentation::Examples{
                {"random", "SELECT generateRandomStructure()"},
                {"with specified number of columns", "SELECT generateRandomStructure(10)"},
                {"with specified seed", "SELECT generateRandomStructure(10, 42)"},
            },
            Documentation::Categories{"Random"}
        },
        FunctionFactory::CaseSensitive);
}

}
