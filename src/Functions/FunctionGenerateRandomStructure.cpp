#include <Functions/FunctionGenerateRandomStructure.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Interpreters/Context.h>
#include <Common/randomSeed.h>
#include <Common/FunctionDocumentation.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>

#include <pcg_random.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_suspicious_low_cardinality_types;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    const size_t MAX_NUMBER_OF_COLUMNS = 128;
    const size_t MAX_TUPLE_ELEMENTS = 16;
    const size_t MAX_DATETIME64_PRECISION = 9;
    const size_t MAX_DECIMAL32_PRECISION = 9;
    const size_t MAX_DECIMAL64_PRECISION = 18;
    const size_t MAX_DECIMAL128_PRECISION = 38;
    const size_t MAX_DECIMAL256_PRECISION = 76;
    const size_t MAX_DEPTH = 16;

    constexpr std::array<TypeIndex, 29> simple_types
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

    constexpr std::array<TypeIndex, 5> complex_types
    {
        TypeIndex::Nullable,
        TypeIndex::LowCardinality,
        TypeIndex::Array,
        TypeIndex::Tuple,
        TypeIndex::Map,
    };

    constexpr std::array<TypeIndex, 22> map_key_types
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
        TypeIndex::Enum8,
        TypeIndex::Enum16,
        TypeIndex::UUID,
        TypeIndex::LowCardinality,
    };

    constexpr std::array<TypeIndex, 22> suspicious_lc_types
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
        TypeIndex::Date,
        TypeIndex::Date32,
        TypeIndex::DateTime,
        TypeIndex::String,
        TypeIndex::FixedString,
        TypeIndex::IPv4,
        TypeIndex::IPv6,
        TypeIndex::UUID,
    };

    template <bool allow_complex_types>
    constexpr auto getAllTypes()
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

    size_t generateNumberOfColumns(pcg64 & rng)
    {
        return rng() % MAX_NUMBER_OF_COLUMNS + 1;
    }

    void writeLowCardinalityNestedType(pcg64 & rng, WriteBuffer & buf, bool allow_suspicious_lc_types)
    {
        bool make_nullable = rng() % 2;
        if (make_nullable)
            writeCString("Nullable(", buf);

        if (allow_suspicious_lc_types)
        {
            TypeIndex type = suspicious_lc_types[rng() % suspicious_lc_types.size()];

            if (type == TypeIndex::FixedString)
                writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
            else
                writeString(magic_enum::enum_name<TypeIndex>(type), buf);
        }
        else
        {
            /// Support only String and FixedString.
            if (rng() % 2)
                writeCString("String", buf);
            else
                writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
        }

        if (make_nullable)
            writeChar(')', buf);
    }

    void writeEnumValues(const String & column_name, pcg64 & rng, WriteBuffer & buf, ssize_t max_value)
    {
        /// Don't generate big enums, because it will lead to really big result
        /// and slowness of this function, and it can lead to `Max query size exceeded`
        /// while using this function with generateRandom.
        size_t num_values = rng() % 16 + 1;
        std::vector<Int16> values(num_values);

        /// Generate random numbers from range [-(max_value + 1), max_value - num_values + 1].
        for (Int16 & x : values)
            x = rng() % (2 * max_value + 3 - num_values) - max_value - 1;
        /// Make all numbers unique.
        std::sort(values.begin(), values.end());
        for (size_t i = 0; i < num_values; ++i)
            values[i] += i;
        std::shuffle(values.begin(), values.end(), rng);
        for (size_t i = 0; i != num_values; ++i)
        {
            if (i != 0)
                writeCString(", ", buf);
            writeString("'" + column_name + "V" + std::to_string(i) + "' = " + std::to_string(values[i]), buf);
        }
    }

    void writeMapKeyType(const String & column_name, pcg64 & rng, WriteBuffer & buf)
    {
        TypeIndex type = map_key_types[rng() % map_key_types.size()];
        switch (type)
        {
            case TypeIndex::FixedString:
                writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
                break;
            case TypeIndex::LowCardinality:
                writeCString("LowCardinality(", buf);
                /// Map key supports only String and FixedString inside LowCardinality.
                if (rng() % 2)
                    writeCString("String", buf);
                else
                    writeString("FixedString(" + std::to_string(rng() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1) + ")", buf);
                writeChar(')', buf);
                break;
            case TypeIndex::Enum8:
                writeCString("Enum8(", buf);
                writeEnumValues(column_name, rng, buf, INT8_MAX);
                writeChar(')', buf);
                break;
            case TypeIndex::Enum16:
                writeCString("Enum16(", buf);
                writeEnumValues(column_name, rng, buf, INT16_MAX);
                writeChar(')', buf);
                break;
            default:
                writeString(magic_enum::enum_name<TypeIndex>(type), buf);
                break;
        }
    }

    template <bool allow_complex_types = true>
    void writeRandomType(const String & column_name, pcg64 & rng, WriteBuffer & buf, bool allow_suspicious_lc_types, size_t depth = 0)
    {
        if (allow_complex_types && depth > MAX_DEPTH)
            writeRandomType<false>(column_name, rng, buf, depth);

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
                writeString("DateTime64(" + std::to_string(rng() % MAX_DATETIME64_PRECISION + 1) + ")", buf);
                return;
            case TypeIndex::Decimal32:
                writeString("Decimal32(" + std::to_string(rng() % MAX_DECIMAL32_PRECISION + 1) + ")", buf);
                return;
            case TypeIndex::Decimal64:
                writeString("Decimal64(" + std::to_string(rng() % MAX_DECIMAL64_PRECISION + 1) + ")", buf);
                return;
            case TypeIndex::Decimal128:
                writeString("Decimal128(" + std::to_string(rng() % MAX_DECIMAL128_PRECISION + 1) + ")", buf);
                return;
            case TypeIndex::Decimal256:
                writeString("Decimal256(" + std::to_string(rng() % MAX_DECIMAL256_PRECISION + 1) + ")", buf);
                return;
            case TypeIndex::Enum8:
                writeCString("Enum8(", buf);
                writeEnumValues(column_name, rng, buf, INT8_MAX);
                writeChar(')', buf);
                return;
            case TypeIndex::Enum16:
                writeCString("Enum16(", buf);
                writeEnumValues(column_name, rng, buf, INT16_MAX);
                writeChar(')', buf);
                return;
            case TypeIndex::LowCardinality:
                writeCString("LowCardinality(", buf);
                writeLowCardinalityNestedType(rng, buf, allow_suspicious_lc_types);
                writeChar(')', buf);
                return;
            case TypeIndex::Nullable:
            {
                writeCString("Nullable(", buf);
                writeRandomType<false>(column_name, rng, buf, allow_suspicious_lc_types, depth + 1);
                writeChar(')', buf);
                return;
            }
            case TypeIndex::Array:
            {
                writeCString("Array(", buf);
                writeRandomType(column_name, rng, buf, allow_suspicious_lc_types, depth + 1);
                writeChar(')', buf);
                return;
            }
            case TypeIndex::Map:
            {
                writeCString("Map(", buf);
                writeMapKeyType(column_name, rng, buf);
                writeCString(", ", buf);
                writeRandomType(column_name, rng, buf, allow_suspicious_lc_types, depth + 1);
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
                    writeRandomType(element_name, rng, buf, allow_suspicious_lc_types, depth + 1);
                }
                writeChar(')', buf);
                return;
            }
            default:
                writeString(magic_enum::enum_name<TypeIndex>(type), buf);
                return;
        }
    }

    void writeRandomStructure(pcg64 & rng, size_t number_of_columns, WriteBuffer & buf, bool allow_suspicious_lc_types)
    {
        for (size_t i = 0; i != number_of_columns; ++i)
        {
            if (i != 0)
                writeCString(", ", buf);
            String column_name = "c" + std::to_string(i + 1);
            writeString(column_name, buf);
            writeChar(' ', buf);
            writeRandomType(column_name, rng, buf, allow_suspicious_lc_types);
        }
    }
}


FunctionPtr FunctionGenerateRandomStructure::create(DB::ContextPtr context)
{
    return std::make_shared<FunctionGenerateRandomStructure>(context->getSettingsRef()[Setting::allow_suspicious_low_cardinality_types].value);
}

DataTypePtr FunctionGenerateRandomStructure::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() > 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, expected from 0 to 2",
            getName(), arguments.size());


    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (!isUInt(arguments[i]) && !arguments[i]->onlyNull())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the {} argument of function {}, expected unsigned integer or Null",
                arguments[i]->getName(),
                i + 1,
                getName());
        }
    }

    return std::make_shared<DataTypeString>();
}

ColumnPtr FunctionGenerateRandomStructure::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
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
    {
        auto buf = WriteBufferFromVector<ColumnString::Chars>(chars);
        writeRandomStructure(rng, number_of_columns, buf, allow_suspicious_lc_types);
    }
    string_column.getOffsets().push_back(chars.size());
    return ColumnConst::create(std::move(col_res), input_rows_count);
}

String FunctionGenerateRandomStructure::generateRandomStructure(size_t seed, const ContextPtr & context)
{
    pcg64 rng(seed);
    size_t number_of_columns = generateNumberOfColumns(rng);
    WriteBufferFromOwnString buf;
    writeRandomStructure(rng, number_of_columns, buf, context->getSettingsRef()[Setting::allow_suspicious_low_cardinality_types]);
    return buf.str();
}

REGISTER_FUNCTION(GenerateRandomStructure)
{
    FunctionDocumentation::Description description = R"(
Generates random table structure in the format `column1_name column1_type, column2_name column2_type, ...`.
)";
    FunctionDocumentation::Syntax syntax = "generateRandomStructure([number_of_columns, seed])";
    FunctionDocumentation::Arguments arguments = {
        {"number_of_columns", "The desired number of columns in the resultant table structure. If set to 0 or `Null`, the number of columns will be random from 1 to 128. Default value: `Null`.", {"UInt64"}},
        {"seed", "Random seed to produce stable results. If seed is not specified or set to `Null`, it is randomly generated.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Randomly generated table structure.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT generateRandomStructure()
        )",
        R"(
c1 Decimal32(5), c2 Date, c3 Tuple(LowCardinality(String), Int128, UInt64, UInt16, UInt8, IPv6), c4 Array(UInt128), c5 UInt32, c6 IPv4, c7 Decimal256(64), c8 Decimal128(3), c9 UInt256, c10 UInt64, c11 DateTime
        )"
    },
    {
        "with specified number of columns",
        R"(
SELECT generateRandomStructure(1)
        )",
        R"(
c1 Map(UInt256, UInt16)
        )"
    },
    {
        "with specified seed",
        R"(
SELECT generateRandomStructure(NULL, 33)
        )",
        R"(
c1 DateTime, c2 Enum8('c2V0' = 0, 'c2V1' = 1, 'c2V2' = 2, 'c2V3' = 3), c3 LowCardinality(Nullable(FixedString(30))), c4 Int16, c5 Enum8('c5V0' = 0, 'c5V1' = 1, 'c5V2' = 2, 'c5V3' = 3), c6 Nullable(UInt8), c7 String, c8 Nested(e1 IPv4, e2 UInt8, e3 UInt16, e4 UInt16, e5 Int32, e6 Map(Date, Decimal256(70)))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateRandomStructure>(documentation);
}

}
