#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace
{

/// If 'is_utf8' - measure offset and length in code points instead of bytes.
class FunctionSubstring : public IFunction
{
public:
    FunctionSubstring(const char * name_, bool is_utf8_) : function_name(name_), is_utf8(is_utf8_) {}

    static FunctionPtr create(const char * name, bool is_utf8) { return std::make_shared<FunctionSubstring>(name, is_utf8); }
    String getName() const override { return function_name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments for function {} doesn't match: "
                            "passed {}, should be 2 or 3", getName(), number_of_arguments);

        if (is_utf8)
        {
            /// UTF8 variant is not available for FixedString and Enum arguments.
            if (!isString(arguments[0]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}, expected String",
                    arguments[0]->getName(), getName());
        }
        else
        {
            if (!isStringOrFixedString(arguments[0]) && !isEnum(arguments[0]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}, expected String, FixedString or Enum",
                    arguments[0]->getName(), getName());
        }

        if (!isNativeNumber(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected (U)Int*",
                arguments[1]->getName(), getName());

        if (number_of_arguments == 3 && !isNativeNumber(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected (U)Int*",
                arguments[2]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    ColumnPtr executeForSource(const ColumnPtr & column_offset, const ColumnPtr & column_length,
                          bool column_offset_const, bool column_length_const,
                          Int64 offset, Int64 length,
                          Source && source, size_t input_rows_count) const
    {
        auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_offset_const)
            {
                if (offset > 0)
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), static_cast<size_t>(offset - 1));
                else if (offset < 0)
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(offset));
                else
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Indices in strings are 1-based");
            }
            else
                sliceDynamicOffsetUnbounded(source, StringSink(*col_res, input_rows_count), *column_offset);
        }
        else
        {
            if (column_offset_const && column_length_const)
            {
                if (offset > 0)
                    sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), static_cast<size_t>(offset - 1), length);
                else if (offset < 0)
                    sliceFromRightConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), -static_cast<size_t>(offset), length);
                else
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Indices in strings are 1-based");
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, input_rows_count), *column_offset, *column_length);
        }

        return col_res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_offset = arguments[1].column;
        ColumnPtr column_length;
        if (number_of_arguments == 3)
            column_length = arguments[2].column;

        const ColumnConst * column_offset_const = checkAndGetColumn<ColumnConst>(column_offset.get());
        const ColumnConst * column_length_const = nullptr;
        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 offset = 0;
        Int64 length = 0;

        if (column_offset_const)
            offset = column_offset_const->getInt(0);
        if (column_length_const)
            length = column_length_const->getInt(0);

        if (is_utf8)
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            {
                bool all_ascii = isAllASCII(col->getChars().data(), col->getChars().size());
                if (all_ascii)
                    return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, StringSource(*col), input_rows_count);
                return executeForSource(
                    column_offset,
                    column_length,
                    column_offset_const,
                    column_length_const,
                    offset,
                    length,
                    UTF8StringSource(*col),
                    input_rows_count);
            }

            if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
            {
                std::string_view str_ref = col_const->getDataAt(0);
                bool all_ascii = isAllASCII(reinterpret_cast<const UInt8 *>(str_ref.data()), str_ref.size());
                if (all_ascii)
                    return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, ConstSource<StringSource>(*col_const), input_rows_count);
                return executeForSource(
                    column_offset,
                    column_length,
                    column_offset_const,
                    column_length_const,
                    offset,
                    length,
                    ConstSource<UTF8StringSource>(*col_const),
                    input_rows_count);
            }
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
        }
        else
        {
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
                return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, StringSource(*col), input_rows_count);
            if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_string.get()))
                return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, FixedStringSource(*col_fixed), input_rows_count);
            if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_string.get()))
                return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, ConstSource<StringSource>(*col_const), input_rows_count);
            if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
                return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, ConstSource<FixedStringSource>(*col_const_fixed), input_rows_count);
            if (isEnum(arguments[0].type))
            {
                if (const typename DataTypeEnum8::ColumnType * col_enum8 = checkAndGetColumn<typename DataTypeEnum8::ColumnType>(column_string.get()))
                {
                    const auto * type_enum8 = assert_cast<const DataTypeEnum8 *>(arguments[0].type.get());
                    return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, EnumSource<DataTypeEnum8>(*col_enum8, *type_enum8), input_rows_count);
                }
                if (const typename DataTypeEnum16::ColumnType * col_enum16 = checkAndGetColumn<typename DataTypeEnum16::ColumnType>(column_string.get()))
                {
                    const auto * type_enum16 = assert_cast<const DataTypeEnum16 *>(arguments[0].type.get());
                    return executeForSource(column_offset, column_length, column_offset_const, column_length_const, offset, length, EnumSource<DataTypeEnum16>(*col_enum16, *type_enum16), input_rows_count);
                }
            }

            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
        }
    }

private:
    const char * function_name;
    bool is_utf8;
};

}

REGISTER_FUNCTION(Substring)
{
    FunctionDocumentation::Description description = R"(
Returns the substring of a string `s` which starts at the specified byte index `offset`.
Byte counting starts from 1 with the following logic:
- If `offset` is `0`, an empty string is returned.
- If `offset` is negative, the substring starts `offset` characters from the end of the string, rather than from the beginning.

An optional argument `length` specifies the maximum number of bytes the returned substring may have.
)";
    FunctionDocumentation::Syntax syntax = "substring(s, offset[, length])";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to calculate a substring from.", {"String", "FixedString", "Enum"}},
        {"offset", "The starting position of the substring in `s`.", {"(U)Int*"}},
        {"length", "Optional. The maximum length of the substring.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a substring of `s` with `length` many bytes, starting at index `offset`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT 'database' AS db, substr(db, 5), substr(db, 5, 1)",
        R"(
┌─db───────┬─substring('database', 5)─┬─substring('database', 5, 1)─┐
│ database │ base                     │ b                           │
└──────────┴──────────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_utf8 = R"(
Returns the substring of a string `s` which starts at the specified code point index `offset`.
Code point counting starts from `1` with the following logic:
- If `offset` is `0`, an empty string is returned.
- If `offset` is negative, the substring starts `offset` code points from the end of the string, rather than from the beginning.

An optional argument `length` specifies the maximum number of code points the returned substring may have.

:::note
This function assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
:::
)";
    FunctionDocumentation::Syntax syntax_utf8 = "substringUTF8(s, offset[, length])";
    FunctionDocumentation::Arguments arguments_utf8 = {
        {"s", "The string to calculate a substring from.", {"String", "FixedString", "Enum"}},
        {"offset", "The starting position of the substring in `s`.", {"Int", "UInt"}},
        {"length", "The maximum length of the substring. Optional.", {"Int", "UInt"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_utf8 = {"Returns a substring of `s` with `length` many code points, starting at code point index `offset`.", {"String"}};
    FunctionDocumentation::Examples examples_utf8 = {
    {
        "Usage example",
        "SELECT 'Täglich grüßt das Murmeltier.' AS str, substringUTF8(str, 9), substringUTF8(str, 9, 5)",
        "Täglich grüßt das Murmeltier.    grüßt das Murmeltier.    grüßt"
    }
    };
    FunctionDocumentation documentation_utf8 = {description_utf8, syntax_utf8, arguments_utf8, {}, returned_value_utf8, examples_utf8, introduced_in, category};

    factory.registerFunction("substring",
        [](ContextPtr){ return FunctionSubstring::create("substring", false); },
        documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("substr", "substring", FunctionFactory::Case::Insensitive); // MySQL alias
    factory.registerAlias("mid", "substring", FunctionFactory::Case::Insensitive); /// MySQL alias
    factory.registerAlias("byteSlice", "substring", FunctionFactory::Case::Insensitive); /// resembles PostgreSQL's get_byte function, similar to ClickHouse's bitSlice

    factory.registerFunction("substringUTF8",
        [](ContextPtr){ return FunctionSubstring::create("substringUTF8", true); },
        documentation_utf8);
}

}
