#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "config.h"

#if USE_RAPIDJSON

/// Prevent stack overflow:
#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/reader.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionPrettyPrintJSON : public IFunction
{
public:
    static constexpr auto name = "prettyPrintJSON";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPrettyPrintJSON>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool canThrow(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"json", &isString, nullptr, "String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"indent", &isNativeUInt, isColumnConst, "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String column", getName());

        unsigned indent_count = 4;
        if (arguments.size() > 1)
        {
            UInt64 indent_value = assert_cast<const ColumnConst &>(*arguments[1].column).getValue<UInt64>();
            if (indent_value > 32)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Indent parameter of function {} must be between 0 and 32, got {}",
                    getName(),
                    indent_value);
            indent_count = static_cast<unsigned>(indent_value);
        }

        auto result = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str_view = col->getDataAt(i);

            /// Since RapidJSON uses '\0' as end-of-stream char in its stream abstraction,
            /// we have to default to this check to prevent silent truncation of the input
            /// unescaped '\0' is not valid in JSON strings anyway
            if (str_view.find('\0') != std::string_view::npos)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: embedded NULL byte",
                    getName());

            rapidjson::StringBuffer buffer;
            rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
            writer.SetIndent(' ', indent_count);

            /// Stream JSON directly from Reader to PrettyWriter without building
            /// a DOM. Both Reader (with kParseIterativeFlag) and PrettyWriter use
            /// heap-allocated stacks, so arbitrarily deep nesting is safe.
            rapidjson::MemoryStream ms(str_view.data(), str_view.size());
            rapidjson::Reader reader;
            auto parse_result = reader.Parse(ms, writer);

            if (parse_result.IsError())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: {}",
                    getName(),
                    rapidjson::GetParseError_En(parse_result.Code()));

            result->insertData(buffer.GetString(), buffer.GetSize());
        }

        return result;
    }
};

}

REGISTER_FUNCTION(PrettyPrintJSON)
{
    FunctionDocumentation::Description description = R"(
Returns a pretty-printed version of a JSON string with newlines and indentation with spaces.
    )";
    FunctionDocumentation::Syntax syntax = "prettyPrintJSON(json [, indent])";
    FunctionDocumentation::Arguments arguments = {
        {"json", "A valid JSON string to format.", {"String"}},
        {"indent", "Number of spaces per indentation level. Default: 4. Max: 32", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A pretty-printed JSON string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Simple object",
            R"(SELECT prettyPrintJSON('{"a":1,"b":"hello"}');)",
            R"(
{
    "a": 1,
    "b": "hello"
}
            )"
        },
        {
            "Custom indent",
            R"(SELECT prettyPrintJSON('{"a":1}', 8);)",
            R"(
{
        "a": 1
}
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrettyPrintJSON>(documentation);
}

}

#endif
