#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace
{
    class FunctionToJSONString : public IFunction
    {
    public:
        static constexpr auto name = "toJSONString";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionToJSONString>(context); }

        explicit FunctionToJSONString(ContextPtr context) : format_settings(getFormatSettings(context)) {}

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeString>(); }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeString>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            auto res = ColumnString::create();
            ColumnString::Chars & data_to = res->getChars();
            ColumnString::Offsets & offsets_to = res->getOffsets();
            offsets_to.resize(input_rows_count);

            auto serializer = arguments[0].type->getDefaultSerialization();
            WriteBufferFromVector<ColumnString::Chars> json(data_to);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                serializer->serializeTextJSON(*arguments[0].column, i, json, format_settings);
                offsets_to[i] = json.count();
            }

            json.finalize();
            return res;
        }

    private:
        /// Affects only subset of part of settings related to json.
        const FormatSettings format_settings;
    };
}

REGISTER_FUNCTION(ToJSONString)
{
    /// toJSONString documentation
    FunctionDocumentation::Description description = R"(
Serializes a value to its JSON representation. Various data types and nested structures are supported.
64-bit [integers](../data-types/int-uint.md) or bigger (like `UInt64` or `Int128`) are enclosed in quotes by default. [output_format_json_quote_64bit_integers](/operations/settings/formats#output_format_json_quote_64bit_integers) controls this behavior.
Special values `NaN` and `inf` are replaced with `null`. Enable [output_format_json_quote_denormals](/operations/settings/formats#output_format_json_quote_denormals) setting to show them.
When serializing an [Enum](../data-types/enum.md) value, the function outputs its name.

See also:
- [output_format_json_quote_64bit_integers](/operations/settings/formats#output_format_json_quote_64bit_integers)
- [output_format_json_quote_denormals](/operations/settings/formats#output_format_json_quote_denormals)
    )";
    FunctionDocumentation::Syntax syntax = "toJSONString(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Value to serialize. Value may be of any data type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the JSON representation of the value.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Map serialization",
        R"(
SELECT toJSONString(map('key1', 1, 'key2', 2));
        )",
        R"(
┌─toJSONString(map('key1', 1, 'key2', 2))─┐
│ {"key1":1,"key2":2}                     │
└─────────────────────────────────────────┘
        )"
    },
    {
        "Special values",
        R"(
SELECT toJSONString(tuple(1.25, NULL, NaN, +inf, -inf, [])) SETTINGS output_format_json_quote_denormals = 1;
        )",
        R"(
┌─toJSONString(tuple(1.25, NULL, NaN, plus(inf), minus(inf), []))─┐
│ [1.25,null,"nan","inf","-inf",[]]                               │
└─────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToJSONString>(documentation);
}

}
