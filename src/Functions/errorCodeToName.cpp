#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/ErrorCodes.h>

namespace DB
{

/** errorCodeToName() - returns the variable name for the error code.
  */
class FunctionErrorCodeToName final : public IFunction
{
public:
    static constexpr auto name = "errorCodeToName";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionErrorCodeToName>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    String getSignatureString() const override
    {
        return "(Number) -> LowCardinality(String)";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t input_rows_count) const override
    {
        const auto & input_column = *arguments[0].column;
        auto col_res = res_type->createColumn();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Int64 error_code = input_column.getInt(i);
            std::string_view error_name =
                ErrorCodes::getName(static_cast<ErrorCodes::ErrorCode>(error_code));
            col_res->insertData(error_name.data(), error_name.size());
        }

        return col_res;
    }
};


REGISTER_FUNCTION(ErrorCodeToName)
{
    FunctionDocumentation::Description description = R"(
Returns the textual name of a numeric ClickHouse error code.
The mapping from numeric error codes to error names is available [here](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp).
)";
    FunctionDocumentation::Syntax syntax = "errorCodeToName(error_code)";
    FunctionDocumentation::Arguments arguments = {
        {"error_code", "ClickHouse error code.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the textual name of `error_code`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT errorCodeToName(252);
        )",
        R"(
┌─errorCodeToName(252)─┐
│ TOO_MANY_PARTS       │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionErrorCodeToName>(documentation);
}

}
