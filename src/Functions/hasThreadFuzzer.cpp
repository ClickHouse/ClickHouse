#include <Common/ThreadFuzzer.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/** Returns whether Thread Fuzzer is effective.
  * It can be used in tests to prevent too long runs.
  */
class FunctionHasThreadFuzzer : public IFunction
{
public:
    static constexpr auto name = "hasThreadFuzzer";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHasThreadFuzzer>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt8().createColumnConst(input_rows_count, ThreadFuzzer::instance().isEffective());
    }
};

}

REGISTER_FUNCTION(HasThreadFuzzer)
{
    FunctionDocumentation::Description description = R"(
Returns whether the thread fuzzer is enabled.
THis function is only useful for testing and debugging.
    )";
    FunctionDocumentation::Syntax syntax = "hasThreadFuzzer()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns whether Thread Fuzzer is effective.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Check Thread Fuzzer status",
        R"(
SELECT hasThreadFuzzer()
        )",
        R"(
┌─hasThreadFuzzer()─┐
│                 0 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasThreadFuzzer>(documentation);
}

}

