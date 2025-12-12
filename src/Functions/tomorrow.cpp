#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>


namespace DB
{

class ExecutableFunctionTomorrow : public IExecutableFunction
{
public:
    explicit ExecutableFunctionTomorrow(time_t time_) : day_value(time_) {}

    String getName() const override { return "tomorrow"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDate().createColumnConst(input_rows_count, day_value);
    }

private:
    DayNum day_value;
};

class FunctionBaseTomorrow : public IFunctionBase
{
public:
    explicit FunctionBaseTomorrow(DayNum day_value_) : day_value(day_value_), return_type(std::make_shared<DataTypeDate>()) {}

    String getName() const override { return "tomorrow"; }

    const DataTypes & getArgumentTypes() const override
    {
        static const DataTypes argument_types;
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionTomorrow>(day_value);
    }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    DayNum day_value;
    DataTypePtr return_type;
};

class TomorrowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "tomorrow";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<TomorrowOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeDate>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        auto day_num = DateLUT::instance().toDayNum(time(nullptr)) + 1;
        return std::make_unique<FunctionBaseTomorrow>(static_cast<DayNum>(day_num));
    }
};

REGISTER_FUNCTION(Tomorrow)
{
    FunctionDocumentation::Description description = R"(
Accepts zero arguments and returns tomorrow's date at one of the moments of query analysis.
    )";
    FunctionDocumentation::Syntax syntax = R"(
tomorrow()
    )";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns tomorrow's date.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Get tomorrow's date", R"(
SELECT tomorrow();
SELECT today() + 1;
        )",
        R"(
┌─tomorrow()──┐
│  2025-06-11 │
└─────────────┘
┌─plus(today(), 1)─┐
│       2025-06-11 │
└──────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<TomorrowOverloadResolver>(documentation);
}

}
