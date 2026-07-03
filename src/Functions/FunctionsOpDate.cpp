#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
template <typename Op>
class FunctionOpDate : public IFunction
{
public:
    static constexpr auto name = Op::name;

    explicit FunctionOpDate(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionOpDate<Op>>(context); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type) && !isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date, a date with time or a string",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2nd argument of function {}. Should be an interval",
                arguments[1].type->getName(),
                getName());

        auto op = FunctionFactory::instance().get(Op::internal_name, context);
        auto op_build = op->build(arguments);

        return op_build->getResultType();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type) && !isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date or a date with time",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2nd argument of function {}. Should be an interval",
                arguments[1].type->getName(),
                getName());

        auto op = FunctionFactory::instance().get(Op::internal_name, context);
        auto op_build = op->build(arguments);

        auto res_type = op_build->getResultType();
        return op_build->execute(arguments, res_type, input_rows_count, /* dry_run = */ false);
    }

private:
    ContextPtr context;
};

}

struct AddDate
{
    static constexpr auto name = "addDate";
    static constexpr auto internal_name = "plus";
};

struct SubDate
{
    static constexpr auto name = "subDate";
    static constexpr auto internal_name = "minus";
};

using FunctionAddDate = FunctionOpDate<AddDate>;
using FunctionSubDate = FunctionOpDate<SubDate>;

REGISTER_FUNCTION(AddInterval)
{
    FunctionDocumentation::Description description_addDate = R"(
Adds the time interval to the provided date, date with time or string-encoded date or date with time.
If the addition results in a value outside the bounds of the data type, the result is undefined.
    )";
    FunctionDocumentation::Syntax syntax_addDate = R"(
addDate(datetime, interval)
    )";
    FunctionDocumentation::Arguments arguments_addDate = {
        {"datetime", "The date or date with time to which `interval` is added.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"interval", "Interval to add.", {"Interval"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addDate = {"Returns date or date with time obtained by adding `interval` to `datetime`.", {"Date", "Date32", "DateTime", "DateTime64"}} ;
    FunctionDocumentation::Examples examples_addDate = {
        {"Add interval to date", R"(
SELECT addDate(toDate('2018-01-01'), INTERVAL 3 YEAR)
        )",
        R"(
┌─addDate(toDa⋯valYear(3))─┐
│               2021-01-01 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addDate = {23, 9};
    FunctionDocumentation::Category category_addDate = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addDate = {description_addDate, syntax_addDate, arguments_addDate, returned_value_addDate, examples_addDate, introduced_in_addDate, category_addDate
    };
    factory.registerFunction<FunctionAddDate>(documentation_addDate, FunctionFactory::Case::Insensitive);

    FunctionDocumentation::Description description_subDate = R"(
Subtracts the time interval from the provided date, date with time or string-encoded date or date with time.
If the subtraction results in a value outside the bounds of the data type, the result is undefined.
    )";
    FunctionDocumentation::Syntax syntax_subDate = R"(
subDate(datetime, interval)
    )";
    FunctionDocumentation::Arguments arguments_subDate = {
        {"datetime", "The date or date with time from which `interval` is subtracted.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"interval", "Interval to subtract.", {"Interval"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_subDate = {"Returns date or date with time obtained by subtracting `interval` from `datetime`.", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_subDate = {
        {"Subtract interval from date", R"(
SELECT subDate(toDate('2018-01-01'), INTERVAL 3 YEAR)
        )",
        R"(
┌─subDate(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                       2015-01-01 │
└──────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subDate = {23, 9};
    FunctionDocumentation::Category category_subDate = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subDate = {description_subDate, syntax_subDate, arguments_subDate, returned_value_subDate, examples_subDate, introduced_in_subDate, category_subDate};

    factory.registerFunction<FunctionSubDate>(documentation_subDate, FunctionFactory::Case::Insensitive);
}

}
