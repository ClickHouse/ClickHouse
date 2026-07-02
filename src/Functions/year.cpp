#include <ctime>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>


namespace DB
{
namespace
{

/// The current year, evaluated once for the entire query (a constant expression).
class ExecutableFunctionYear final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionYear(UInt16 year_) : year_value(year_) {}

    String getName() const override { return "year"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt16().createColumnConst(input_rows_count, year_value);
    }

private:
    UInt16 year_value;
};

class FunctionBaseYear final : public IFunctionBase
{
public:
    explicit FunctionBaseYear(UInt16 year_) : year_value(year_), return_type(std::make_shared<DataTypeUInt16>()) {}

    String getName() const override { return "year"; }

    const DataTypes & getArgumentTypes() const override
    {
        static const DataTypes argument_types;
        return argument_types;
    }

    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionYear>(year_value);
    }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    UInt16 year_value;
    DataTypePtr return_type;
};

/// `year([datetime[, timezone]])`.
///   * With arguments it is identical to `toYear`: the resolver forwards to the `toYear`
///     overload resolver, so `year(<date>)` keeps `toYear`'s result type, monotonicity,
///     preimage and index analysis.
///   * Without arguments it returns the current year, analogous to `now()`/`today()`.
///
/// Determinism is argument-count dependent: only the niladic `year()` form is non-deterministic
/// (it reads the wall clock, like `now()`/`today()`), while `year(<date>)` delegates to `toYear`
/// and is fully deterministic. The pre-build checks that inspect the unresolved AST by function
/// name (query result cache, mutation determinism checks) query `isDeterministic(number_of_arguments)`
/// so that `year(<date>)` remains cacheable and usable in Replicated mutations, exactly like
/// `toYear(<date>)`. The argument-less `isDeterministic()` reports non-deterministic (used by the
/// arg-blind `system.functions.is_deterministic` column), which is the conservative choice.
class FunctionYearOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "year";

    explicit FunctionYearOverloadResolver(ContextPtr context)
        : to_year(FunctionFactory::instance().get("toYear", context))
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionYearOverloadResolver>(context);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministic(size_t number_of_arguments) const override { return number_of_arguments != 0; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Forward everything about the argument form to `toYear`.
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return to_year->getArgumentsThatAreAlwaysConstant(); }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            return std::make_shared<FunctionBaseYear>(static_cast<UInt16>(DateLUT::instance().toYear(time(nullptr))));

        return to_year->build(arguments);
    }

private:
    FunctionOverloadResolverPtr to_year;
};

}

REGISTER_FUNCTION(Year)
{
    FunctionDocumentation::Description description = R"(
Returns the year component (AD) of a `Date` or `DateTime` value.

When called without arguments, `year()` returns the current year at the moment of query
analysis (equivalent to `toYear(today())`), mirroring `now()` and `today()`.
    )";
    FunctionDocumentation::Syntax syntax = "year([datetime[, timezone]])";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Optional. Date or date with time to get the year from. When omitted, the current year is returned.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone name for the returned value. Only applicable together with `datetime`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the year of the given (or current) Date or DateTime", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
    {
        "Year of a value",
        R"(
SELECT year(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─year(toDateTime('2023-04-21 10:20:30'))─┐
│                                    2023 │
└─────────────────────────────────────────┘
        )"
    },
    {
        "Year in a specific timezone",
        R"(
SELECT year(toDateTime('2023-12-31 23:00:00', 'UTC'), 'Asia/Yekaterinburg')
        )",
        R"(
┌─year(toDateTime('2023-12-31 23:00:00', 'UTC'), 'Asia/Yekaterinburg')─┐
│                                                                 2024 │
└──────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Current year",
        R"(
SELECT year()
        )",
        R"(
┌─year()─┐
│   2023 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    /// Registered case-insensitively so the MySQL-style spelling `YEAR` (and `Year`, ...)
    /// resolves here without a separate alias.
    factory.registerFunction<FunctionYearOverloadResolver>(documentation, FunctionFactory::Case::Insensitive);
}

}
