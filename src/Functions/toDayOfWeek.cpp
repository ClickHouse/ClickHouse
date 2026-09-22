#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/IFunctionAdaptors.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace
{

using FunctionToDayOfWeekBase = FunctionCustomWeekToSomething<DataTypeUInt8, ToDayOfWeekImpl>;

class FunctionToDayOfWeek final : public FunctionToDayOfWeekBase
{
public:
    explicit FunctionToDayOfWeek(bool is_monotonic_in_monday_week_)
        : is_monotonic_in_monday_week(is_monotonic_in_monday_week_)
    {
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        /// `ToMondayImpl` partitions the input into Monday-based weeks. Modes 0 and 1 are monotonic
        /// within those weeks, but modes 2 and 3 drop at Sunday, inside the factor's interval.
        /// Claiming monotonicity there would let range analysis prune matching rows.
        if (!is_monotonic_in_monday_week)
            return {};

        return FunctionToDayOfWeekBase::getMonotonicityForRange(type, left, right);
    }

private:
    const bool is_monotonic_in_monday_week;
};

class ToDayOfWeekOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = ToDayOfWeekImpl::name;
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<ToDayOfWeekOverloadResolver>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return FunctionToDayOfWeekBase{}.getReturnTypeImpl(arguments);
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return FunctionToDayOfWeekBase{}.getReturnTypeForDefaultImplementationForDynamic();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        /// The three-argument form supplies a time zone that the default factor analysis does not use.
        /// Its monotonicity remains unknown even for Monday-first modes.
        bool is_monotonic_in_monday_week = arguments.size() == 1;
        if (arguments.size() == 2)
        {
            /// A type-only build does not know the mode. A `NULL` mode makes the result `NULL`.
            /// Default handling of `NULL` or `Nothing` can also bypass argument validation.
            /// These cases supply no Monday-based monotonicity proof.
            const auto & mode_column = arguments[1].column;
            if (mode_column && isColumnConst(*mode_column) && !result_type->onlyNull() && !isNothing(result_type)
                && !mode_column->isNullAt(0))
            {
                /// `check_week_day_mode` uses only the two lowest bits;
                /// the second selects Sunday-first numbering.
                const auto mode = DateLUT::instance().check_week_day_mode(static_cast<UInt8>(mode_column->getUInt(0)));
                is_monotonic_in_monday_week = mode == WeekDayMode::WeekStartsMonday0 || mode == WeekDayMode::WeekStartsMonday1;
            }
        }

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        return std::make_shared<FunctionToFunctionBaseAdaptor>(
            std::make_shared<FunctionToDayOfWeek>(is_monotonic_in_monday_week), std::move(argument_types), result_type);
    }
};

}


REGISTER_FUNCTION(ToDayOfWeek)
{
    FunctionDocumentation::Description description = R"(
Returns the number of the day within the week of a `Date` or `DateTime` value.

The two-argument form of `toDayOfWeek()` enables you to specify whether the week starts on Monday or Sunday,
and whether the return value should be in the range from 0 to 6 or 1 to 7.

| Mode | First day of week | Range                                          |
|------|-------------------|------------------------------------------------|
| 0    | Monday            | 1-7: Monday = 1, Tuesday = 2, ..., Sunday = 7  |
| 1    | Monday            | 0-6: Monday = 0, Tuesday = 1, ..., Sunday = 6  |
| 2    | Sunday            | 0-6: Sunday = 0, Monday = 1, ..., Saturday = 6 |
| 3    | Sunday            | 1-7: Sunday = 1, Monday = 2, ..., Saturday = 7 |
        )";
    FunctionDocumentation::Syntax syntax = "toDayOfWeek(datetime[, mode[, timezone]])";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the day of week from.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"mode", "Optional. Integer specifying the week mode (0-3). Defaults to 0 if omitted.", {"UInt8"}},
        {"timezone", "Optional. Timezone to use for the conversion.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the day of the week for the given `Date` or `DateTime`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
-- The following date is April 21, 2023, which was a Friday:
SELECT
    toDayOfWeek(toDateTime('2023-04-21')),
    toDayOfWeek(toDateTime('2023-04-21'), 1)
            )",
        R"(
┌─toDayOfWeek(toDateTime('2023-04-21'))─┬─toDayOfWeek(toDateTime('2023-04-21'), 1)─┐
│                                     5 │                                        4 │
└───────────────────────────────────────┴──────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<ToDayOfWeekOverloadResolver>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFWEEK", "toDayOfWeek", FunctionFactory::Case::Insensitive);
}

}
