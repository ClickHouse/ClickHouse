#include <ctime>
#include <Core/Field.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/ErrnoException.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CLOCK_GETTIME;
}

namespace
{

/// Compute the current time of day (seconds since midnight) in the server/session time zone.
/// The computation matches the `DateTime` -> `Time` conversion (`CAST(now() AS Time)`).
Int32 currentTimeOfDay()
{
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    const auto & date_lut = DateLUT::instance();
    Int64 utc_seconds = spec.tv_sec;
    Int64 offset = date_lut.timezoneOffset(utc_seconds);
    Int64 local_seconds = (utc_seconds + offset) % 86400;
    if (local_seconds < 0)
        local_seconds += 86400;

    return static_cast<Int32>(local_seconds);
}

/// Get the current time of day. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionLocalTime final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionLocalTime(Int32 time_) : time_value(time_) {}

    String getName() const override { return "localtime"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeTime().createColumnConst(input_rows_count, static_cast<Int64>(time_value));
    }

private:
    Int32 time_value;
};

class FunctionBaseLocalTime final : public IFunctionBase
{
public:
    explicit FunctionBaseLocalTime(Int32 time_) : time_value(time_), return_type(std::make_shared<DataTypeTime>()) {}

    String getName() const override { return "localtime"; }

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
        return std::make_unique<ExecutableFunctionLocalTime>(time_value);
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    Int32 time_value;
    DataTypePtr return_type;
};

class LocalTimeOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "localtime";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool allowsOmittingParentheses() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<LocalTimeOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeTime>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionBaseLocalTime>(currentTimeOfDay());
    }
};

}

REGISTER_FUNCTION(LocalTime)
{
    FunctionDocumentation::Description description = R"(
Returns the current time of day in the server (or session) time zone, at the moment of query analysis.
The function is a constant expression. It is the SQL-standard / PostgreSQL `LOCALTIME`, and is equivalent to `CAST(now() AS Time)`.
    )";
    FunctionDocumentation::Syntax syntax = "localtime()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current time of day.", {"Time"}};
    FunctionDocumentation::Examples examples = {
        {"SQL standard syntax without parentheses", R"(
SELECT LOCALTIME
        )",
        R"(
┌───────LOCALTIME─┐
│        07:42:09 │
└─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<LocalTimeOverloadResolver>(documentation, FunctionFactory::Case::Insensitive);
}

}
