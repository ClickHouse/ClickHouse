#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <base/sleep.h>
#include <Common/FailPoint.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

#include <unistd.h>

namespace ProfileEvents
{
extern const Event SleepFunctionCalls;
extern const Event SleepFunctionMicroseconds;
extern const Event SleepFunctionElapsedMicroseconds;
}

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 function_sleep_max_microseconds_per_block;
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_SLOW;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
extern const char infinite_sleep[];
}

namespace
{

/** sleep(seconds) - the specified number of seconds sleeps each columns.
  */

enum class FunctionSleepVariant : uint8_t
{
    PerBlock,
    PerRow
};

template <FunctionSleepVariant variant>
class FunctionSleep : public IFunction
{
private:
    UInt64 max_microseconds;
    QueryStatusPtr query_status;

public:
    static constexpr auto name = variant == FunctionSleepVariant::PerBlock ? "sleep" : "sleepEachRow";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSleep<variant>>(
            context->getSettingsRef()[Setting::function_sleep_max_microseconds_per_block], context->getProcessListElementSafe());
    }

    FunctionSleep(UInt64 max_microseconds_, QueryStatusPtr query_status_)
        : max_microseconds(std::min(max_microseconds_, static_cast<UInt64>(std::numeric_limits<UInt32>::max())))
        , query_status(query_status_)
    {
    }

    String getName() const override { return name; }
    bool isSuitableForConstantFolding() const override { return false; } /// Do not sleep during query analysis.
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);

        if (!which.isFloat() && !which.isNativeUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected UInt* or Float*",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeUInt8>();
    }
    ColumnPtr
    executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        return execute(arguments, result_type, true);
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        return execute(arguments, result_type, false);
    }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, bool dry_run) const
    {
        const IColumn * col = arguments[0].column.get();

        if (!isColumnConst(*col))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant.", getName());

        Float64 seconds = applyVisitor(FieldVisitorConvertToNumber<Float64>(), assert_cast<const ColumnConst &>(*col).getField());

        if (seconds < 0 || !std::isfinite(seconds) || seconds > static_cast<Float64>(std::numeric_limits<UInt32>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot sleep infinite, very large or negative amount of time (not implemented)");

        size_t size = col->size();

        /// We do not sleep if the columns is empty.
        if (size > 0)
        {
            /// When sleeping, the query cannot be cancelled. For ability to cancel query, we limit sleep time.
            UInt64 microseconds = static_cast<UInt64>(seconds * 1e6);
            FailPointInjection::pauseFailPoint(FailPoints::infinite_sleep);

            if (max_microseconds && microseconds > max_microseconds)
                throw Exception(
                    ErrorCodes::TOO_SLOW,
                    "The maximum sleep time is {} microseconds. Requested: {} microseconds",
                    max_microseconds,
                    microseconds);

            if (!dry_run)
            {
                UInt64 count = (variant == FunctionSleepVariant::PerBlock ? 1 : size);
                microseconds *= count;

                if (max_microseconds && microseconds > max_microseconds)
                    throw Exception(
                        ErrorCodes::TOO_SLOW,
                        "The maximum sleep time is {} microseconds. Requested: {} microseconds per block (of size {})",
                        max_microseconds,
                        microseconds,
                        size);

                UInt64 elapsed = 0;
                while (elapsed < microseconds)
                {
                    UInt64 sleep_time = microseconds - elapsed;
                    if (query_status)
                        sleep_time = std::min(sleep_time, /* 1 second */ static_cast<UInt64>(1000000));

                    sleepForMicroseconds(sleep_time);
                    elapsed += sleep_time;

                    if (query_status && !query_status->checkTimeLimit())
                        break;
                }

                ProfileEvents::increment(ProfileEvents::SleepFunctionCalls, count);
                ProfileEvents::increment(ProfileEvents::SleepFunctionMicroseconds, microseconds);
                ProfileEvents::increment(ProfileEvents::SleepFunctionElapsedMicroseconds, elapsed);
            }
        }

        /// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each columns.
        return result_type->createColumnConst(size, 0u)->convertToFullColumnIfConst();
    }
};

}

REGISTER_FUNCTION(Sleep)
{
    FunctionDocumentation::Description description = R"(
Sleeps for the specified number of seconds. The function can operate in two modes:
- `sleep` sleeps for the specified duration once per data block
- `sleepEachRow` sleeps for the specified duration for each row in the data block

This function is primarily intended for development, debugging, and demonstration purposes.
For security reasons, the function can only be executed in the default user profile (with `allow_sleep` enabled).
)";
    FunctionDocumentation::Syntax syntax = "sleep(seconds)\nsleepEachRow(seconds)";
    FunctionDocumentation::Arguments arguments = {
        {"seconds", "The number of seconds to sleep. Must be a constant value.", {"Float", "UInt"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 0.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT sleep(2);
            )",
            R"(
SELECT sleep(2)

Query id: 8aa9943e-a686-45e1-8317-6e8e3a5596ac

0 rows in set. Elapsed: 2.001 sec.
            )"
        },
        {
            "sleepEachRow usage",
            R"(
SELECT number, sleepEachRow(0.5) FROM system.numbers LIMIT 5;
            )",
            R"(
┌─number─┬─sleepEachRow(0.5)─┐
│      0 │                 0 │
│      1 │                 0 │
│      2 │                 0 │
│      3 │                 0 │
│      4 │                 0 │
└────────┴───────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>(documentation);
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
}

}
