#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Returns the current value of a profile event accumulated by the query (all threads of its thread group).
/// The value is read once per block, so all rows of a block get the same value.
class FunctionGetQueryProfileEvent final : public IFunction
{
public:
    static constexpr auto name = "getQueryProfileEvent";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGetQueryProfileEvent>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return false; }
    [[nodiscard]] bool isDeterministic() const override { return false; }
    [[nodiscard]] bool isDeterministicInScopeOfQuery() const override { return false; }
    /// The value changes while the query runs, so it must be read during execution, not during query analysis.
    [[nodiscard]] bool isSuitableForConstantFolding() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    [[nodiscard]] String getSignatureString() const override { return "(const String) -> UInt64"; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a constant string with the name of a profile event, got {}",
                getName(), arguments[0].type->getName());

        /// Validate the name early, so that an unknown event is reported during query analysis.
        std::ignore = getEvent(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ProfileEvents::Event event = getEvent(arguments);

        auto thread_group = CurrentThread::getGroup();
        if (!thread_group)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function {} can be used only inside a query: the current thread is not attached to a query", getName());

        /// A nested group of the same query (e.g. of a materialized view) only forwards its counters to the parent.
        while (auto parent = thread_group->getParent())
        {
            if (parent->query_context.lock() != thread_group->query_context.lock())
                break;
            thread_group = parent;
        }

        UInt64 value = thread_group->performance_counters[event];
        return ColumnUInt64::create(input_rows_count, value);
    }

private:
    [[nodiscard]] ProfileEvents::Event getEvent(const ColumnsWithTypeAndName & arguments) const
    {
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The argument of function {} should be a constant string with the name of a profile event", getName());

        return ProfileEvents::getByName(column->getDataAt(0));
    }
};

}

REGISTER_FUNCTION(GetQueryProfileEvent)
{
    FunctionDocumentation::Description description = R"(
Returns the current value of a [profile event](/reference/system-tables/events) for the current query.

The value is accumulated over all threads that execute the query, the same way as in the `ProfileEvents` column of `system.query_log`.
The function is non-deterministic: it is evaluated once per [block](/resources/develop-contribute/introduction/architecture#block) of data during query execution,
so all rows of one block get the same value, and the value grows as the query progresses.
This can be used to annotate the output or the inserted data with the progress of the query, such as the number of rows or bytes read so far.

On a distributed query, every server returns the value for its own part of the query.
Some events, such as `RealTimeMicroseconds` or `OSReadBytes`, are updated only periodically, so their value may lag behind.
)";
    FunctionDocumentation::Syntax syntax = "getQueryProfileEvent(event_name)";
    FunctionDocumentation::Arguments arguments = {
        {"event_name", "The name of the profile event, as in `system.events`.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The current value of the profile event for the current query.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Progress of reading",
        R"(
SELECT number, getQueryProfileEvent('SelectedRows') AS selected_rows
FROM numbers_mt(10)
SETTINGS max_block_size = 3, max_threads = 1
        )",
        R"(
┌─number─┬─selected_rows─┐
│      0 │             3 │
│      1 │             3 │
│      2 │             3 │
│      3 │             6 │
│      4 │             6 │
│      5 │             6 │
│      6 │             9 │
│      7 │             9 │
│      8 │             9 │
│      9 │            10 │
└────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetQueryProfileEvent>(documentation);
}

}
