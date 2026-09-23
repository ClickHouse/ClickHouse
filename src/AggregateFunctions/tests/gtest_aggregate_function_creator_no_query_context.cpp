#include <gtest/gtest.h>

#include <thread>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

/// Tests that an aggregate function is constructible on a thread that has no query context.
/// The factory takes the settings it hands to a creator from the current thread's query context
/// and passes none when the thread has no query context, so a creator that reads settings
/// unconditionally faults there.
TEST(AggregateFunctionFactory, CreatorIsConstructibleWithoutAQueryContext)
{
    tryRegisterAggregateFunctions();

    const DataTypes argument_types{std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    const Array parameters;

    bool without_query_context = false;
    AggregateFunctionPtr function;
    String error_message;

    /// A plain thread never gets a `ThreadStatus`, unlike a pool thread, which can inherit the
    /// thread group, and with it the query context, of whoever scheduled the task.
    std::thread thread([&]
    {
        without_query_context = !CurrentThread::isInitialized() || !CurrentThread::get().tryGetQueryContext();
        try
        {
            AggregateFunctionProperties properties;
            function = AggregateFunctionFactory::instance().get(
                "flameGraph", NullsAction::EMPTY, argument_types, parameters, properties);
        }
        catch (const Exception & e)
        {
            error_message = e.displayText();
        }
        catch (...)
        {
            error_message = getCurrentExceptionMessage(true);
        }
    });
    thread.join();

    ASSERT_TRUE(without_query_context);
    ASSERT_EQ(error_message, "");
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->getName(), "flameGraph");
}
