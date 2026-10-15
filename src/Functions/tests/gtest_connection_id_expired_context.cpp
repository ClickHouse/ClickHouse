#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Columns/IColumn.h>

using namespace DB;

/// `connectionId()` used to inherit from `WithContext` and dereference a weak reference to the query
/// context in `executeImpl`. A function built while analyzing a subquery can be executed after the
/// context that built it is gone - e.g. a scalar subquery whose expression actions are reused by the
/// outer query - and the weak reference then threw the logical error `Context has expired`.
///
/// After capturing the connection id at construction time, executing the function must still succeed
/// (and return the captured value) once the building context has been released.
TEST(ConnectionIdFunction, SurvivesExpiredBuildContext)
{
    tryRegisterFunctions();

    ContextMutablePtr query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setClientConnectionId(4242);

    /// Building the function captures the connection id (before the fix it captured a weak context ref).
    ColumnsWithTypeAndName no_arguments;
    auto resolver = FunctionFactory::instance().get("connectionId", query_context);
    auto function = resolver->build(no_arguments);

    /// Release the context that built the function, mimicking a subquery whose context is gone by the
    /// time the outer query's expression actions run.
    query_context.reset();

    ColumnPtr result;
    ASSERT_NO_THROW(result = function->execute(no_arguments, function->getResultType(), 1, /*dry_run=*/ false));
    ASSERT_EQ(result->getUInt(0), 4242u);
}
