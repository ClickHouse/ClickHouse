#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>
#include <thread>

using namespace DB;

template <typename Ptr>
void run(Ptr context)
{
    for (size_t i = 0; i < 100; ++i)
    {
        std::thread t1([context]
        {
            if constexpr (std::is_same_v<ContextWeakPtr, Ptr>)
                context.lock()->getAsyncReadCounters();
            else
                context->getAsyncReadCounters();
        });

        std::thread t2([context]
        {
            Context::createCopy(context);
        });

        t1.join();
        t2.join();
    }
}

TEST(Context, MutableRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextMutablePtr>(context);
}

TEST(Context, ConstRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextPtr>(context);
}

TEST(Context, WeakRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextWeakPtr>(context);
}
/// Regression test for a cross-query leak in `system.query_log.used_privileges`.
///
/// A query context is created via `createCopy(session_or_global_context)` followed by
/// `makeQueryContext()`. Session contexts are themselves `createCopy(global_context)` and never
/// call `makeQueryContext`, so the session and global contexts share a single `QueryPrivilegesInfo`
/// object. `makeQueryContext` used to seed the new query's privileges by *copying the contents* of
/// that shared parent object (`std::make_shared<QueryPrivilegesInfo>(*query_privileges_info)`).
///
/// As a result, any privilege string that ever landed in the shared parent object (and, under
/// concurrency, partial state observed mid-write) leaked into the `used_privileges` of unrelated
/// later queries from other sessions and databases. The fix seeds every query with an empty
/// `QueryPrivilegesInfo`. This test models the scenario deterministically: it pollutes a parent
/// context's privileges, derives a query context from it, and asserts the query starts clean.
///
/// See issue ClickHouse/ClickHouse#105983.
TEST(Context, MakeQueryContextDoesNotInheritPrivileges)
{
    /// Stand in for the shared session/global context whose `QueryPrivilegesInfo` gets polluted.
    auto parent_context = Context::createCopy(getContext().context);
    parent_context->addQueryPrivilegesInfo("SELECT(naughty_column) ON some_db.some_table", true);
    parent_context->addQueryPrivilegesInfo("INSERT ON some_db.some_table", false);

    {
        const auto & parent_info = parent_context->getQueryPrivilegesInfo();
        std::lock_guard lock(parent_info.mutex);
        ASSERT_FALSE(parent_info.used_privileges.empty());
        ASSERT_FALSE(parent_info.missing_privileges.empty());
    }

    /// A new query context, as created by `Session::makeQueryContextImpl`.
    auto query_context = Context::createCopy(parent_context);
    query_context->makeQueryContext();

    /// The fresh query must not have inherited any of the parent's privilege strings.
    {
        const auto & query_info = query_context->getQueryPrivilegesInfo();
        std::lock_guard lock(query_info.mutex);
        EXPECT_TRUE(query_info.used_privileges.empty());
        EXPECT_TRUE(query_info.missing_privileges.empty());
    }

    /// Privileges checked during the new query must be tracked independently of the parent,
    /// and must not bleed back into the parent's (shared) object.
    query_context->addQueryPrivilegesInfo("SELECT ON my_db.my_table", true);
    {
        const auto & query_info = query_context->getQueryPrivilegesInfo();
        std::lock_guard lock(query_info.mutex);
        EXPECT_EQ(query_info.used_privileges.count("SELECT ON my_db.my_table"), 1u);
    }
    {
        const auto & parent_info = parent_context->getQueryPrivilegesInfo();
        std::lock_guard lock(parent_info.mutex);
        EXPECT_EQ(parent_info.used_privileges.count("SELECT ON my_db.my_table"), 0u);
    }
}
