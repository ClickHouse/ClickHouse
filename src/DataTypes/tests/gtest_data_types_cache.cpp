#include <gtest/gtest.h>

#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesCache.h>
#include <Interpreters/Context.h>

using namespace DB;

namespace
{

ContextMutablePtr makeQueryContext(const String & query_id, const String & session_timezone)
{
    auto query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    query_context->setSetting("session_timezone", session_timezone);
    return query_context;
}

/// `DateTime` without an explicit timezone captures the current query's `session_timezone`
/// at construction (see TimezoneMixin and DateLUT::instance), so the timezone of the returned
/// type shows which query context the cached entry was built under.
String cachedDateTimeTimezone()
{
    auto type = getDataTypesCache().getType("DateTime");
    return assert_cast<const DataTypeDateTime &>(*type).getTimeZone().getTimeZone();
}

}

TEST(DataTypesCache, ReusesEntriesWithinOneQuery)
{
    ThreadStatus thread_status;

    auto query_context = makeQueryContext("data_types_cache_test_same_query", "UTC");
    auto query_scope = QueryScope::create(query_context);

    auto first = getDataTypesCache().getType("DateTime64(3)");
    auto second = getDataTypesCache().getType("DateTime64(3)");
    ASSERT_EQ(first.get(), second.get());
}

TEST(DataTypesCache, InvalidatedOnQueryContextChange)
{
    ThreadStatus thread_status;

    /// A long-lived thread serves one query, caching a context-dependent type...
    {
        auto query_context = makeQueryContext("data_types_cache_test_query_1", "Asia/Tokyo");
        auto query_scope = QueryScope::create(query_context);

        ASSERT_EQ(cachedDateTimeTimezone(), "Asia/Tokyo");
        /// The second lookup is served from the cache and must see the same context.
        ASSERT_EQ(cachedDateTimeTimezone(), "Asia/Tokyo");
    }

    /// ...and is then reused by another query with a different `session_timezone`.
    /// Without query-scoped invalidation, the thread-local cache would return the
    /// `DateTime` type constructed under the previous query's timezone.
    {
        auto query_context = makeQueryContext("data_types_cache_test_query_2", "Europe/Amsterdam");
        auto query_scope = QueryScope::create(query_context);

        ASSERT_EQ(cachedDateTimeTimezone(), "Europe/Amsterdam");
    }
}
