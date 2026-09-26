#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/DateLUTImpl.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesCache.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
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

/// The gtest binary's main thread already has a `MainThreadStatus` attached, and
/// `ThreadStatus`'s constructor asserts no `ThreadStatus` is already current on this
/// thread. Reset `current_thread` before constructing a local `ThreadStatus` and restore
/// it afterward, matching the `FileCacheTest` fixture pattern in gtest_filecache.cpp.
/// Declare this before the `ThreadStatus` it guards so construction/destruction order
/// (reverse of declaration) resets first and restores last.
struct ResetCurrentThreadGuard
{
    ResetCurrentThreadGuard() { current_thread = nullptr; }
    ~ResetCurrentThreadGuard() { current_thread = MainThreadStatus::get(); }
};

/// Renders the epoch through the cached serialization. The zone is not observable in the type's
/// name, so what the serialization writes is the only statement of which zone it ended up with.
String renderEpoch(const String & type_name)
{
    auto column = ColumnUInt32::create();
    column->insertValue(0);

    WriteBufferFromOwnString out;
    getDataTypesCache().getSerialization(type_name)->serializeText(*column, 0, out, FormatSettings{});
    return out.str();
}

bool poolsSerialization(const String & type_name)
{
    return getDataTypesCache().getSerialization(type_name).get() == getDataTypesCache().getSerialization(type_name).get();
}

}

TEST(DataTypesCache, ReusesEntriesWithinOneQuery)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    auto query_context = makeQueryContext("data_types_cache_test_same_query", "UTC");
    auto query_scope = QueryScope::create(query_context);

    auto first = getDataTypesCache().getType("DateTime64(3)");
    auto second = getDataTypesCache().getType("DateTime64(3)");
    ASSERT_EQ(first.get(), second.get());
}

TEST(DataTypesCache, SerializationFollowsSessionTimezoneAcrossQueries)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    /// A long-lived thread serves one query, which populates the cache...
    {
        auto query_context = makeQueryContext("data_types_cache_test_query_1", "Asia/Tokyo");
        auto query_scope = QueryScope::create(query_context);

        ASSERT_EQ(renderEpoch("DateTime"), "1970-01-01 09:00:00");
        ASSERT_EQ(renderEpoch("DateTime64(3)"), "1970-01-01 09:00:00.000");
    }

    /// ...and is then reused by another query with a different `session_timezone`. The entry the
    /// first query left behind must not decide how the second one renders.
    {
        auto query_context = makeQueryContext("data_types_cache_test_query_2", "UTC");
        auto query_scope = QueryScope::create(query_context);

        ASSERT_EQ(renderEpoch("DateTime"), "1970-01-01 00:00:00");
        ASSERT_EQ(renderEpoch("DateTime64(3)"), "1970-01-01 00:00:00.000");
    }
}

TEST(DataTypesCache, SerializationFollowsSessionTimezoneWithinOneContext)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    /// clickhouse-client keeps one long-lived client context attached to the client thread by a
    /// single query scope for the whole session, and mutates `session_timezone` on it in place
    /// between queries (see `ClientBase::onTimezoneUpdate`), so the context identity never changes.
    auto client_context = makeQueryContext("data_types_cache_test_client_session", "Asia/Tokyo");
    auto query_scope = QueryScope::create(client_context);

    ASSERT_EQ(renderEpoch("DateTime"), "1970-01-01 09:00:00");

    client_context->setSetting("session_timezone", String("UTC"));

    ASSERT_EQ(renderEpoch("DateTime"), "1970-01-01 00:00:00");
}

TEST(DataTypesCache, DoesNotPoolSerializationsThatCapturedTheQueryContext)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    auto query_context = makeQueryContext("data_types_cache_test_not_pooled", "Asia/Tokyo");
    auto query_scope = QueryScope::create(query_context);

    /// A declared zone is part of the type, so such a serialization is context-free and pooled.
    ASSERT_TRUE(poolsSerialization("DateTime('Asia/Tokyo')"));
    ASSERT_TRUE(poolsSerialization("Array(DateTime('Asia/Tokyo'))"));

    /// Without one, the zone comes from the session and the serialization is rebuilt per request,
    /// including when it is reached through a container or a `JSON` typed path.
    ASSERT_FALSE(poolsSerialization("DateTime"));
    ASSERT_FALSE(poolsSerialization("DateTime64(3)"));
    ASSERT_FALSE(poolsSerialization("Nullable(DateTime)"));
    ASSERT_FALSE(poolsSerialization("Array(DateTime)"));
    ASSERT_FALSE(poolsSerialization("Tuple(DateTime, String)"));
    ASSERT_FALSE(poolsSerialization("Map(String, Array(DateTime))"));
    ASSERT_FALSE(poolsSerialization("JSON(ts DateTime)"));

    /// The type is still pooled: the zone it captured is read by nobody.
    auto first_type = getDataTypesCache().getType("DateTime");
    auto second_type = getDataTypesCache().getType("DateTime");
    ASSERT_EQ(first_type.get(), second_type.get());
}

TEST(DataTypesCache, PoolsNonPoolableSerializations)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    /// SerializationJSON reports supportsPooling() == false, but neither its mutable extraction-tree
    /// state nor its parser choice is reached by the ways this cache is used, so it is pooled here
    /// even across queries - which is what keeps a `JSON` column in a text format from rebuilding
    /// it per value.
    SerializationPtr first_serialization;
    {
        auto query_context = makeQueryContext("data_types_cache_test_non_poolable_query_1", "UTC");
        auto query_scope = QueryScope::create(query_context);

        first_serialization = getDataTypesCache().getSerialization("JSON");
        ASSERT_FALSE(first_serialization->supportsPooling());
        ASSERT_EQ(getDataTypesCache().getSerialization("JSON").get(), first_serialization.get());
    }

    {
        auto query_context = makeQueryContext("data_types_cache_test_non_poolable_query_2", "UTC");
        auto query_scope = QueryScope::create(query_context);

        ASSERT_EQ(getDataTypesCache().getSerialization("JSON").get(), first_serialization.get());
    }
}
