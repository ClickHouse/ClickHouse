#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesCache.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>

#include <array>

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
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    auto query_context = makeQueryContext("data_types_cache_test_same_query", "UTC");
    auto query_scope = QueryScope::create(query_context);

    auto first = getDataTypesCache().getType("DateTime64(3)");
    auto second = getDataTypesCache().getType("DateTime64(3)");
    ASSERT_EQ(first.get(), second.get());
}

TEST(DataTypesCache, InvalidatedOnQueryContextChange)
{
    ResetCurrentThreadGuard reset_current_thread;
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

TEST(DataTypesCache, PoolsJSONSerializationsWithinOneQuery)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    auto query_context = makeQueryContext("data_types_cache_test_json_same_query", "UTC");
    auto query_scope = QueryScope::create(query_context);

    auto first_type = getDataTypesCache().getType("JSON");
    auto second_type = getDataTypesCache().getType("JSON");
    ASSERT_EQ(first_type.get(), second_type.get());

    auto first_serialization = getDataTypesCache().getSerialization("JSON");
    auto second_serialization = getDataTypesCache().getSerialization("JSON");
    ASSERT_TRUE(first_serialization->supportsPooling());
    ASSERT_EQ(first_serialization.get(), second_serialization.get());
}

TEST(DataTypesCache, InvalidatesTypesAcrossQueriesAndSharesJSONSerializations)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    /// Kept alive (not just its raw address) across both query scopes: if only the address
    /// were recorded, the first cache entry could be destroyed once its query scope ends,
    /// and an unrelated later allocation could reuse the same address, making the comparison
    /// below pass even when invalidation was actually broken.
    SerializationPtr first_serialization;
    DataTypePtr first_type;
    {
        auto query_context = makeQueryContext("data_types_cache_test_json_query_1", "UTC");
        auto query_scope = QueryScope::create(query_context);
        first_type = getDataTypesCache().getType("JSON");
        first_serialization = getDataTypesCache().getSerialization("JSON");
    }

    /// Type entries are query-local even when their immutable serializations can be shared.
    {
        auto query_context = makeQueryContext("data_types_cache_test_json_query_2", "UTC");
        auto query_scope = QueryScope::create(query_context);
        ASSERT_NE(getDataTypesCache().getType("JSON"), first_type);
        ASSERT_EQ(getDataTypesCache().getSerialization("JSON"), first_serialization);
    }
}

TEST(DataTypesCache, InvalidatedOnSessionTimezoneChangeWithinOneContext)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;

    /// clickhouse-client keeps one long-lived client context attached to the client thread
    /// by a single query scope for the whole session, and mutates `session_timezone` on it
    /// in place between queries (see `ClientBase::onTimezoneUpdate`). The context identity
    /// never changes here, so the cache must track the setting value itself.
    auto client_context = makeQueryContext("data_types_cache_test_client_session", "Asia/Tokyo");
    auto query_scope = QueryScope::create(client_context);

    ASSERT_EQ(cachedDateTimeTimezone(), "Asia/Tokyo");

    /// The next query of the same client session changes the setting on the same context.
    client_context->setSetting("session_timezone", String("Europe/Amsterdam"));

    ASSERT_EQ(cachedDateTimeTimezone(), "Europe/Amsterdam");
}

TEST(DataTypesCache, JSONTypedTimezonesRemainDistinct)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;
    SerializationPtr first;
    {
        auto context = makeQueryContext("json_typed_timezone_1", "Asia/Tokyo");
        auto scope = QueryScope::create(context);
        first = getDataTypesCache().getSerialization("JSON(d DateTime, n Array(DateTime64(3)))");
    }
    {
        auto context = makeQueryContext("json_typed_timezone_2", "Europe/Amsterdam");
        auto scope = QueryScope::create(context);
        EXPECT_NE(first, getDataTypesCache().getSerialization("JSON(d DateTime, n Array(DateTime64(3)))"));
    }
}

TEST(DataTypesCache, JSONParsingFollowsSettingsWithinOneClientContext)
{
    ResetCurrentThreadGuard reset_current_thread;
    ThreadStatus thread_status;
    auto context = makeQueryContext("json_client_settings", "UTC");
    auto scope = QueryScope::create(context);
    auto type = DataTypeFactory::instance().get("JSON");
    auto serialization = type->getDefaultSerialization();
    FormatSettings settings;
    settings.try_infer_datetimes = true;
    settings.try_infer_dates = false;
    const std::array<std::pair<const char *, UInt64>, 4> timezones{{
        {"UTC", 1704110400},
        {"Asia/Tokyo", 1704078000},
        {"", DateLUT::serverTimezoneInstance().makeDateTime(2024, 1, 1, 12, 0, 0)},
        {"UTC", 1704110400},
    }};

    for (bool simdjson : {false, true})
    {
        context->setSetting("allow_simdjson", simdjson);
        for (const auto & [session_timezone, expected_timestamp] : timezones)
        {
            context->setSetting("session_timezone", String(session_timezone));
            for (size_t row = 0; row < 2; ++row)
            {
                auto column = type->createColumn();
                ReadBufferFromString input(std::string_view(R"({"d":"2024-01-01 12:00:00"})"));
                serialization->deserializeWholeText(*column, input, settings);
                auto dates = type->getSubcolumn("d.:`DateTime`", column->getPtr());
                EXPECT_EQ((*dates)[0].safeGet<UInt64>(), expected_timestamp);
            }
        }
    }
}
