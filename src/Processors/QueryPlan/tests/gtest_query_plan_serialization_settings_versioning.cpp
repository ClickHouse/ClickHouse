#include <gtest/gtest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

using namespace DB;

namespace DB::QueryPlanSerializationSetting
{
extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_join;
extern const QueryPlanSerializationSettingsUInt64 max_memory_usage;
extern const QueryPlanSerializationSettingsBool enable_join_in_memory_compression;
extern const QueryPlanSerializationSettingsUInt64 join_decompressed_columns_cache_bytes;
}

namespace
{

QueryPlanSerializationSettings roundTrip(const QueryPlanSerializationSettings & settings, UInt64 version)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out, version);

    QueryPlanSerializationSettings result;
    ReadBufferFromString in(out.str());
    result.readBinary(in);
    return result;
}

}

/// The in-memory join compression settings were added to the plan serialization in version 2.
/// When serializing for a version-1 receiver (a pre-PR server in a mixed-version cluster), their
/// names must be omitted: BaseSettings::readBinary throws on unknown setting names, so emitting them
/// would break mixed-version distributed queries with serialize_query_plan even at default values.
TEST(QueryPlanSerializationSettings, JoinCompressionSettingsOmittedForVersion1)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_bytes_in_join] = 777;
    settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
    settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
    settings[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes] = 4096;

    /// A version-2 receiver gets all of the settings, including the new ones.
    {
        auto v2 = roundTrip(settings, 2);
        EXPECT_EQ(v2[QueryPlanSerializationSetting::max_bytes_in_join].value, 777u);
        EXPECT_EQ(v2[QueryPlanSerializationSetting::max_memory_usage].value, 12345u);
        EXPECT_EQ(v2[QueryPlanSerializationSetting::enable_join_in_memory_compression].value, true);
        EXPECT_EQ(v2[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes].value, 4096u);
    }

    /// A version-1 receiver does not get the new settings (they fall back to their defaults), while
    /// the pre-existing settings are still sent. This is exactly the stream a pre-PR server reads.
    {
        auto v1 = roundTrip(settings, 1);
        EXPECT_EQ(v1[QueryPlanSerializationSetting::max_bytes_in_join].value, 777u);
        EXPECT_EQ(v1[QueryPlanSerializationSetting::max_memory_usage].value, 0u);
        EXPECT_EQ(v1[QueryPlanSerializationSetting::enable_join_in_memory_compression].value, false);
        EXPECT_EQ(v1[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes].value, 128ull * 1024 * 1024);
    }
}
