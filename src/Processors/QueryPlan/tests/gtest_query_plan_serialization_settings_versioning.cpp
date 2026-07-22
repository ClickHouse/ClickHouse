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

/// The in-memory join compression settings were added to the plan serialization in version 4.
/// When serializing for a receiver older than version 4 (a pre-PR server in a mixed-version cluster,
/// including a version-3 server that only knows the parallel-replicas flag and a version-2 server that
/// only knows the bucketed-read encoding), their names must be omitted: BaseSettings::readBinary throws
/// on unknown setting names, so emitting them would break mixed-version distributed queries with
/// serialize_query_plan even at default values.
TEST(QueryPlanSerializationSettings, JoinCompressionSettingsOmittedForOlderVersions)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_bytes_in_join] = 777;
    settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
    settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
    settings[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes] = 4096;

    /// A version-4 receiver gets all of the settings, including the new ones.
    {
        auto v4 = roundTrip(settings, 4);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::max_bytes_in_join].value, 777u);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::max_memory_usage].value, 12345u);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::enable_join_in_memory_compression].value, true);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes].value, 4096u);
    }

    /// A receiver older than version 4 (version 3, version 2 and version 1) does not get the new
    /// settings (they fall back to their defaults), while the pre-existing settings are still sent.
    /// This is exactly the stream a pre-PR server reads.
    for (UInt64 old_version : {1u, 2u, 3u})
    {
        auto old = roundTrip(settings, old_version);
        EXPECT_EQ(old[QueryPlanSerializationSetting::max_bytes_in_join].value, 777u);
        EXPECT_EQ(old[QueryPlanSerializationSetting::max_memory_usage].value, 0u);
        EXPECT_EQ(old[QueryPlanSerializationSetting::enable_join_in_memory_compression].value, false);
        EXPECT_EQ(old[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes].value, 128ull * 1024 * 1024);
    }
}

/// getMinRequiredVersion reports the lowest serialization version that keeps every changed setting.
/// It drives the stateless-worker path, which has no version negotiation: only settings that a step
/// actually changed away from their defaults raise the version above the baseline, so a fragment that
/// touches no version-4 setting stays serializable for an older worker.
TEST(QueryPlanSerializationSettings, MinRequiredVersion)
{
    /// Nothing changed, or only pre-version-4 settings changed: the baseline version is enough.
    {
        QueryPlanSerializationSettings settings;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);

        settings[QueryPlanSerializationSetting::max_bytes_in_join] = 777;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }

    /// Each version-4 setting, when changed from its default, requires version 4.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes] = 4096;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }
}
