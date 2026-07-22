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
extern const QueryPlanSerializationSettingsJoinAlgorithm join_algorithm;
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

/// getMinRequiredVersion reports the lowest serialization version at which serializing these settings
/// does not change the receiver's behavior. It drives the stateless-worker path, which has no version
/// negotiation. It must be keyed on the values, not on the "changed" flags: a join step assigns every
/// setting it serializes (marking it changed even at the default value), so a flag-based check would
/// raise every join fragment - including a `full_sorting_merge` join, or a hash join with compression
/// off but a non-default `max_memory_usage` - to version 4 and get it rejected by a version-3 worker
/// during a rolling upgrade. Only an actually enabled `enable_join_in_memory_compression` requires
/// version 4; omitting the other version-4 settings reproduces pre-version-4 behavior.
TEST(QueryPlanSerializationSettings, MinRequiredVersion)
{
    /// Nothing set, or only pre-version-4 settings set: the baseline version is enough.
    {
        QueryPlanSerializationSettings settings;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);

        settings[QueryPlanSerializationSetting::max_bytes_in_join] = 777;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }

    /// Enabled in-memory join compression is the one case where a version-1 stream would silently
    /// drop the requested feature, so it requires version 4.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }

    /// The other version-4 settings are only the compression trigger and tuning: with compression
    /// disabled they do not alter execution, so they must not raise the version even when assigned
    /// (a join step assigns them - changed-flagged - on every serialization, e.g. the query-level
    /// `max_memory_usage`). A version-3 worker then simply behaves like a pre-version-4 server.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
        settings[QueryPlanSerializationSetting::join_decompressed_columns_cache_bytes] = 4096;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }

    /// Explicitly assigning the default (disabled) value must not raise the version either.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = false;
        settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }

    /// Even with compression enabled, a step whose `join_algorithm` cannot resolve to a hash-family
    /// implementation (`full_sorting_merge`, `partial_merge`, `direct`) never consumes the setting,
    /// so its fragment must stay on the baseline version and remain readable by a version-3 worker.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        settings[QueryPlanSerializationSetting::join_algorithm]
            = std::vector<JoinAlgorithm>{JoinAlgorithm::FULL_SORTING_MERGE, JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::DIRECT};
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }

    /// A single hash-capable algorithm in the set is enough to require version 4 - including
    /// `prefer_partial_merge`, which falls back to hash when partial merge does not support the
    /// join kind, and `grace_hash`, whose buckets are hash joins.
    for (auto algorithm : {JoinAlgorithm::DEFAULT, JoinAlgorithm::AUTO, JoinAlgorithm::HASH,
                           JoinAlgorithm::PREFER_PARTIAL_MERGE, JoinAlgorithm::PARALLEL_HASH, JoinAlgorithm::GRACE_HASH})
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        settings[QueryPlanSerializationSetting::join_algorithm]
            = std::vector<JoinAlgorithm>{JoinAlgorithm::FULL_SORTING_MERGE, algorithm};
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }
}
