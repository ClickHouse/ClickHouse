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

    /// A version-4 receiver gets all of the settings, including the new ones.
    {
        auto v4 = roundTrip(settings, 4);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::max_bytes_in_join].value, 777u);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::max_memory_usage].value, 12345u);
        EXPECT_EQ(v4[QueryPlanSerializationSetting::enable_join_in_memory_compression].value, true);
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
    }
}

/// getMinRequiredVersion reports the lowest serialization version at which serializing these settings
/// does not change the receiver's behavior. It drives the stateless-worker path, which has no version
/// negotiation. It must be keyed on the values, not on the "changed" flags: a join step assigns every
/// setting it serializes (marking it changed even at the default value), so a flag-based check would
/// raise every join fragment - including a `full_sorting_merge` join, or a hash join with compression
/// off but a non-default `max_memory_usage` - to version 4 and get it rejected by a version-3 worker
/// during a rolling upgrade. Only an actually enabled `enable_join_in_memory_compression` or a
/// step-local `max_memory_usage` override requires version 4; omitting a query-wide
/// `max_memory_usage` reproduces pre-version-4 behavior.
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

    /// The other version-4 setting (`max_memory_usage`) must not raise the version even when
    /// assigned (a join step assigns it - changed-flagged - on every serialization, e.g. the
    /// query-level `max_memory_usage`): a receiver that does not find it in the stream restores it
    /// from its query context settings (see JoinStepLogical::deserialize), and a version-3 worker
    /// simply behaves like a pre-version-4 server.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
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

    /// A step that never consults `enable_join_in_memory_compression` (a `ConstantJoin` - a
    /// CROSS/COMMA join or a join with a constant predicate, which keeps a dedicated threshold-based
    /// compression path - and PASTE, which stores no build side; the serializing step flags them via
    /// join_kind_consumes_in_memory_compression, see JoinStepLogical::serializeSettings) must not
    /// raise the version even with compression enabled and a hash-capable `join_algorithm`: such a
    /// join executes as `ConstantJoin` whatever the algorithm setting says, and bumping its fragment
    /// would make a version-3 receiver reject it for a setting it would never consume.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        settings.join_kind_consumes_in_memory_compression = false;
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);

        /// A step-local `max_memory_usage` still requires version 4 for such a step: `ConstantJoin`
        /// consumes `max_memory_usage` as its plain shrink trigger.
        settings.max_memory_usage_is_step_local = true;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);
    }

    /// A step-local `max_memory_usage` (a subquery-local SETTINGS override, flagged by
    /// JoinSettings::updatePlanSettings) is the other case that requires version 4 even with
    /// compression off: the receiver's query context carries only the outer query's value, so an
    /// omitted step-local value could not be restored (see JoinStepLogical::deserialize). The gate is
    /// the implementation that will run: hash-family joins consume the setting, and so does
    /// `ConstantJoin`, which is chosen regardless of `join_algorithm`.
    {
        QueryPlanSerializationSettings settings;
        settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;
        settings.max_memory_usage_is_step_local = true;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);

        settings[QueryPlanSerializationSetting::join_algorithm]
            = std::vector<JoinAlgorithm>{JoinAlgorithm::FULL_SORTING_MERGE, JoinAlgorithm::PARTIAL_MERGE, JoinAlgorithm::DIRECT};
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);

        /// ... but a `ConstantJoin` step (`CROSS JOIN`, `JOIN ON 1`) ignores `join_algorithm`, and it
        /// consumes `max_memory_usage`, so its fragment must still carry a step-local value.
        settings.join_executes_as_constant_join = true;
        EXPECT_EQ(settings.getMinRequiredVersion(), 4u);

        /// Compression, on the other hand, is never consumed by `ConstantJoin`: such a step stays on
        /// the baseline version when the step-local override is absent.
        settings.max_memory_usage_is_step_local = false;
        settings[QueryPlanSerializationSetting::enable_join_in_memory_compression] = true;
        settings.join_kind_consumes_in_memory_compression = false;
        settings[QueryPlanSerializationSetting::join_algorithm] = std::vector<JoinAlgorithm>{JoinAlgorithm::HASH};
        EXPECT_EQ(settings.getMinRequiredVersion(), 1u);
    }
}

/// `max_memory_usage` is consumed by `HashJoin::shrinkStoredBlocksToFit` even with compression off,
/// and a fragment without compression is serialized below version 4, so the stream omits it.
/// JoinStepLogical::deserialize then restores it from the receiver's query context settings, keyed
/// on the changed flag: a value read from the stream must be marked changed (so an explicitly sent
/// value - even one equal to the default - is never overridden), and a value absent from the stream
/// must not be (so the receiver knows to fall back).
TEST(QueryPlanSerializationSettings, MaxMemoryUsageChangedFlagAfterRoundTrip)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_memory_usage] = 12345;

    /// A version-4 stream carries the value; the receiver sees it as changed.
    EXPECT_TRUE(roundTrip(settings, 4).isChanged("max_memory_usage"));

    /// A version-3 stream omits it; the receiver sees it as unchanged and falls back.
    EXPECT_FALSE(roundTrip(settings, 3).isChanged("max_memory_usage"));

    /// A default-constructed instance has nothing changed.
    EXPECT_FALSE(QueryPlanSerializationSettings{}.isChanged("max_memory_usage"));
}
