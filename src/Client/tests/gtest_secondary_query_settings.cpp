#include <gtest/gtest.h>

#include <Client/SecondaryQuerySettings.h>
#include <Core/Settings.h>

using namespace DB;

/// The settings an interserver sender (`MultiplexedConnections`, `HedgedConnections`, `RemoteInserter`)
/// serializes into a secondary query. `Connection::sendQuery` sends only the settings marked as changed,
/// and a shard applies what it receives through its own settings constraints, so which settings carry the
/// `changed` flag - not only their values - is part of the contract with the shard.

TEST(SecondaryQuerySettings, CompatibilityDerivedValuesAreNotSerialized)
{
    /// The shard re-derives them from `compatibility`, which is serialized. Sending them explicitly would
    /// subject them to the shard's own constraints - a `CONST` pin drops them, a range pin clamps them -
    /// and the shard would then run under a different codec than the initiator's `compatibility` selects.
    Settings settings;
    settings.set("compatibility", "26.6");
    ASSERT_TRUE(settings.isChanged("network_compression_method"));
    ASSERT_TRUE(settings.isChanged("network_zstd_compression_level"));
    const String derived_method = settings.get("network_compression_method").safeGet<String>();
    const UInt64 derived_level = settings.get("network_zstd_compression_level").safeGet<UInt64>();

    prepareSecondaryQuerySettings(settings);

    EXPECT_TRUE(settings.isChanged("compatibility"));
    EXPECT_FALSE(settings.isChanged("network_compression_method"));
    EXPECT_FALSE(settings.isChanged("network_zstd_compression_level"));

    /// The values stay: they select the codec of the packets this side originates.
    EXPECT_EQ(settings.get("network_compression_method").safeGet<String>(), derived_method);
    EXPECT_EQ(settings.get("network_zstd_compression_level").safeGet<UInt64>(), derived_level);
}

TEST(SecondaryQuerySettings, KeepsAnExplicitOverrideOfACompatibilityDerivedValue)
{
    /// An explicit `SET` wins over `compatibility`, and the shard cannot re-derive it, so it must be sent.
    Settings settings;
    settings.set("compatibility", "26.6");
    settings.set("network_compression_method", "NONE");

    prepareSecondaryQuerySettings(settings);

    EXPECT_TRUE(settings.isChanged("network_compression_method"));
    EXPECT_EQ(settings.get("network_compression_method").safeGet<String>(), "NONE");
    EXPECT_FALSE(settings.isChanged("network_zstd_compression_level"));
}

TEST(SecondaryQuerySettings, ForcesClickHouseSQL)
{
    /// The sender ships a query it has already rewritten into ClickHouse SQL.
    Settings settings;
    settings.set("dialect", "kusto");

    prepareSecondaryQuerySettings(settings);

    EXPECT_TRUE(settings.isChanged("dialect"));
    EXPECT_EQ(settings.get("dialect").safeGet<String>(), "clickhouse");
}

TEST(SecondaryQuerySettings, ForcesClickHouseSQLForAnUntouchedSession)
{
    /// Even when the session never touched `dialect`: the shard would otherwise take the parser from the
    /// effective `dialect` of the authenticated user, which its own profile may default to Kusto or PRQL.
    /// The override has to stay `changed`, or `Connection::sendQuery` would not serialize it at all.
    Settings settings;

    prepareSecondaryQuerySettings(settings);

    EXPECT_TRUE(settings.isChanged("dialect"));
    EXPECT_EQ(settings.get("dialect").safeGet<String>(), "clickhouse");
}
