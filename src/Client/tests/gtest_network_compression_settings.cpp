#include <gtest/gtest.h>

#include <Client/ClientBaseHelpers.h>
#include <Client/Connection.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>

using namespace DB;

/// The client sends its own helper queries (autocomplete, the `help` command, the AI metadata query)
/// with only the settings selecting the network codec, so that the rest of the session does not leak
/// into them, plus an explicit `dialect = 'clickhouse'` override: the queries are written in ClickHouse
/// SQL, and dropping the session `dialect` is not enough, because the server would then take the parser
/// from the effective `dialect` of the authenticated user.

TEST(NetworkCompressionSettings, KeepsOnlyTheChangedCompressionSettings)
{
    Settings settings;
    settings.set("network_compression_method", "LZ4");
    settings.set("network_zstd_compression_level", 7);
    settings.set("allow_suspicious_codecs", true);
    settings.set("dialect", "kusto");
    settings.set("max_threads", 3);

    const Settings result = networkCompressionSettings(settings);

    EXPECT_TRUE(result.isChanged("network_compression_method"));
    EXPECT_EQ(result.get("network_compression_method").safeGet<String>(), "LZ4");
    EXPECT_TRUE(result.isChanged("network_zstd_compression_level"));
    EXPECT_EQ(result.get("network_zstd_compression_level").safeGet<UInt64>(), 7u);
    EXPECT_TRUE(result.isChanged("allow_suspicious_codecs"));

    /// The session `dialect` does not leak in; it is overridden with ClickHouse SQL instead.
    EXPECT_TRUE(result.isChanged("dialect"));
    EXPECT_EQ(result.get("dialect").safeGet<String>(), "clickhouse");

    EXPECT_FALSE(result.isChanged("max_threads"));
    EXPECT_FALSE(result.isChanged("allow_experimental_codecs"));
}

TEST(NetworkCompressionSettings, OnlyTheDialectOverrideForAnUntouchedSession)
{
    /// Nothing to carry over from the session, but the `dialect` override is unconditional: the server
    /// picks the parser from the authenticated user's effective `dialect`, which a profile may default
    /// to Kusto or PRQL even though the session never touched it.
    const Settings settings;

    const Settings result = networkCompressionSettings(settings);

    EXPECT_EQ(result.changes().size(), 1u);
    EXPECT_TRUE(result.isChanged("dialect"));
    EXPECT_EQ(result.get("dialect").safeGet<String>(), "clickhouse");
}

TEST(NetworkCompressionSettings, CompatibilityDerivedValuesActButAreNotSerialized)
{
    /// `compatibility` older than the release that flipped the network defaults derives
    /// `network_compression_method` / `network_zstd_compression_level` back to the old values. The derived
    /// values must select the client-side codec of the helper query (`Connection::sendQuery` reads them by
    /// value), but they must not be serialized explicitly — the server re-derives them from `compatibility`
    /// itself, and a profile may pin them as read-only. `compatibility` itself is forwarded so the server
    /// treats the helper query like an ordinary query of this session. The same rule ordinary queries follow
    /// via `ClientBase::settingsWithoutCompatibilityDerived`.
    Settings settings;
    settings.set("compatibility", "26.6");
    ASSERT_TRUE(settings.isChanged("network_compression_method"));
    ASSERT_TRUE(settings.isChanged("network_zstd_compression_level"));

    const Settings result = networkCompressionSettings(settings);

    EXPECT_TRUE(result.isChanged("compatibility"));
    EXPECT_EQ(result.get("compatibility").safeGet<String>(), "26.6");

    EXPECT_FALSE(result.isChanged("network_compression_method"));
    EXPECT_FALSE(result.isChanged("network_zstd_compression_level"));
    EXPECT_EQ(result.get("network_compression_method").safeGet<String>(),
              settings.get("network_compression_method").safeGet<String>());
    EXPECT_EQ(result.get("network_zstd_compression_level").safeGet<UInt64>(),
              settings.get("network_zstd_compression_level").safeGet<UInt64>());

    /// Only `compatibility` and the unconditional `dialect` override go over the wire.
    EXPECT_TRUE(result.isChanged("dialect"));
    EXPECT_EQ(result.get("dialect").safeGet<String>(), "clickhouse");
    EXPECT_EQ(result.changes().size(), 2u);
}

TEST(NetworkCompressionSettings, KeepsAnExplicitOverrideOfACompatibilityDerivedValue)
{
    /// An explicit `SET` wins over `compatibility`, and the server cannot re-derive it, so it must be sent.
    Settings settings;
    settings.set("compatibility", "26.6");
    settings.set("network_compression_method", "NONE");

    const Settings result = networkCompressionSettings(settings);

    EXPECT_TRUE(result.isChanged("network_compression_method"));
    EXPECT_EQ(result.get("network_compression_method").safeGet<String>(), "NONE");
    EXPECT_FALSE(result.isChanged("network_zstd_compression_level"));
    EXPECT_TRUE(result.isChanged("compatibility"));
    EXPECT_TRUE(result.isChanged("dialect"));
}

/// The other half of the contract: `Connection::sendQuery` picks the codec for the compressed packets
/// the client originates (`INSERT` data, external tables) from the setting *values*, regardless of the
/// `changed` flags — so the values that `compatibility` derived apply to the wire even though they are
/// not serialized to the server.

TEST(ChooseNetworkCompressionCodec, CompatibilityRollsBackTheClientSideCodec)
{
    Settings settings;
    settings.set("compatibility", "26.6");
    /// What ordinary queries pass to `Connection::sendQuery` (`ClientBase::settingsWithoutCompatibilityDerived`).
    Settings for_ordinary_query = settings;
    for_ordinary_query.markSettingsChangedByCompatibilityAsUnchanged();
    /// What helper queries (autocomplete, `help`, the AI metadata query) pass.
    const Settings for_helper_query = networkCompressionSettings(settings);

    const auto old_default = CompressionCodecFactory::instance().get("LZ4", {});
    EXPECT_EQ(chooseNetworkCompressionCodec(&for_ordinary_query)->getMethodByte(), old_default->getMethodByte());
    EXPECT_EQ(chooseNetworkCompressionCodec(&for_helper_query)->getMethodByte(), old_default->getMethodByte());
}

TEST(ChooseNetworkCompressionCodec, DefaultIsZSTD)
{
    const Settings settings;
    const auto zstd = CompressionCodecFactory::instance().get("ZSTD", 3);
    EXPECT_EQ(chooseNetworkCompressionCodec(&settings)->getMethodByte(), zstd->getMethodByte());
    EXPECT_EQ(chooseNetworkCompressionCodec(nullptr)->getMethodByte(), zstd->getMethodByte());
}
