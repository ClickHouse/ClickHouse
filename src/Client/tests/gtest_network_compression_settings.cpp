#include <gtest/gtest.h>

#include <Client/ClientBaseHelpers.h>
#include <Core/Settings.h>

using namespace DB;

/// The client sends its own helper queries (autocomplete, the `help` command, the AI metadata query)
/// with only the settings selecting the network codec, so that the rest of the session — the `dialect`
/// above all — does not leak into them.

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

    EXPECT_FALSE(result.isChanged("dialect"));
    EXPECT_FALSE(result.isChanged("max_threads"));
    EXPECT_FALSE(result.isChanged("allow_experimental_codecs"));
}

TEST(NetworkCompressionSettings, EmptyForAnUntouchedSession)
{
    const Settings settings;
    EXPECT_TRUE(networkCompressionSettings(settings).changes().empty());
}

TEST(NetworkCompressionSettings, DropsCompatibilityDerivedValues)
{
    /// `compatibility` older than the release that flipped the network defaults derives
    /// `network_compression_method` / `network_zstd_compression_level` back to the old values. The server
    /// derives them on its own, and a profile may pin them as read-only, so they must not be serialized
    /// explicitly into the helper query — the same rule ordinary queries follow via
    /// `ClientBase::settingsWithoutCompatibilityDerived`.
    Settings settings;
    settings.set("compatibility", "26.6");
    ASSERT_TRUE(settings.isChanged("network_compression_method"));
    ASSERT_TRUE(settings.isChanged("network_zstd_compression_level"));

    EXPECT_TRUE(networkCompressionSettings(settings).changes().empty());
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
}
