#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>
#include <Poco/Util/XMLConfiguration.h>
#include <sstream>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

/// Per-TU extern declarations for the `ContentAddressedSetting` entries this file uses -- the
/// established pattern for `BaseSettings`-derived classes in this codebase (see e.g.
/// `RegisterDiskCache.cpp`'s `namespace FileCacheSetting` block): the entries are DEFINED once in
/// `ContentAddressedSettings.cpp`, and each consumer TU declares only the ones it references.
namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsBool gc_enabled;
    extern const ContentAddressedSettingsUInt64 gc_shards;
    extern const ContentAddressedSettingsUInt64 gc_interval_sec;
    extern const ContentAddressedSettingsString scratch_path;
}

namespace
{
Poco::AutoPtr<Poco::Util::XMLConfiguration> makeConfig(const std::string & inner)
{
    std::istringstream iss("<clickhouse><disk>" + inner + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(iss);
}

const auto identity_macros = [](const std::string & s) { return s; };

class ScopedCasSettingsLogCapture
{
public:
    ScopedCasSettingsLogCapture()
        : logger(getLogger("ContentAddressedSettings"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel())
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("warning");
    }

    ~ScopedCasSettingsLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const
    {
        return stream.str();
    }

private:
    LoggerPtr logger;
    std::ostringstream stream;
    Poco::AutoPtr<Poco::StreamChannel> channel;
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

size_t countOccurrences(const String & haystack, const String & needle)
{
    size_t n = 0;
    for (size_t at = haystack.find(needle); at != String::npos; at = haystack.find(needle, at + 1))
        ++n;
    return n;
}

void expectLoadFailureWithExactMessage(const String & config, int code, const String & message)
{
    auto cfg = makeConfig(config);
    ContentAddressedSettings settings;
    try
    {
        settings.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected settings load to fail";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code);
        EXPECT_EQ(e.message(), message);
    }
}
}

TEST(CASContentAddressedSettings, DefaultsAndOverridesLand)
{
    auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shards>4</cas_gc_shards>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/data", "/data/default_scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::gc_shards].value, 4u);
    EXPECT_EQ(s[ContentAddressedSetting::gc_interval_sec].value, 60u);          /// table default
    /// Absent key -> the verbatim default (never touches the anchor).
    EXPECT_EQ(s[ContentAddressedSetting::scratch_path].value, "/data/default_scratch");
}

TEST(CASContentAddressedSettings, RemovedCacheSettingsAreRejected)
{
    for (const std::string & suffix : {"cache_bytes", "head_first_min_bytes"})
    {
        const std::string setting = "cas_deduplication_" + suffix;
        SCOPED_TRACE(setting);
        auto cfg = makeConfig(
            "<cas_server_root_id>srv1</cas_server_root_id><" + setting + ">4096</" + setting + ">");
        ContentAddressedSettings settings;
        try
        {
            settings.loadFromConfig(*cfg, "disk", "/data", "/data/scratch", identity_macros);
            FAIL() << "expected removed setting " << setting << " to be rejected as unknown";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_SETTING);
        }
    }
}

TEST(CASContentAddressedSettings, UnknownKeyRejected)
{
    expectLoadFailureWithExactMessage(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shardz>4</cas_gc_shardz>",
        ErrorCodes::UNKNOWN_SETTING,
        "Unknown setting 'cas_gc_shardz'");
}

TEST(CASContentAddressedSettings, MissingRequiredSettingNamesExternalConfigKey)
{
    expectLoadFailureWithExactMessage(
        "<cas_gc_shards>1</cas_gc_shards>",
        ErrorCodes::NO_ELEMENTS_IN_CONFIG,
        "Expected `cas_server_root_id` in config for a content-addressed disk");
}

TEST(CASContentAddressedSettings, InvalidBoundsDiagnosticNamesExternalConfigKeys)
{
    expectLoadFailureWithExactMessage(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shards>0</cas_gc_shards>",
        ErrorCodes::BAD_ARGUMENTS,
        "content_addressed disk: cas_gc_interval_sec and cas_gc_shards must be >= 1 (got 60, 0)");
}

TEST(CASContentAddressedSettings, InvalidEnumDiagnosticsNameExternalConfigKeys)
{
    expectLoadFailureWithExactMessage(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_blob_hash>md5</cas_blob_hash>",
        ErrorCodes::BAD_ARGUMENTS,
        "parseBlobHashAlgo: unknown cas_blob_hash config value 'md5' (expected one of cityhash128|xxh3-128|sha256)");
    expectLoadFailureWithExactMessage(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_staging_backend>remote</cas_staging_backend>",
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown cas_staging_backend value 'remote' (expected 'local' or 's3')");
    expectLoadFailureWithExactMessage(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_part_folder_validate>sometimes</cas_part_folder_validate>",
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown cas_part_folder_validate value 'sometimes' (expected 'always', 'never', or 'age <non-negative integer seconds>')");
}

/// The point of this test is that none of these names appears anywhere in CAS code. It is not an
/// enumeration to be extended when a backend adds a setting; it samples the classes that a
/// name-based skip-list provably cannot cover.
TEST(CASContentAddressedSettings, ForeignKeysAreNeverInspected)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<type>object_storage</type><object_storage_type>s3</object_storage_type>"
        "<metadata_type>cas</metadata_type><endpoint>http://x/y</endpoint>"
        "<path>cas_pool/</path><name>cas_test_disk</name><use_fake_transaction>1</use_fake_transaction>"
        "<http_keep_alive_timeout>60</http_keep_alive_timeout>"
        "<http_keep_alive_max_requests>100</http_keep_alive_max_requests>"
        "<connect_timeout_ms>1000</connect_timeout_ms><session_token>t</session_token>"
        "<s3_retry_attempts>7</s3_retry_attempts><s3_max_put_rps>100</s3_max_put_rps>"
        "<header>X-A: 1</header><header>X-B: 2</header>"
        "<access_header>X-C: 3</access_header><user_alice>alice</user_alice>"
        "<proxy><uri>http://proxy:8080</uri></proxy>"
        "<server_side_encryption_kms_config><key_id>k</key_id></server_side_encryption_kms_config>"
        "<account_name>acct</account_name><container_name>c</container_name>"
        "<connection_string>DefaultEndpointsProtocol=http;</connection_string>");
    ContentAddressedSettings s;
    EXPECT_NO_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros));
}

TEST(CASContentAddressedSettings, LegacySpellingStillLoadsDuringMigrationWindow)
{
    auto cfg = makeConfig("<server_root_id>srv1</server_root_id><gc_shards>4</gc_shards>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::gc_shards].value, 4u);
}

TEST(CASContentAddressedSettings, PartialMigrationLoadsAndReportsEveryLegacyKey)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<gc_shards>4</gc_shards><gc_interval_sec>7</gc_interval_sec>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        captured = capture.captured();
    }
    EXPECT_EQ(s[ContentAddressedSetting::gc_shards].value, 4u);
    EXPECT_EQ(s[ContentAddressedSetting::gc_interval_sec].value, 7u);
    EXPECT_EQ(countOccurrences(captured, "superseded unprefixed spelling"), 1u);
    EXPECT_NE(captured.find("gc_shards"), String::npos);
    EXPECT_NE(captured.find("gc_interval_sec"), String::npos);
}

TEST(CASContentAddressedSettings, FullyMigratedBlockWarnsAboutNothing)
{
    auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shards>4</cas_gc_shards>");
    ContentAddressedSettings s;
    ScopedCasSettingsLogCapture capture;
    s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
    EXPECT_EQ(capture.captured().find("superseded"), String::npos);
}

TEST(CASContentAddressedSettings, BothSpellingsOfOneSettingRejected)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_gc_shards>4</cas_gc_shards><gc_shards>8</gc_shards>");
    ContentAddressedSettings s;
    try
    {
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected the ambiguous pair to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(CASContentAddressedSettings, MalformedRepeatedPrefixedKeyIsRejectedBeforeParsing)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_gc_shards>not-a-number</cas_gc_shards><cas_gc_shards>8</cas_gc_shards>");
    ContentAddressedSettings s;
    try
    {
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected the repeated key to be rejected before parsing its value";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(String(e.message()).find("set more than once"), String::npos);
    }
}

TEST(CASContentAddressedSettings, MalformedPrefixedValueCannotMaskBothSpellingsConflict)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_gc_shards>not-a-number</cas_gc_shards><gc_shards>8</gc_shards>");
    ContentAddressedSettings s;
    try
    {
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected the ambiguous pair to be rejected before parsing its values";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(String(e.message()).find("both"), String::npos);
    }
}

TEST(CASContentAddressedSettings, AmbiguousConfigDoesNotWarnOrPartiallyApplySettings)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_gc_shards>4</cas_gc_shards><gc_shards>8</gc_shards>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
        captured = capture.captured();
    }
    EXPECT_EQ(captured.find("are applied"), String::npos);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_shards].changed);
}

TEST(CASContentAddressedSettings, UnknownPrefixedKeyDoesNotWarnOrPartiallyApplySettings)
{
    auto cfg = makeConfig(
        "<cas_gc_shards>3</cas_gc_shards><gc_enabled>0</gc_enabled>"
        "<cas_gc_shardz>8</cas_gc_shardz>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected the unknown prefixed key to be rejected";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_SETTING);
        }
        captured = capture.captured();
    }
    EXPECT_EQ(captured.find("are applied"), String::npos);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_shards].changed);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_enabled].changed);
}

TEST(CASContentAddressedSettings, MalformedPrefixedKeyDoesNotWarnOrPartiallyApplySettings)
{
    auto cfg = makeConfig(
        "<cas_gc_shards>3</cas_gc_shards><gc_enabled>0</gc_enabled>"
        "<cas_gc_interval_sec>not-a-number</cas_gc_interval_sec>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected the malformed prefixed key to be rejected";
        }
        catch (const Exception &)
        {
        }
        captured = capture.captured();
    }
    EXPECT_EQ(captured.find("are applied"), String::npos);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_shards].changed);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_enabled].changed);
}

TEST(CASContentAddressedSettings, ValidMixedConfigCommitsAfterAllValuesValidate)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shards>3</cas_gc_shards>"
        "<gc_enabled>0</gc_enabled>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::gc_shards].value, 3u);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_enabled].value);
}

TEST(CASContentAddressedSettings, SemanticInvalidPrefixedKeyDoesNotWarnOrPartiallyApplySettings)
{
    auto cfg = makeConfig(
        "<gc_enabled>0</gc_enabled><cas_gc_shards>0</cas_gc_shards>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected the semantically invalid prefixed key to be rejected";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
        captured = capture.captured();
    }
    EXPECT_EQ(captured.find("are applied"), String::npos);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_enabled].changed);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_shards].changed);
}

TEST(CASContentAddressedSettings, InvalidEnumDoesNotWarnOrPartiallyApplySettings)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id><gc_enabled>0</gc_enabled>"
        "<cas_blob_hash>md5</cas_blob_hash>");
    ContentAddressedSettings s;
    String captured;
    {
        ScopedCasSettingsLogCapture capture;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected the invalid hash algorithm to be rejected";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
        captured = capture.captured();
    }
    EXPECT_EQ(captured.find("are applied"), String::npos);
    EXPECT_FALSE(s[ContentAddressedSetting::gc_enabled].changed);
}

/// Poco renders a repeated element as `name`, `name[1]`. A key of ours that appears twice must be
/// recognized by its base name rather than passed over as foreign, or the first value would silently win.
TEST(CASContentAddressedSettings, RepeatedKeyRejectedInEitherSpelling)
{
    for (const std::string & spelling : {std::string("gc_shards"), std::string("cas_gc_shards")})
    {
        SCOPED_TRACE(spelling);
        auto cfg = makeConfig(
            "<cas_server_root_id>srv1</cas_server_root_id>"
            "<" + spelling + ">4</" + spelling + ">"
            "<" + spelling + ">8</" + spelling + ">");
        ContentAddressedSettings s;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected a repeated key to be rejected";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

TEST(CASContentAddressedSettings, SkipAccessCheckKeepsItsBareSpelling)
{
    auto with = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<skip_access_check>1</skip_access_check>");
    ContentAddressedSettings s;
    s.loadFromConfig(*with, "disk", "/scratch", "/scratch", identity_macros);
    EXPECT_TRUE(s.skipAccessCheck());

    auto prefixed = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_skip_access_check>1</cas_skip_access_check>");
    ContentAddressedSettings rejected;
    try
    {
        rejected.loadFromConfig(*prefixed, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected `cas_skip_access_check` to be unknown";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_SETTING);
    }
}

TEST(CASContentAddressedSettings, PrefixedGcsCapIsNotACasSetting)
{
    auto cfg = makeConfig(
        "<cas_server_root_id>srv1</cas_server_root_id>"
        "<cas_gcs_max_conditional_put_bytes>4096</cas_gcs_max_conditional_put_bytes>");
    ContentAddressedSettings s;
    try
    {
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
        FAIL() << "expected the prefixed cap name to be unknown";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_SETTING);
    }
}

TEST(CASContentAddressedSettings, ValidateFailsClosed)
{
    {
        auto cfg = makeConfig("<cas_gc_shards>1</cas_gc_shards>");
        ContentAddressedSettings s;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected an exception";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        }
    }
    {
        auto cfg = makeConfig("<cas_server_root_id></cas_server_root_id>");
        ContentAddressedSettings s;
        try
        {
            s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
            FAIL() << "expected an exception";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
    {
        auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_gc_shards>0</cas_gc_shards>");
        ContentAddressedSettings s;
        EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
    }
    {
        auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_blob_hash>md5</cas_blob_hash>");
        ContentAddressedSettings s;
        EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
    }
}

TEST(CASContentAddressedSettings, RelativeScratchPathAnchored)
{
    auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_scratch_path>rel/dir</cas_scratch_path>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/data", "/data/disks/x/cas_scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::scratch_path].value, "/data/rel/dir");
}

TEST(CASContentAddressedSettings, AbsentScratchPathUsesDefaultVerbatim)
{
    auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/data", "/data/disks/x/cas_scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::scratch_path].value, "/data/disks/x/cas_scratch");
}
