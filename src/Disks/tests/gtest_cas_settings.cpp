#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Common/Exception.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/AutoPtr.h>
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
    extern const ContentAddressedSettingsUInt64 gc_shards;
    extern const ContentAddressedSettingsUInt64 gc_interval_sec;
    extern const ContentAddressedSettingsUInt64 gcs_max_conditional_put_bytes;
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
}

TEST(CASContentAddressedSettings, DefaultsAndOverridesLand)
{
    auto cfg = makeConfig("<server_root_id>srv1</server_root_id><gc_shards>4</gc_shards>");
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
        const std::string setting = "deduplication_" + suffix;
        SCOPED_TRACE(setting);
        auto cfg = makeConfig(
            "<server_root_id>srv1</server_root_id><" + setting + ">4096</" + setting + ">");
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

/// The generation-store single-PUT cap applies to every conditional non-blob write, including
/// create-if-absent artifacts and conditional replacements. Blob publication remains unconditional.
TEST(CASContentAddressedSettings, ConditionalPutCapParsesAndDefaults)
{
    auto with_override = makeConfig(
        "<server_root_id>srv1</server_root_id>"
        "<gcs_max_conditional_put_bytes>4096</gcs_max_conditional_put_bytes>");
    ContentAddressedSettings s;
    s.loadFromConfig(*with_override, "disk", "/data", "/data/scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::gcs_max_conditional_put_bytes].value, 4096u);

    auto without = makeConfig("<server_root_id>srv1</server_root_id>");
    ContentAddressedSettings d;
    d.loadFromConfig(*without, "disk", "/data", "/data/scratch", identity_macros);
    EXPECT_EQ(d[ContentAddressedSetting::gcs_max_conditional_put_bytes].value, 1ULL << 30);
}

/// The cap's pre-release name carries no alias: `CAS` ships no persisted data yet, so a config using
/// the old key must fail loudly rather than be silently accepted under a compatibility shim.
TEST(CASContentAddressedSettings, LegacyTokenProducingPutCapNameRejected)
{
    auto cfg = makeConfig(
        "<server_root_id>srv1</server_root_id>"
        "<gcs_max_token_producing_put_bytes>4096</gcs_max_token_producing_put_bytes>");
    ContentAddressedSettings s;
    try
    {
        s.loadFromConfig(*cfg, "disk", "/data", "/data/scratch", identity_macros);
        FAIL() << "expected the legacy cap name to be rejected as unknown";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_SETTING);
    }
}

TEST(CASContentAddressedSettings, UnknownKeyRejected)
{
    auto cfg = makeConfig("<server_root_id>srv1</server_root_id><gc_shardz>4</gc_shardz>");
    ContentAddressedSettings s;
    EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
}

TEST(CASContentAddressedSettings, ObjectStorageKeysSkipped)
{
    auto cfg = makeConfig(
        "<metadata_type>cas</metadata_type><type>object_storage</type>"
        "<object_storage_type>s3</object_storage_type><endpoint>http://x/y</endpoint>"
        "<access_key_id>k</access_key_id><secret_access_key>s</secret_access_key>"
        "<server_root_id>srv1</server_root_id>"
        /// Regression pin (stateless-lane startup fix): the local-object-storage CAS disk config
        /// (`tests/config/config.d/cas_storage_policy_for_merge_tree_by_default.xml`)
        /// sets `path` -- the generic local-object-storage pool root, consumed by
        /// `ObjectStorageFactory`/`IDisk`, same class as `endpoint`/`access_key_id` above -- and it
        /// was missing from `non_cas_keys`, which threw `UNKNOWN_SETTING` at server startup.
        "<path>cas_pool/</path>"
        /// Regression pin: `name`, read generically by `DiskFromAST` for the inline SQL `disk(...)`
        /// form used by the `05002`-`05015` CAS stateless tests, was likewise missing and threw
        /// `UNKNOWN_SETTING` for every one of those tests (only the XML-config `path` gap was fixed
        /// first; `name` surfaced once the stateless lane actually ran end to end).
        "<name>cas_test_disk</name>"
        /// Regression pin: `use_fake_transaction`, validated generically in
        /// `RegisterDiskObjectStorage.cpp` for every metadata type that needs a real transaction (not
        /// a CAS-specific check), must reach that check rather than being rejected here as unknown --
        /// `05015_cas_reject_fake_transaction` depends on it doing so.
        "<use_fake_transaction>1</use_fake_transaction>"
        /// Regression pin: `http_client = gcp_oauth` has TWO token sources and `requestBearerToken`
        /// picks between them, so BOTH key sets must be accepted. The metadata-server triple was
        /// missing (found by the CAS-over-GCS integration fixture, which had to point the OAuth client
        /// at a fake metadata server); the ADC triple was missing too, and it is the only way to run
        /// `gcp_oauth` off a GCE instance. Either omission threw `UNKNOWN_SETTING` at startup.
        "<http_client>gcp_oauth</http_client>"
        "<metadata_service>metadata.example.invalid</metadata_service>"
        "<request_token_path>computeMetadata/v1/instance/service-accounts</request_token_path>"
        "<service_account>cas@example.invalid</service_account>"
        "<google_adc_client_id>cas-adc-client</google_adc_client_id>"
        "<google_adc_client_secret>cas-adc-secret</google_adc_client_secret>"
        "<google_adc_refresh_token>cas-adc-refresh</google_adc_refresh_token>"
        /// Same class: a generic S3 request setting, not a CAS one.
        "<max_single_part_upload_size>1073741824</max_single_part_upload_size>");
    ContentAddressedSettings s;
    EXPECT_NO_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros));
}

TEST(CASContentAddressedSettings, ValidateFailsClosed)
{
    {   /// missing server_root_id: ABSENT key -> typed NO_ELEMENTS_IN_CONFIG (distinct from a
        /// present-but-invalid value, checked below), mirroring the pre-F4b factory behavior.
        auto cfg = makeConfig("<gc_shards>1</gc_shards>");
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
    {   /// present but invalid (empty) server_root_id -> BAD_ARGUMENTS from
        /// `Cas::validateServerRootId`, not NO_ELEMENTS_IN_CONFIG.
        auto cfg = makeConfig("<server_root_id></server_root_id>");
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
    {   /// zero gc_shards
        auto cfg = makeConfig("<server_root_id>srv1</server_root_id><gc_shards>0</gc_shards>");
        ContentAddressedSettings s;
        EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
    }
    {   /// unknown blob_hash spelling
        auto cfg = makeConfig("<server_root_id>srv1</server_root_id><blob_hash>md5</blob_hash>");
        ContentAddressedSettings s;
        EXPECT_THROW(s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros), Exception);
    }
}

TEST(CASContentAddressedSettings, RelativeScratchPathAnchored)
{
    /// Reproduces the pre-F4b factory's anchor behavior (review finding, Critical): a relative
    /// `scratch_path` override is anchored to the SERVER DATA PATH (`scratch_path_anchor_if_relative`)
    /// -- NOT to `default_scratch_path`, which is itself a per-disk subdirectory
    /// (`.../disks/<name>/cas_scratch`) that must never leak into an override's resolved path.
    auto cfg = makeConfig("<server_root_id>srv1</server_root_id><scratch_path>rel/dir</scratch_path>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/data", "/data/disks/x/cas_scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::scratch_path].value, "/data/rel/dir");
}

TEST(CASContentAddressedSettings, AbsentScratchPathUsesDefaultVerbatim)
{
    /// Absent key -> `default_scratch_path` verbatim, unaffected by the anchor (the per-disk default
    /// already lives under the server data path; only an explicit relative OVERRIDE needs anchoring).
    auto cfg = makeConfig("<server_root_id>srv1</server_root_id>");
    ContentAddressedSettings s;
    s.loadFromConfig(*cfg, "disk", "/data", "/data/disks/x/cas_scratch", identity_macros);
    EXPECT_EQ(s[ContentAddressedSetting::scratch_path].value, "/data/disks/x/cas_scratch");
}
