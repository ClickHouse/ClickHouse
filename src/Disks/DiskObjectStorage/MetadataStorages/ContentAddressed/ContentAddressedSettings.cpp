#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <filesystem>
#include <set>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

/// The `cas` disk block is shared with the generic object-storage/disk layer, whose
/// keys are consumed elsewhere (`ObjectStorageFactory`, `S3Settings`, `MetadataStorageFactory`'s
/// `getObjectKeyCompatiblePrefix`, `IDisk`, `DiskFromAST` for the inline SQL `disk(...)` form,
/// `RegisterDiskObjectStorage`'s fake-transaction gate) and must be skipped here rather than rejected
/// as unknown. As of 2026-07-21 this list covers ALL in-repo CAS disk configs and inline `disk(...)`
/// definitions, enumerated from four sources, each filtered down to the keys that are direct
/// children of an actual `metadata_type=cas` disk block (the raw `rg -o` output below
/// also contains disk-name and policy/volume wrapper tags, which are not config keys at all):
///   1) `rg -o "<([a-z_0-9]+)>" -r '$1' utils/ca-soak/configs/storage_conf*.xml utils/ca-soak/configs/storage_overrides*.xml`
///   2) every CAS integration-test disk config, both the `storage_conf.xml` bodies and the
///      per-node `server_root_id_node*.xml` overrides (`git grep -l content_addressed
///      tests/integration | grep -E '\.xml$'`) -- and every integration `test.py`
///      (`git ls-files 'tests/integration/*/test.py' | xargs grep -l
///      'metadata_type.*content_addressed'`) confirmed EMPTY: no integration test builds a CAS disk
///      via inline SQL `disk(...)`, only via these XML configs.
///   3) every CAS disk config under `tests/config/config.d/cas_*.xml` (the
///      stateless-lane XML configs) -- these are the only place in the tree with a LOCAL
///      `object_storage_type` CAS disk, so they are the only source of the generic `path` key below.
///   4) every inline `disk(...)` SQL construct across ALL `cas_*`
///      stateless tests, `04278`-`04300` (pre-dating this settings struct) through `05002`-`05015`
///      (current) -- these supply `name` (read by `DiskFromAST` to derive the ad-hoc disk's name)
///      and `use_fake_transaction` (validated generically in `RegisterDiskObjectStorage.cpp` against
///      EVERY metadata type that needs a real transaction, not a CAS-specific check -- exercised by
///      `05015_cas_reject_fake_transaction` deliberately setting it to assert the REJECTION, which
///      needs the key to reach that check rather than being rejected earlier as unknown). The
///      `04278`-`04300` range turned up no keys beyond what `05002`-`05015` already required.
/// Any new CAS config FAMILY -- a new XML config directory or a new inline-`disk()` test pattern --
/// added to the tree needs the same four-way scan repeated against it and this note updated.
/// `skip_access_check` is deliberately NOT in this set: it is registered as a CAS setting
/// below (the same config key also has meaning to `IDisk::startupImpl`, which drops it before
/// `metadata_storage->startup()` runs, but that does not make it foreign here).
static const std::set<std::string> non_cas_keys = {
    "type", "object_storage_type", "metadata_type", "path", "name", "use_fake_transaction",
    "endpoint", "access_key_id", "secret_access_key", "region", "use_environment_credentials",
    "readonly", "expect_continue_min_bytes", "http_client", "key_compatibility_prefix",
    /// `http_client = gcp_oauth` has TWO token sources, and `requestBearerToken` picks between them:
    /// the GCE metadata server, and Application Default Credentials. Both sets are consumed by
    /// `diskSettings.cpp` into the client configuration, and both must be accepted here — the ADC
    /// triple is the only way to run `gcp_oauth` off a GCE instance, which is what a developer or a
    /// non-GCE deployment uses.
    "metadata_service", "request_token_path", "service_account",
    "google_adc_client_id", "google_adc_client_secret", "google_adc_refresh_token",
    "max_single_part_upload_size",
};

/// Config-key convention: the disk block already scopes every key to this disk, so no
/// key below carries a redundant `cas_`/`ca_` prefix (e.g. `part_folder_cache_bytes`, not
/// `cas_part_folder_cache_bytes`).
#define LIST_OF_CONTENT_ADDRESSED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, scratch_path, "", "Server-local scratch dir for the write-buffer spill; a relative value is anchored to the server data path", 0) \
    DECLARE(Bool,   gc_enabled, true, "Run the background GC scheduler on this disk", 0) \
    DECLARE(UInt64, gc_interval_sec, 60, "Seconds between background GC rounds (>= 1)", 0) \
    DECLARE(String, blob_hash, "cityhash128", "Pool blob content-hash function (cityhash128 | xxh3-128 | sha256); fixed at pool creation", 0) \
    DECLARE(Bool,   blob_hash_allow_new, false, "Explicit opt-in to admit a NEW hash algo into an existing pool's algos_used", 0) \
    DECLARE(Bool,   skip_access_check, false, "Skip the boot-time capability probe (start now, fix later)", 0) \
    DECLARE(UInt64, gc_snapshot_generations_to_keep, 3, "GC snapshot generations retained", 0) \
    DECLARE(UInt64, gc_shards, 1, "Blob-hash-prefix reducer shards (>= 1); creation-time only", 0) \
    DECLARE(UInt64, manifest_sweep_list_budget_keys, 1000, "Orphan-manifest sweep LIST budget per round", 0) \
    DECLARE(UInt64, manifest_sweep_delete_budget_keys, 100, "Orphan-manifest sweep DELETE budget per round", 0) \
    DECLARE(UInt64, gc_round_graduation_budget, 5000, "Blob graduation (condemned -> delete_pending) cohort cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_redelete_budget, 5000, "Blob redelete (exact-token delete of a prior delete_pending row) cohort cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_sweep_namespace_budget, 20, "Orphan-manifest sweep: distinct namespaces per page whose protection view may be built (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_sweep_recovery_op_budget, 5000, "Orphan-manifest sweep: committed-tail ref-log GET/decode ops the recovery walk may spend per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_ref_cleanup_budget, 5000, "Ref-object cleanup (covered log/snapshot deletes) cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_prefix_wholesale_budget, 20000, "Generation-prefix wholesale delete (prune only) object cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_handoff_prefix_wholesale_budget, 5000, "Post-CAS hand-off generation-prefix reclaim object cap per round, reserved separately from gc_round_prefix_wholesale_budget so a prune-heavy round cannot starve the one-shot hand-off (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_outcome_entry_budget, 5000, "GcOutcomes per-round entry cap across the redelete/spared audit log (0 = unbounded)", 0) \
    DECLARE(String, server_root_id, "", "REQUIRED explicit layout subtree identity; macros expand as in the s3 endpoint", 0) \
    DECLARE(UInt64, gcs_max_conditional_put_bytes, 1ULL << 30, "GCS single-PUT budget for genuine conditional writes (generation-token stores only)", 0) \
    DECLARE(UInt64, part_folder_cache_bytes, 64ULL << 20, "Part-folder view cache byte budget (0 disables retention)", 0) \
    DECLARE(UInt64, part_folder_cache_max_entries, 10000, "Part-folder view cache entry cap", 0) \
    DECLARE(UInt64, part_folder_cache_max_entry_bytes, 16ULL << 20, "Oversized part-folder views bypass retention above this size", 0) \
    DECLARE(String, part_folder_validate, "always", "ForceFresh body re-proof policy (always | never | age <seconds>)", 0) \
    DECLARE(UInt64, manifest_decode_cache_bytes, 128ULL << 20, "Manifest DECODE cache byte budget (0 disables)", 0) \
    DECLARE(UInt64, gc_meta_pool_size, 16, "Bounded pool size for GC per-hash freshness-meta writes", 0) \
    DECLARE(String, staging_backend, "local", "Blob staging backend (local | s3); s3 is opt-in", 0) \

DECLARE_SETTINGS_TRAITS(ContentAddressedSettingsTraits, LIST_OF_CONTENT_ADDRESSED_SETTINGS, CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES)

struct ContentAddressedSettingsImpl : public BaseSettings<ContentAddressedSettingsTraits>
{
    /// Parsed by `validate` from the corresponding string setting; cached here (rather than
    /// re-parsed on every access) because the public header only forward-declares
    /// `Cas::StagingBackend` / `Cas::PartFolderValidate` and cannot store them by value.
    Cas::BlobHashAlgo blob_hash_algo_cached = Cas::BlobHashAlgo::CityHash128;
    Cas::StagingBackend staging_backend_cached = Cas::StagingBackend::Local;
    Cas::PartFolderValidate part_folder_validate_cached{};
};

IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL(ContentAddressedSettingsTraits, LIST_OF_CONTENT_ADDRESSED_SETTINGS, ContentAddressedSettings, ContentAddressedSetting)

ContentAddressedSettings::ContentAddressedSettings() : impl(std::make_unique<ContentAddressedSettingsImpl>())
{
}

ContentAddressedSettings::~ContentAddressedSettings() = default;

CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES(ContentAddressedSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

ContentAddressedSettings::ContentAddressedSettings(const ContentAddressedSettings & settings)
    : impl(std::make_unique<ContentAddressedSettingsImpl>(*settings.impl))
{
}

void ContentAddressedSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const std::string & scratch_path_anchor_if_relative,
    const std::string & default_scratch_path,
    const MacroExpander & expand_macros)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const std::string & key : config_keys)
    {
        if (non_cas_keys.contains(key))
            continue;
        impl->set(key, config.getString(config_prefix + "." + key));
    }

    auto & settings = *this;

    /// Server-local scratch dir for the write-buffer spill. Mirrors how other metadata storages
    /// compute their local working dir: a real filesystem path, NEVER the object-storage key
    /// prefix. A configured RELATIVE scratch path is anchored to `scratch_path_anchor_if_relative`
    /// (the caller-provided server data path), NOT the process CWD (which varies by launch method)
    /// and NOT `default_scratch_path` -- that default is itself a per-disk subdirectory of the
    /// server data path (`.../disks/<name>/cas_scratch/`), so anchoring a relative override to it
    /// instead of to the server data path directly would silently nest the override two levels
    /// deeper than intended (review finding: this is exactly the pre-existing factory's anchor,
    /// which callers already depend on in shipped configs). The default is already absolute; only
    /// an explicit relative override needs anchoring, and only to the server-data-path anchor.
    if (settings[ContentAddressedSetting::scratch_path].changed)
    {
        if (fs::path(settings[ContentAddressedSetting::scratch_path].value).is_relative())
            settings[ContentAddressedSetting::scratch_path] = (fs::path(scratch_path_anchor_if_relative) / settings[ContentAddressedSetting::scratch_path].value).string();
    }
    else
    {
        settings[ContentAddressedSetting::scratch_path] = default_scratch_path;
    }

    /// Phase 0 (mount safety): macros expand here exactly as in the s3 `endpoint`
    /// (`ObjectStorageFactory`): on a multi-replica stand every replica mounts ONE shared pool
    /// (same endpoint) and must own a DISTINCT subtree, so the natural single-template config is
    /// `<server_root_id>{replica}</server_root_id>`. An unknown macro throws (fail closed, via the
    /// caller-supplied `expand_macros`). Gated on `.changed`: assigning unconditionally would mark the
    /// field changed even when the key was ABSENT from config, defeating `validate`'s `.changed` check
    /// below (the ABSENT-vs-invalid `NO_ELEMENTS_IN_CONFIG`-vs-`BAD_ARGUMENTS` distinction) -- a missing
    /// key must reach `validate` still unchanged, not silently expanded-in-place to the same empty string.
    if (settings[ContentAddressedSetting::server_root_id].changed)
        settings[ContentAddressedSetting::server_root_id] = expand_macros(settings[ContentAddressedSetting::server_root_id].value);

    validate();
}

void ContentAddressedSettings::validate()
{
    auto & settings = *this;

    if (settings[ContentAddressedSetting::gc_interval_sec] == 0 || settings[ContentAddressedSetting::gc_shards] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: gc_interval_sec and gc_shards must be >= 1 (got {}, {})",
            settings[ContentAddressedSetting::gc_interval_sec].value, settings[ContentAddressedSetting::gc_shards].value);

    /// The layout subtree identity is explicit and REQUIRED — no default, so an ABSENT key throws a
    /// typed `NO_ELEMENTS_IN_CONFIG` (mirroring the `metadata_type` check in `MetadataStorageFactory`),
    /// distinct from a PRESENT-but-invalid value, which falls through to `validateServerRootId`'s
    /// `BAD_ARGUMENTS` below. `.changed` is exactly "the config had this key" (or a caller set it
    /// explicitly via the subscript operator); an unset field never reaches here as anything but empty.
    if (!settings[ContentAddressedSetting::server_root_id].changed)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Expected `server_root_id` in config for a content-addressed disk");

    Cas::validateServerRootId(settings[ContentAddressedSetting::server_root_id].value);

    impl->blob_hash_algo_cached = Cas::parseBlobHashAlgo(settings[ContentAddressedSetting::blob_hash].value);
    impl->staging_backend_cached = ContentAddressedMetadataStorage::parseStagingBackend(settings[ContentAddressedSetting::staging_backend].value);
    impl->part_folder_validate_cached = ContentAddressedMetadataStorage::parsePartFolderValidate(settings[ContentAddressedSetting::part_folder_validate].value);
}

Cas::BlobHashAlgo ContentAddressedSettings::blobHashAlgo() const
{
    return impl->blob_hash_algo_cached;
}

Cas::StagingBackend ContentAddressedSettings::stagingBackend() const
{
    return impl->staging_backend_cached;
}

Cas::PartFolderValidate ContentAddressedSettings::partFolderValidate() const
{
    return impl->part_folder_validate_cached;
}

}
