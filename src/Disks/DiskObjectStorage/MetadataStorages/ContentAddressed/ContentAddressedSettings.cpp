#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <algorithm>
#include <filesystem>
#include <string_view>
#include <vector>

#include <fmt/ranges.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_SETTING;
}

namespace
{
/// Poco renders repeated XML elements as `name`, `name[1]`, `name[2]` -- the convention
/// `StorageURL` and `HTTPDictionarySource` both handle when reading repeated `<header>` elements.
/// A key of ours that appears twice must be recognised by its base name rather than passed over as
/// foreign, or the first value would silently win where a duplicate used to be an error.
struct ConfigKeyName
{
    std::string_view base;
    bool repeated = false;
};

ConfigKeyName splitRepeatIndex(const std::string & key)
{
    const auto bracket = key.find('[');
    if (bracket == std::string::npos)
        return {key, false};
    return {std::string_view(key).substr(0, bracket), true};
}

constexpr std::string_view CAS_KEY_PREFIX = "cas_";
}

/// The disk configuration element is shared by the object-storage and generic disk layers, as
/// well as CAS. Only the user-facing CAS keys carry the `cas_` prefix; every other key belongs to
/// one of the other consumers. The declarations keep their unprefixed internal setting names.
#define LIST_OF_CONTENT_ADDRESSED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, scratch_path, "", "Server-local scratch dir for the write-buffer spill; a relative value is anchored to the server data path", 0) \
    DECLARE(Bool,   gc_enabled, true, "Run the background GC scheduler on this disk", 0) \
    DECLARE(UInt64, gc_interval_sec, 60, "Seconds between background GC rounds (>= 1)", 0) \
    DECLARE(String, blob_hash, "cityhash128", "Pool blob content-hash function (cityhash128 | xxh3-128 | sha256); fixed at pool creation", 0) \
    DECLARE(Bool,   blob_hash_allow_new, false, "Explicit opt-in to admit a NEW hash algo into an existing pool's algos_used", 0) \
    DECLARE(UInt64, gc_snapshot_generations_to_keep, 3, "GC snapshot generations retained", 0) \
    DECLARE(UInt64, gc_shards, 1, "Blob-hash-prefix reducer shards (>= 1); creation-time only", 0) \
    DECLARE(UInt64, manifest_sweep_list_budget_keys, 1000, "Orphan-manifest sweep LIST budget per round", 0) \
    DECLARE(UInt64, manifest_sweep_delete_budget_keys, 100, "Orphan-manifest sweep candidate budget per round: keys that pass every retain check, whose bodies are read and decided", 0) \
    DECLARE(UInt64, gc_round_graduation_budget, 5000, "Blob graduation (condemned -> delete_pending) cohort cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_redelete_budget, 5000, "Blob redelete (exact-token delete of a prior delete_pending row) cohort cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_sweep_namespace_budget, 20, "Orphan-manifest sweep: distinct namespaces per page whose protection view may be built (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_sweep_recovery_op_budget, 5000, "Orphan-manifest sweep: committed-tail ref-log GET/decode ops the recovery walk may spend per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_ref_cleanup_budget, 5000, "Ref-object cleanup (covered log/snapshot deletes) cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_prefix_wholesale_budget, 20000, "Generation-prefix wholesale delete (prune only) object cap per round (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_handoff_prefix_wholesale_budget, 5000, "Post-CAS hand-off generation-prefix reclaim object cap per round, reserved separately from gc_round_prefix_wholesale_budget so a prune-heavy round cannot starve the one-shot hand-off (0 = unbounded)", 0) \
    DECLARE(UInt64, gc_round_outcome_entry_budget, 5000, "GcOutcomes per-round entry cap across the redelete/spared audit log (0 = unbounded)", 0) \
    DECLARE(UInt64, mount_lease_ttl_ms, 30000, "Mount lease validity after a successful claim or renewal, in milliseconds", 0) \
    DECLARE(UInt64, mount_renew_period_ms, 10000, "Interval between background mount lease renewals, in milliseconds", 0) \
    DECLARE(Bool, unsafe_remount_no_delay, false, "Reclaim a mount slot that carries this server's own uuid at once after a hard restart, without observing the slot's token for the lease TTL. Unsafe whenever two processes can hold the same server_uuid (a copied uuid file, a stalled predecessor): after such a reclaim the predecessor can still start conditional writes until its own cutoff (confirmed deadline − margin − 2 × envelope) or until its next renewal meets the token guard, and a request it already sent may materialize later. Ref-log keys carry (writer_epoch, sequence) and creates are conditional, so two writers can never commit different bodies to one key, and recovery's epoch seal settles stragglers -- the exposure is availability, not data: recovery fails closed after 64 successive seal-create attempts displaced by newly materializing old-epoch transactions. Intended for test stands and deployments that guarantee one process per uuid", 0) \
    DECLARE(String, server_root_id, "", "REQUIRED explicit layout subtree identity; macros expand as in the s3 endpoint", 0) \
    DECLARE(UInt64, part_folder_cache_bytes, 64ULL << 20, "Part-folder view cache byte budget (0 disables retention)", 0) \
    DECLARE(UInt64, part_folder_cache_max_entries, 10000, "Part-folder view cache entry cap", 0) \
    DECLARE(UInt64, part_folder_cache_max_entry_bytes, 16ULL << 20, "Oversized part-folder views bypass retention above this size", 0) \
    DECLARE(UInt64, manifest_decode_cache_bytes, 128ULL << 20, "Manifest DECODE cache byte budget (0 disables)", 0) \
    DECLARE(UInt64, gc_meta_pool_size, 16, "Bounded pool size for GC per-hash freshness-meta writes", 0) \
    DECLARE(UInt64, gc_read_concurrency, 16, "Bounded pool size for the GC fold's read-ahead of checkpoints, ref logs, manifest bodies and zero-candidate HEADs; 1 disables read-ahead", 0) \
    DECLARE(UInt64, gc_bulk_delete_chunk_keys, 1000, "Keys per batch delete request in GC's write-once families (owner-removed manifest bodies, covered ref logs and snapshots); 1 to 1000", 0) \
    DECLARE(UInt64, attempt_timeout_ms, 5000, "Budget for one HTTP attempt of a writable Native mount's control-plane requests (read, head, list, remove, conditional write), at least 1. With the connect cap it forms the attempt envelope the lease arithmetic reserves", 0) \
    DECLARE(UInt64, lease_safety_margin_ms, 2000, "Startup-only margin validated against the mount lease TTL: attempt envelope + this must be strictly less than the TTL, and renew period + 2 × envelope + this too", 0) \
    DECLARE(String, staging_backend, "local", "Blob staging backend (local | s3); s3 is opt-in", 0) \

DECLARE_SETTINGS_TRAITS(ContentAddressedSettingsTraits, LIST_OF_CONTENT_ADDRESSED_SETTINGS, CONTENT_ADDRESSED_SETTINGS_SUPPORTED_TYPES)

struct ContentAddressedSettingsImpl : public BaseSettings<ContentAddressedSettingsTraits>
{
    /// Parsed by `validate` from the corresponding string setting; cached here (rather than
    /// re-parsed on every access) because the public header only forward-declares
    /// `Cas::StagingBackend` and cannot store it by value.
    Cas::BlobHashAlgo blob_hash_algo_cached = Cas::BlobHashAlgo::CityHash128;
    bool skip_access_check_cached = false;
    Cas::StagingBackend staging_backend_cached = Cas::StagingBackend::Local;
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

    ContentAddressedSettings candidate(*this);
    std::vector<std::string> prefixed_names;
    std::vector<std::string> legacy_names;

    for (const std::string & key : config_keys)
    {
        const auto [base, repeated] = splitRepeatIndex(key);

        if (base.starts_with(CAS_KEY_PREFIX))
        {
            if (repeated)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "content_addressed disk `{}`: `{}` is set more than once", config_prefix, base);
            prefixed_names.emplace_back(base.substr(CAS_KEY_PREFIX.size()));
        }
        else if (ContentAddressedSettingsImpl::hasBuiltin(base))
        {
            if (repeated)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "content_addressed disk `{}`: `{}` is set more than once", config_prefix, base);
            legacy_names.emplace_back(base);
        }
        /// Anything else belongs to another consumer of this disk block -- the object storage, the
        /// generic disk layer, the proxy resolver -- and is neither read nor judged here.
    }

    for (const std::string & key : legacy_names)
    {
        if (std::find(prefixed_names.begin(), prefixed_names.end(), key) != prefixed_names.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "content_addressed disk `{}`: both `{}` and `cas_{}` are set; remove the unprefixed "
                "one", config_prefix, key, key);
    }

    for (const std::string & key : config_keys)
    {
        const auto base = splitRepeatIndex(key).base;
        if (base.starts_with(CAS_KEY_PREFIX))
        {
            const auto internal_name = base.substr(CAS_KEY_PREFIX.size());
            if (!ContentAddressedSettingsImpl::hasBuiltin(internal_name))
                BaseSettingsHelpers::throwSettingNotFound(base);
            candidate.impl->set(internal_name, config.getString(config_prefix + "." + key));
        }
    }

    for (const std::string & key : legacy_names)
    {
        candidate.impl->set(key, config.getString(config_prefix + "." + key));
    }

    /// Not a CAS setting: the generic disk layer reads this same unprefixed key for its own access
    /// check, so one spelling must serve both.
    candidate.impl->skip_access_check_cached = config.getBool(config_prefix + ".skip_access_check", false);

    auto & settings = candidate;

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
    /// `<cas_server_root_id>{replica}</cas_server_root_id>`. An unknown macro throws (fail closed, via the
    /// caller-supplied `expand_macros`). Gated on `.changed`: assigning unconditionally would mark the
    /// field changed even when the key was ABSENT from config, defeating `validate`'s `.changed` check
    /// below (the ABSENT-vs-invalid `NO_ELEMENTS_IN_CONFIG`-vs-`BAD_ARGUMENTS` distinction) -- a missing
    /// key must reach `validate` still unchanged, not silently expanded-in-place to the same empty string.
    if (settings[ContentAddressedSetting::server_root_id].changed)
        settings[ContentAddressedSetting::server_root_id] = expand_macros(settings[ContentAddressedSetting::server_root_id].value);

    candidate.validate();

    /// The unprefixed spelling is accepted for a bounded period, because configurations using it
    /// already exist outside this repository. Deleting this block is what closes that period: an
    /// unprefixed CAS setting name then throws instead, naming the spelling to use.
    if (!legacy_names.empty())
        LOG_WARNING(getLogger("ContentAddressedSettings"),
            "content_addressed disk `{}`: {} use the superseded unprefixed spelling and are applied "
            "for now; write them with the `cas_` prefix. Support for the unprefixed spelling will be "
            "removed.", config_prefix, fmt::join(legacy_names, ", "));

    impl.swap(candidate.impl);
}

void ContentAddressedSettings::validate()
{
    auto & settings = *this;

    if (settings[ContentAddressedSetting::gc_interval_sec] == 0 || settings[ContentAddressedSetting::gc_shards] == 0
        || settings[ContentAddressedSetting::gc_read_concurrency] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: cas_gc_interval_sec, cas_gc_shards and cas_gc_read_concurrency must be >= 1 "
            "(got {}, {}, {})",
            settings[ContentAddressedSetting::gc_interval_sec].value, settings[ContentAddressedSetting::gc_shards].value,
            settings[ContentAddressedSetting::gc_read_concurrency].value);

    if (settings[ContentAddressedSetting::gc_bulk_delete_chunk_keys] == 0
        || settings[ContentAddressedSetting::gc_bulk_delete_chunk_keys] > 1000)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: gc_bulk_delete_chunk_keys must be between 1 and 1000 (got {})",
            settings[ContentAddressedSetting::gc_bulk_delete_chunk_keys].value);

    if (settings[ContentAddressedSetting::attempt_timeout_ms] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: cas_attempt_timeout_ms must be >= 1 (got {})",
            settings[ContentAddressedSetting::attempt_timeout_ms].value);

    if (settings[ContentAddressedSetting::mount_lease_ttl_ms] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: cas_mount_lease_ttl_ms must be >= 1 (got {})",
            settings[ContentAddressedSetting::mount_lease_ttl_ms].value);

    if (settings[ContentAddressedSetting::mount_renew_period_ms] == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "content_addressed disk: cas_mount_renew_period_ms must be >= 1 (got {})",
            settings[ContentAddressedSetting::mount_renew_period_ms].value);

    /// The layout subtree identity is explicit and REQUIRED — no default, so an ABSENT key throws a
    /// typed `NO_ELEMENTS_IN_CONFIG` (mirroring the `metadata_type` check in `MetadataStorageFactory`),
    /// distinct from a PRESENT-but-invalid value, which falls through to `validateServerRootId`'s
    /// `BAD_ARGUMENTS` below. `.changed` is exactly "the config had this key" (or a caller set it
    /// explicitly via the subscript operator); an unset field never reaches here as anything but empty.
    if (!settings[ContentAddressedSetting::server_root_id].changed)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Expected `cas_server_root_id` in config for a content-addressed disk");

    Cas::validateServerRootId(settings[ContentAddressedSetting::server_root_id].value);

    impl->blob_hash_algo_cached = Cas::parseBlobHashAlgo(settings[ContentAddressedSetting::blob_hash].value);
    impl->staging_backend_cached = ContentAddressedMetadataStorage::parseStagingBackend(settings[ContentAddressedSetting::staging_backend].value);
}

Cas::BlobHashAlgo ContentAddressedSettings::blobHashAlgo() const
{
    return impl->blob_hash_algo_cached;
}

bool ContentAddressedSettings::skipAccessCheck() const
{
    return impl->skip_access_check_cached;
}

Cas::StagingBackend ContentAddressedSettings::stagingBackend() const
{
    return impl->staging_backend_cached;
}

}
