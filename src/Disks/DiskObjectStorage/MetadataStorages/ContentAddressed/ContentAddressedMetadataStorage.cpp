#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <IO/S3/Client.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasSentinelProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContentAddressedGarbageCollectionLog.h>
#include <Interpreters/ContentAddressedLog.h>
#include <Common/CurrentThread.h>
#include <base/getThreadId.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/sleep.h>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <ctime>
#include <unordered_set>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int READONLY;
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_STATE;
}

namespace ContentAddressedSetting
{
    extern const ContentAddressedSettingsString scratch_path;
    extern const ContentAddressedSettingsString server_root_id;
    extern const ContentAddressedSettingsBool gc_enabled;
    extern const ContentAddressedSettingsUInt64 gc_interval_sec;
    extern const ContentAddressedSettingsUInt64 gc_snapshot_generations_to_keep;
    extern const ContentAddressedSettingsUInt64 gc_shards;
    extern const ContentAddressedSettingsUInt64 manifest_sweep_list_budget_keys;
    extern const ContentAddressedSettingsUInt64 manifest_sweep_delete_budget_keys;
    extern const ContentAddressedSettingsUInt64 gc_round_graduation_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_redelete_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_sweep_namespace_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_sweep_recovery_op_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_ref_cleanup_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_prefix_wholesale_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_handoff_prefix_wholesale_budget;
    extern const ContentAddressedSettingsUInt64 gc_round_outcome_entry_budget;
    extern const ContentAddressedSettingsUInt64 gcs_max_conditional_put_bytes;
    extern const ContentAddressedSettingsUInt64 part_folder_cache_bytes;
    extern const ContentAddressedSettingsUInt64 part_folder_cache_max_entries;
    extern const ContentAddressedSettingsUInt64 part_folder_cache_max_entry_bytes;
    extern const ContentAddressedSettingsUInt64 manifest_decode_cache_bytes;
    extern const ContentAddressedSettingsUInt64 gc_meta_pool_size;
    extern const ContentAddressedSettingsBool blob_hash_allow_new;
    extern const ContentAddressedSettingsBool skip_access_check;
}

namespace
{
/// The `lifecycle` column value for `system.cas_mounts` (spec §7): the pool lifecycle
/// condition collapsed to the operator-facing vocabulary. The two `Vanished*` sub-states both map to the
/// bare `vanished` -- the sub-state (replaced/forgotten) lives in the `lifecycle_reason` column,
/// so a `NULL`-free `lifecycle` stays a small, stable enumerated set.
const char * casLifecycleToString(Cas::PoolLifecycle lc)
{
    switch (lc)
    {
        case Cas::PoolLifecycle::Live:              return "live";
        case Cas::PoolLifecycle::TransientNotLive:  return "not_live";
        case Cas::PoolLifecycle::IdentityLost:      return "identity_lost";
        case Cas::PoolLifecycle::VanishedReplaced:
        case Cas::PoolLifecycle::VanishedForgotten: return "vanished";
    }
    return "unknown";
}

/// The ENUM-CLEAN `lifecycle_reason` word: the `vanished` sub-state (replaced/forgotten) so a
/// downstream `lifecycle || '(' || lifecycle_reason || ')'` yields exactly e.g. `vanished(forgotten)`.
/// Empty for every non-`vanished` state (the `lifecycle` column already fully names those). The rich [D5]
/// text is carried separately in `lifecycle_detail`.
const char * casLifecycleReasonWord(Cas::PoolLifecycle lc)
{
    switch (lc)
    {
        case Cas::PoolLifecycle::Live:
        case Cas::PoolLifecycle::TransientNotLive:
        case Cas::PoolLifecycle::IdentityLost:      return "";
        case Cas::PoolLifecycle::VanishedReplaced:  return "replaced";
        case Cas::PoolLifecycle::VanishedForgotten: return "forgotten";
    }
    return "";
}
}

/// ============================================================================================
/// The method -> operation-class inventory (rev.7 spec §1; the review artifact of Task 8).
///
/// EVERY public method of `ContentAddressedMetadataStorage` and `ContentAddressedTransaction` is listed
/// with the `CasOpClass` it routes through `checkOpAdmitted` (or "Factory" for the never-gated I/O-free
/// surface). The gate is consulted at the method's entry; `store()`/`partAccess()`/`poolAccess()` keep
/// their own terminal check (Task 5) as low-level defense, reached only in the `Live` case.
///
/// ---- ContentAddressedMetadataStorage ----
/// Factory (never gated): getType, getPath, supportsChmod, supportsStat, isReadOnly, isContentAddressed,
///   transactionIsStagingOverlay, supportsAtomicFileWrites, supportsTransactionalMutableFiles,
///   areBlobPathsRandom, getHardlinkCount, createTransaction (I/O-free -- allocates a txn), getPoolUUID,
///   serverRootId, scratchPath, stagingBackend, objectStorage, gcHealth,
///   lifecycleSnapshot (both non-store()-gated introspection reads for system.cas_mounts --
///   readable in EVERY lifecycle state including a not-live/vanished/null pool, spec §7),
///   parseStagingBackend/parsePartFolderValidate/
///   tryFromDisk (static), checkNotReadOnly, the *ForTest seams, serverPrefix/liveNamespace/
///   shadowNamespace/shadowScope/route/classifyDirectory (pure path computation, no pool I/O),
///   ownsNamespace (the relink-confirm routing predicate -- a string comparison against
///   `server_root_id`, deliberately answerable in EVERY lifecycle state).
/// Probe:       existsFile, existsDirectory, existsFileOrDirectory, listDirectory, iterateDirectory,
///              isDirectoryEmpty, getStorageObjectsIfExist, liveTreeDirHasChildren, listLiveTreeChildren.
///              EMPTY-PROOF RULE (Task 9, spec §1 [B3]): on a NON-terminal (Live/read-only) pool, an
///              empty `listDirectory` answer at a `TableDir`/`DetachedContainer` root additionally runs
///              `confirmPoolIdentityForEmptyEnumeration` (one authoritative, UNCACHED `_pool_meta` probe)
///              -- a `KeyAbsent`/`ContainerAbsent`/transport result throws the typed 668 instead of the
///              empty answer. `iterateDirectory`/`isDirectoryEmpty` inherit it (both funnel through
///              `listDirectory`). A `Vanished` pool never reaches it (the gate short-circuits `Probe`).
/// ContentRead: getFileSize, getLastModified, getStorageObjects, getBlobViewPlan, readBlobPayload,
///              prepareInManifestRead, tryGetInManifestBytes, getRelinkOffer, confirmExactRef
///              (the ONE ContentRead entry that converts the gate's refusal into its own typed
///              `Unknown` answer instead of propagating it -- a confirm never throws at its caller).
/// Write:       adoptPartFromManifest.
/// Admin:       runOneGcRoundForTest, runGarbageCollectionRoundNow, runGcRebuildNow, runFsckNow (the
///              rev.8 FSCK-on-running path -- an FSCK of a not-live disk is refused by the Admin gate).
/// Lifecycle/uncgated drivers (NOT op-gated -- they DRIVE the state): startup, shutdown, forgetDisk,
///   gcStop, gcStart. store()/partAccess()/poolAccess() are the internal accessors, not public op entries.
///   readableNamespaceFilesLife, stagingKeyPrefix, detachedRefNames, movingRefNames are post-gate helpers
///   (their public callers gate first; they reach store() only in the Live case).
///   confirmPoolIdentityForEmptyEnumeration is a post-gate helper too (the EMPTY-PROOF RULE, Task 9):
///   listDirectory calls it only after the gate admitted a Probe and only on an empty table-root answer.
///
/// ---- ContentAddressedTransaction (routes through metadata_storage.checkOpAdmitted) ----
/// Write:  writeFile (and tryCreateWriteBuffer, which funnels into it), createDirectory,
///         createDirectoryRecursive, createHardLink, moveDirectory, moveFile, replaceFile,
///         setLastModified, setReadOnly.
/// Remove: removeDirectory, removeRecursive, unlinkFile.
/// commit/tryCommit: Remove when there is nothing to publish (parts empty -- the DROP/rename path, which
///         applied its ref mutations immediately), Write when it must publish staged parts. This is what
///         lets a vanished-disk table's DROP finish (Remove -> no-op success) while a publishing commit
///         throws the typed Vanished [D5] refusal.
/// Unsupported (always throw, state-independent -- no gate needed): createMetadataFile,
///   generateObjectKeyForPath, chmod, truncateFile.
/// Factory / overlay-only (no committed-pool I/O): supportsChmod, getSubmittedForRemovalBlobs, and the
///   read-your-writes overlay readers tryGetInFlightStorageObjects/tryReadFileInFlight/
///   tryGetInFlightFileSize/hasInFlightDirectory/listInFlightDirectory (they read THIS transaction's own
///   in-memory staging; a vanished-disk transaction has none because its writes threw at writeFile).
///
/// Null-pool rule: a storage with no published pool (before `startup` or after `shutdown` -- the
/// storage-level Constructing/ShutDown lifecycle) fails loud for EVERY class, `Probe` included, via
/// `throwStorageNotStarted`. There is no benign "absent" answer for a storage that has never published a
/// pool (or torn one down); the pool lifecycle below is the sole authority for a published pool.
/// ============================================================================================

namespace
{

/// Canonical disk-relative path: components joined by single '/', no leading/trailing slashes.
/// Callers hand paths in both shapes (the Unfreezer walks shadow dirs WITH a trailing slash);
/// namespace strings and prefix matching need the canonical form.
std::string canonicalDiskPath(const std::string & path)
{
    std::string result;
    std::string component;
    auto flush = [&]
    {
        if (component.empty())
            return;
        if (!result.empty())
            result += '/';
        result += component;
        component.clear();
    };
    for (char c : path)
    {
        if (c == '/')
            flush();
        else
            component.push_back(c);
    }
    flush();
    return result;
}

/// "<first>/<rest...>" -> {first, rest} ({whole, ""} when there is no '/').
std::pair<std::string, std::string> splitFirstComponent(const std::string & s)
{
    const auto slash = s.find('/');
    if (slash == std::string::npos)
        return {s, ""};
    return {s.substr(0, slash), s.substr(slash + 1)};
}

void addFirstComponent(std::unordered_set<std::string> & out, const std::string & name)
{
    const auto slash = name.find('/');
    out.emplace(slash == std::string::npos ? name : name.substr(0, slash));
}

/// Drop a trailing `@cas@` content-addressing boundary marker from a mirrored path segment, so a
/// table-dir surfaces under its logical (unsuffixed) name in directory listings.
std::string stripCasArchiveSuffix(std::string s)
{
    const auto & suffix = Cas::kCasArchiveSuffix;
    if (s.size() >= suffix.size() && std::string_view(s).ends_with(suffix))
        s.resize(s.size() - suffix.size());
    return s;
}

std::vector<std::string> toVector(std::unordered_set<std::string> && set)
{
    return std::vector<std::string>(std::make_move_iterator(set.begin()), std::make_move_iterator(set.end()));
}

/// The server uuid string (with dashes) -> the core's UInt128 server id.
UInt128 serverIdToU128(const std::string & server_id)
{
    String hex;
    hex.reserve(32);
    for (char c : server_id)
        if (c != '-')
            hex += c;
    if (hex.size() == 32)
        return Cas::hexToU128(hex);
    /// Unit-test ids ("srv1") are not uuids — hash them stably.
    UInt128 r{};
    for (char c : server_id)
        r = r * 131 + static_cast<unsigned char>(c);
    return r == UInt128(0) ? UInt128(1) : r;
}

}

ContentAddressedMetadataStorage::ContentAddressedMetadataStorage(
    ObjectStoragePtr object_storage_,
    String storage_path_prefix_,
    String server_id_,
    String disk_name_,
    ContextPtr context_,
    const ContentAddressedSettings & settings_)
    : object_storage(std::move(object_storage_))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , server_id(std::move(server_id_))
    , server_root_id(settings_[ContentAddressedSetting::server_root_id].value)
    , disk_name(!disk_name_.empty() ? disk_name_ : storage_path_prefix)
    , local_scratch_path(settings_[ContentAddressedSetting::scratch_path].value)
    , context(context_)
    , gc_enabled(settings_[ContentAddressedSetting::gc_enabled].value)
    , gc_interval(std::chrono::seconds(settings_[ContentAddressedSetting::gc_interval_sec].value))
    , gc_snapshot_generations_to_keep(settings_[ContentAddressedSetting::gc_snapshot_generations_to_keep].value)
    , gc_shards(settings_[ContentAddressedSetting::gc_shards].value)
    , manifest_sweep_list_budget_keys(settings_[ContentAddressedSetting::manifest_sweep_list_budget_keys].value)
    , manifest_sweep_delete_budget_keys(settings_[ContentAddressedSetting::manifest_sweep_delete_budget_keys].value)
    , gc_round_graduation_budget(settings_[ContentAddressedSetting::gc_round_graduation_budget].value)
    , gc_round_redelete_budget(settings_[ContentAddressedSetting::gc_round_redelete_budget].value)
    , gc_round_sweep_namespace_budget(settings_[ContentAddressedSetting::gc_round_sweep_namespace_budget].value)
    , gc_round_sweep_recovery_op_budget(settings_[ContentAddressedSetting::gc_round_sweep_recovery_op_budget].value)
    , gc_round_ref_cleanup_budget(settings_[ContentAddressedSetting::gc_round_ref_cleanup_budget].value)
    , gc_round_prefix_wholesale_budget(settings_[ContentAddressedSetting::gc_round_prefix_wholesale_budget].value)
    , gc_round_handoff_prefix_wholesale_budget(settings_[ContentAddressedSetting::gc_round_handoff_prefix_wholesale_budget].value)
    , gc_round_outcome_entry_budget(settings_[ContentAddressedSetting::gc_round_outcome_entry_budget].value)
    , gcs_max_conditional_put_bytes(settings_[ContentAddressedSetting::gcs_max_conditional_put_bytes].value)
    , cas_part_folder_cache_bytes(settings_[ContentAddressedSetting::part_folder_cache_bytes].value)
    , cas_part_folder_cache_max_entries(settings_[ContentAddressedSetting::part_folder_cache_max_entries].value)
    , cas_part_folder_cache_max_entry_bytes(settings_[ContentAddressedSetting::part_folder_cache_max_entry_bytes].value)
    , manifest_decode_cache_bytes(settings_[ContentAddressedSetting::manifest_decode_cache_bytes].value)
    , gc_meta_pool_size(settings_[ContentAddressedSetting::gc_meta_pool_size].value)
    , staging_backend(settings_.stagingBackend())
    , blob_hash_algo(settings_.blobHashAlgo())
    , blob_hash_allow_new(settings_[ContentAddressedSetting::blob_hash_allow_new].value)
    , skip_access_check(settings_[ContentAddressedSetting::skip_access_check].value)
    , part_folder_validate(settings_.partFolderValidate())
{
}

Cas::StagingBackend ContentAddressedMetadataStorage::parseStagingBackend(const std::string & value)
{
    if (value == "local")
        return Cas::StagingBackend::Local;
    if (value == "s3")
        return Cas::StagingBackend::S3;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown staging_backend value '{}' (expected 'local' or 's3')", value);
}

Cas::StagingBackend ContentAddressedMetadataStorage::parseStagingBackend(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return parseStagingBackend(config.getString(config_prefix + ".staging_backend", "local"));
}

Cas::PartFolderValidate ContentAddressedMetadataStorage::parsePartFolderValidate(const std::string & value)
{
    using PartFolderValidate = Cas::PartFolderValidate;
    if (value == "always")
        return {PartFolderValidate::Mode::Always, 0};
    if (value == "never")
        return {PartFolderValidate::Mode::Never, 0};
    if (value.starts_with("age "))
    {
        /// `std::from_chars` against an UNSIGNED type never accepts a leading '-' (unlike
        /// `std::stoull`, which silently negates modulo 2^64) -- a malformed/negative/non-digit/empty
        /// suffix falls through to the terminal throw below instead of wrapping into an astronomical
        /// age_seconds that behaves as skip-forever.
        const std::string age_str = value.substr(4);
        uint64_t age_seconds = 0;
        const auto [ptr, ec] = std::from_chars(age_str.data(), age_str.data() + age_str.size(), age_seconds);
        if (ec == std::errc{} && ptr == age_str.data() + age_str.size())
            return {PartFolderValidate::Mode::Age, age_seconds};
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown part_folder_validate value '{}' (expected 'always', 'never', or 'age <non-negative integer seconds>')", value);
}

Cas::PartFolderValidate ContentAddressedMetadataStorage::parsePartFolderValidate(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return parsePartFolderValidate(config.getString(config_prefix + ".part_folder_validate", "always"));
}

ContentAddressedMetadataStorage * ContentAddressedMetadataStorage::tryFromDisk(const DiskPtr & disk)
{
    /// The cheap predicate FIRST, never an exception probe: for every non-object-storage disk
    /// (DiskLocal & co.) `getMetadataStorage` throws NOT_IMPLEMENTED, and merely CONSTRUCTING that
    /// exception increments `system.errors` even when the throw is caught. This function runs on
    /// every asynchronous-metrics tick for every configured disk, so the old exception-as-control-
    /// flow probe polluted `system.errors` with a steady stream of NOT_IMPLEMENTED on pure-local
    /// servers — caught as a stray-error failure by strict-error tests (`test_cancel_backup`'s
    /// NoTrashChecker, Altinity PR#2073). `isContentAddressed` is a throw-free virtual (IDisk
    /// defaults to false; wrappers forward it — see ReadOnlyDiskWrapper).
    if (!disk || !disk->isContentAddressed())
        return nullptr;
    /// A content-addressed disk always implements getMetadataStorage (it IS the CA metadata
    /// storage), so no NOT_IMPLEMENTED handling is needed past the predicate.
    return dynamic_cast<ContentAddressedMetadataStorage *>(disk->getMetadataStorage().get());
}

void ContentAddressedMetadataStorage::runOneGcRoundForTest()
{
    /// Admin class (rev.7 spec §1): refuse on a transient / IdentityLost / Vanished pool (a terminal disk
    /// throws the typed Vanished [D5] refusal; an uncertain one throws 668) before touching the scheduler.
    /// The later `!cas_store` re-check under `gc_scheduler_mutex` still guards the not-mounted race.
    checkOpAdmitted(CasOpClass::Admin);
    /// The pacing scheduler must be STABLE across calls: the lease's observation-window steal
    /// protocol compares consecutive observations of the SAME observer (gc_id), so an ad-hoc
    /// scheduler per call would acquire the lease on the first call and then back off forever
    /// ("incumbent alive" - its own previous incarnation). Recreating the scheduler for every call
    /// would therefore make every round after the first a silent no-op.
    /// Hold gc_scheduler_mutex for the whole round: a concurrent `shutdown` waits for the round to
    /// finish because clean GC completion takes priority over fast shutdown. pointer_mutex (a
    /// separate, briefly-held mutex) only guards the scheduler snapshot/creation below, so
    /// gcHealth/store/partAccess never block behind this round.
    /// Test-only seam (inert in production): lets a test interleave a concurrent FORGET into the window
    /// between the pre-lock admission check above and the lock acquisition below -- the exact I-1 TOCTOU.
    if (gc_verb_admit_window_hook_for_test)
        gc_verb_admit_window_hook_for_test();
    std::lock_guard round_lock(gc_scheduler_mutex);
    /// Re-run the admission gate UNDER `gc_scheduler_mutex` (mirroring `gcStart`'s lock-then-gate): the
    /// pre-lock check above is only a fast-fail. A round admitted while `Live` can block on this mutex behind
    /// a concurrent FORGET (which holds it for its whole teardown); once FORGET settles the pool `Vanished`,
    /// this re-check throws the typed [D5] refusal instead of resurrecting a scheduler on a decommissioned
    /// pool. `checkOpAdmitted` takes only the brief `pointer_mutex` -- lock order gc_scheduler -> pointer.
    checkOpAdmitted(CasOpClass::Admin);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot run garbage collection after ContentAddressedMetadataStorage shutdown has begun");
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            /// `checkOpAdmitted(Admin)` above already fails loud on a null pool; this is the defensive
            /// re-check for a concurrent `shutdown` reset between the two `pointer_mutex` acquisitions.
            /// Surface the same `INVALID_STATE` refusal `poolAccess()` gives, never `LOGICAL_ERROR`
            /// (which would abort under debug/ASan builds).
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = std::make_shared<Cas::CasGcScheduler>(
                cas_store, gc_interval, fmt::format("{}::ContentAddressedGC", storage_path_full),
                disk_name, makeGcRoundLogger());
        }
        snapshot = gc_scheduler;
    }
    snapshot->runOneRoundNow();
}

void ContentAddressedMetadataStorage::requestGcRoundSoon()
{
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (snapshot)
        snapshot->requestRoundSoon();
}

std::optional<Cas::CasGcScheduler::GcHealth> ContentAddressedMetadataStorage::gcHealth() const
{
    /// A brief pointer_mutex snapshot only -- this must NEVER wait behind gc_scheduler_mutex (an
    /// in-flight round can hold that for a long time; an unprivileged SELECT on
    /// system.cas_mounts must not stall behind it). The snapshot keeps the scheduler
    /// alive via its own refcount even if `shutdown` concurrently resets the member, and
    /// CasGcScheduler::gcHealth() is itself lock-free (atomic reads), so calling it outside any lock
    /// here is safe.
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (!snapshot)
        return std::nullopt;
    return snapshot->gcHealth();
}

CasLifecycleSnapshot ContentAddressedMetadataStorage::lifecycleSnapshot() const
{
    /// Factory-class (spec §7): I/O-free and reachable in EVERY state. NEVER calls store()/poolAccess()
    /// (which refuse a not-mounted disk) or touches the backend -- that is the whole point, so the disk
    /// that vanished is still visible. Only a brief pointer_mutex snapshot of the pool pointer, plus reads
    /// of storage members that are immutable after startup.
    CasLifecycleSnapshot snap;
    snap.server_root_id = server_root_id;
    /// The last-known pool identity. `pool_uuid` is written once (single-threaded) at the end of `startup`
    /// and never reset by `shutdown`, so it is empty ONLY before the first successful startup and stable
    /// thereafter -- the disk stays introspectable under its identity even once the pool is torn down.
    snap.pool_id = pool_uuid;

    Cas::PoolPtr pool;
    {
        std::lock_guard lock(pointer_mutex);
        pool = cas_store;
    }
    if (!pool)
    {
        /// No pool published: the storage-level lifecycle (spec §1's Constructing/ShutDown). Distinguish
        /// the two by whether startup ever ran, which `pool_uuid` records (empty => never started). A
        /// disk that was started then torn down (shutdown) reports `shutdown`. reason/detail/since stay
        /// empty/0 -- no terminal cause.
        snap.lifecycle = snap.pool_id.empty() ? "constructing" : "shutdown";
        return snap;
    }

    const Cas::Pool::LifecycleSnapshot ps = pool->lifecycleSnapshot();
    snap.lifecycle = casLifecycleToString(ps.lifecycle);
    snap.reason = casLifecycleReasonWord(ps.lifecycle);
    snap.detail = ps.detail;
    snap.since = ps.since;
    return snap;
}

Cas::GcRoundLogger ContentAddressedMetadataStorage::makeGcRoundLogger() const
{
    /// Unit tests pass a null context (no system logs); the scheduler then runs without a sink.
    if (!context)
        return {};
    auto ctx = context;
    /// The configured disk name (threaded from the metadata-storage factory); falls back to
    /// storage_path_prefix for callers that don't supply one (e.g. unit tests).
    const String disk = disk_name;
    return [ctx, disk](const Cas::GcRoundLogRecord & r)
    {
        auto log = ctx->getContentAddressedGarbageCollectionLog();
        if (!log)
            return;
        ContentAddressedGarbageCollectionLogElement e;
        const auto now = std::chrono::system_clock::now();
        e.event_time = std::chrono::system_clock::to_time_t(now);
        e.event_time_microseconds = timeInMicroseconds(now);
        switch (r.event_type)
        {
            case Cas::GcRoundLogRecord::EventType::Start:
                e.event_type = ContentAddressedGarbageCollectionLogElement::START;
                break;
            case Cas::GcRoundLogRecord::EventType::Finish:
                e.event_type = ContentAddressedGarbageCollectionLogElement::FINISH;
                break;
            case Cas::GcRoundLogRecord::EventType::Phase:
                e.event_type = ContentAddressedGarbageCollectionLogElement::PHASE;
                break;
        }
        e.disk_name = r.disk_name.empty() ? disk : r.disk_name;
        e.srid = r.srid;
        e.gc_id = r.gc_id;
        e.trigger = r.trigger == Cas::GcRoundLogRecord::Trigger::Manual
            ? ContentAddressedGarbageCollectionLogElement::MANUAL
            : ContentAddressedGarbageCollectionLogElement::SCHEDULED;
        switch (r.outcome)
        {
            case Cas::GcRoundLogRecord::Outcome::Unknown:
                e.outcome = ContentAddressedGarbageCollectionLogElement::UNKNOWN;
                break;
            case Cas::GcRoundLogRecord::Outcome::Success:
                e.outcome = ContentAddressedGarbageCollectionLogElement::SUCCESS;
                break;
            case Cas::GcRoundLogRecord::Outcome::NotALeader:
                e.outcome = ContentAddressedGarbageCollectionLogElement::NOT_A_LEADER;
                break;
            case Cas::GcRoundLogRecord::Outcome::Failed:
                e.outcome = ContentAddressedGarbageCollectionLogElement::FAILED;
                break;
            case Cas::GcRoundLogRecord::Outcome::Deferred:
                e.outcome = ContentAddressedGarbageCollectionLogElement::DEFERRED;
                break;
        }
        e.round = r.round;
        e.candidates_marked = r.candidates_marked;
        e.objects_deleted = r.objects_deleted;
        e.objects_absent = r.objects_absent;
        e.objects_replaced = r.objects_replaced;
        e.objects_spared = r.objects_spared;
        e.manifests_deleted = r.manifests_deleted;
        e.entries_condemned = r.entries_condemned;
        e.entries_graduated = r.entries_graduated;
        e.entries_redeleted = r.entries_redeleted;
        e.fence_outs = r.fence_outs;
        e.anomalies = r.anomalies;
        e.duration_ms = r.duration_ms;
        e.error = r.error;
        e.profile_events = r.profile_events;
        e.round_id = r.round_id;
        e.phase = r.phase;
        e.phase_duration_microseconds = r.phase_duration_microseconds;
        e.phase_metrics = r.phase_metrics;
        /// Best-effort: SystemLog::add never blocks GC; a full queue drops the row with a warning.
        log->add(std::move(e));
    };
}

Cas::CasEventSink ContentAddressedMetadataStorage::makeCasEventSink() const
{
    /// Unit tests pass a null context (no system logs); the Pool then runs without a sink.
    if (!context)
        return {};
    auto ctx = context;
    /// The configured disk name (threaded from the metadata-storage factory); falls back to
    /// storage_path_prefix for callers that don't supply one (e.g. unit tests).
    const String disk = disk_name;
    return [ctx, disk](Cas::CasEvent ev)
    {
        auto log = ctx->getContentAddressedLog();
        if (!log)
            return;
        ContentAddressedLogElement e;
        const auto now = std::chrono::system_clock::now();
        e.event_time = std::chrono::system_clock::to_time_t(now);
        e.event_time_microseconds = timeInMicroseconds(now);
        e.event_type = toString(ev.type);
        e.disk_name = disk;
        e.namespace_ = std::move(ev.namespace_);
        e.ref_name = std::move(ev.ref_name);
        e.object_kind = toString(ev.object_kind);
        e.object_hash = std::move(ev.object_hash);
        e.token = std::move(ev.token);
        e.round = ev.round;
        e.gen = ev.gen;
        e.at_version = ev.at_version;
        e.outcome = std::move(ev.outcome);
        e.reason = std::move(ev.reason);
        e.thread_id = getThreadId();
        e.query_id = CurrentThread::getQueryId();
        e.detail = std::move(ev.detail);
        /// Best-effort: SystemLog::add never blocks the Core; a full queue drops the row with a warning.
        log->add(std::move(e));
    };
}

Cas::RoundReport ContentAddressedMetadataStorage::runGarbageCollectionRoundNow()
{
    checkNotReadOnly("GC round");
    if (!gc_enabled)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Garbage collection is not enabled on this content-addressed disk");
    /// Admin class (rev.7 spec §1): refuse on a transient / IdentityLost / Vanished pool before touching
    /// the scheduler -- `SYSTEM CAS GC RUN` reaches here directly.
    checkOpAdmitted(CasOpClass::Admin);
    /// Mirror runOneGcRoundForTest: a STABLE scheduler instance across calls (the lease's
    /// observation-window steal protocol compares consecutive observations of the same gc_id).
    /// Hold gc_scheduler_mutex for the whole round: a concurrent `shutdown` waits for the round to
    /// finish because clean GC completion takes priority over fast shutdown. pointer_mutex only
    /// guards the scheduler snapshot/creation below, so gcHealth/store/partAccess never block behind
    /// this round.
    /// Test-only seam (inert in production): lets a test interleave a concurrent FORGET into the window
    /// between the pre-lock admission check above and the lock acquisition below -- the exact I-1 TOCTOU.
    if (gc_verb_admit_window_hook_for_test)
        gc_verb_admit_window_hook_for_test();
    std::lock_guard round_lock(gc_scheduler_mutex);
    /// Re-run the admission gate UNDER `gc_scheduler_mutex` (mirroring `gcStart`'s lock-then-gate): the
    /// pre-lock check above is only a fast-fail. A round admitted while `Live` can block on this mutex behind
    /// a concurrent FORGET (which holds it for its whole teardown); once FORGET settles the pool `Vanished`,
    /// this re-check throws the typed [D5] refusal instead of resurrecting a scheduler on a decommissioned
    /// pool. `checkOpAdmitted` takes only the brief `pointer_mutex` -- lock order gc_scheduler -> pointer.
    checkOpAdmitted(CasOpClass::Admin);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot run garbage collection after ContentAddressedMetadataStorage shutdown has begun");
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            /// Same reasoning as `runOneGcRoundForTest` above: `checkOpAdmitted(Admin)` already failed
            /// loud on a null pool, so this is the defensive re-check for a concurrent `shutdown` reset
            /// -- a normal `INVALID_STATE` refusal, never a `LOGICAL_ERROR` abort.
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = std::make_shared<Cas::CasGcScheduler>(
                cas_store, gc_interval, fmt::format("{}::ContentAddressedGC", storage_path_full),
                disk_name, makeGcRoundLogger());
        }
        snapshot = gc_scheduler;
    }
    return snapshot->runOneRoundNow(Cas::GcRoundLogRecord::Trigger::Manual);
}

Cas::RebuildReport ContentAddressedMetadataStorage::runGcRebuildNow(bool force) const
{
    checkNotReadOnly("GC rebuild");
    if (!gc_enabled)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Garbage collection is not enabled on this content-addressed disk");
    /// Admin class (rev.7 spec §1): refuse on a transient / IdentityLost / Vanished pool. (`store()` below
    /// already throws on a terminal pool, but not on the merely-transient one -- this closes that gap and
    /// keeps the refusal uniform with the other GC entry points.) Pre-lock: only a fast-fail.
    checkOpAdmitted(CasOpClass::Admin);
    /// Serialize the whole rebuild under `gc_scheduler_mutex`, exactly as a synchronous round and FORGET's
    /// teardown are (both hold this same mutex). Held for the rebuild's DURATION so a concurrent FORGET waits
    /// it out (fail-closed) instead of reporting the disk decommissioned while the rebuild's one-shot
    /// `Cas::Gc` is still issuing durable `gc/`-plane writes. `Gc::rebuildBaseline` holds only the
    /// Pool/backend (no back-reference to this storage), so it never re-takes this mutex -- no deadlock; and
    /// `store()` takes only the brief `pointer_mutex` (lock order gc_scheduler -> pointer), so
    /// gcHealth/store/partAccess never block behind this rebuild.
    std::lock_guard round_lock(gc_scheduler_mutex);
    /// Re-run the admission gate under the lock (mirroring the round verbs): a rebuild admitted while `Live`
    /// but blocked here behind a FORGET refuses once the pool is `Vanished` (the later `store()` is also
    /// fail-closed, so this is a fast-fail before minting the GC identity).
    checkOpAdmitted(CasOpClass::Admin);
    /// Test-only seam (inert in production): fires WHILE this rebuild holds `gc_scheduler_mutex` -- the
    /// in-flight window a concurrent FORGET must serialize behind (I-2). Lets a test hold the lock here and
    /// observe that FORGET blocks until the rebuild releases it.
    if (gc_verb_admit_window_hook_for_test)
        gc_verb_admit_window_hook_for_test();
    /// A one-shot Gc instance is fine here (unlike the scheduler's stable-instance requirement for
    /// the lease's observation-window steal protocol): rebuildBaseline does its own lease
    /// acquire/steal check internally and this command runs exactly one round.
    const UInt128 gc_id = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
    const auto cas_store_snapshot = store();
    Cas::Gc gc(cas_store_snapshot, gc_id, {}, {},
        getLogger(fmt::format("CasGc({})", cas_store_snapshot->poolConfig().server_root_id)));
    return gc.rebuildBaseline(force);
}

ContentAddressedMetadataStorage::PoolView ContentAddressedMetadataStorage::openPoolView() const
{
    /// Native mode rides real conditional ops (probed fail-closed by Pool::open); Local object
    /// storage has none, so the backend emulates exact token semantics in-process (single server).
    const auto mode = object_storage->getType() == ObjectStorageType::Local
        ? Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess
        : Cas::ObjectStorageBackend::Mode::Native;
    auto backend = std::make_shared<Cas::ObjectStorageBackend>(object_storage, mode, gcs_max_conditional_put_bytes);
    const Cas::TokenType backend_token_type = backend->nativeTokenType();

    /// EmulatedSingleProcess emulates the conditional-op / exact-token semantics in-process (local
    /// object storage has none). That emulation is per-process: two servers pointed at the SAME local
    /// pool (e.g. an NFS/shared mount) each keep independent token state and would silently violate
    /// the CAS invariants — the capability probe cannot detect this (each process passes it alone).
    /// Make a shared-pool misconfiguration visible at INFO, not WARNING.
    /// An inline `disk = disk(... object_storage_type=local ...)` opens the disk on the QUERY thread, so
    /// a WARNING is forwarded to the client at the functional-test default `send_logs_level=warning` and
    /// fails EVERY such query (clickhouse-test fails a test on ANY client stderr). At INFO the message
    /// still lands in the server log for operator visibility but is not forwarded to client queries, so
    /// the ~15 CA-over-local stateless tests stop failing on a benign single-server note. (A genuinely
    /// shared local pool is a niche risk that would also surface via CAS/GC corruption; a future
    /// `system.warnings` entry could restore a louder, test-safe signal.)
    if (mode == Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess)
        LOG_INFO(
            getLogger("ContentAddressedMetadataStorage"),
            "Content-addressed disk over LOCAL object storage uses emulated in-process conditional "
            "operations — safe ONLY for a single server. Do NOT share this pool path between multiple "
            "ClickHouse servers (e.g. a shared/NFS mount): the CAS/GC invariants would break silently. "
            "Use an S3-backed pool for multi-server / shared deployments.");

    /// Key spaces per mode: the Emulated (Local) backend maps bare pool keys under
    /// getCommonKeyPrefix (the disk root dir), so the POOL prefix must be bucket-relative - strip
    /// the common prefix when the configured prefix carries it (the local factory passes the root
    /// path). Native passes keys through, so the configured prefix is used as-is (for S3 it
    /// already embeds the endpoint sub-path).
    String pool_prefix = storage_path_prefix;
    /// The configured prefix is an endpoint sub-path and usually carries a TRAILING slash
    /// ("content_addressed_s3/"); Cas::Layout joins components with '/', and a doubled slash in
    /// keys is backend-hostile (RustFS rejects "p//_probe" LIST prefixes with InvalidArgument -
    /// Some backends reject such prefixes while others merely tolerate them).
    while (!pool_prefix.empty() && pool_prefix.back() == '/')
        pool_prefix.pop_back();
    String physical_key_prefix_local;
    if (mode == Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess)
    {
        physical_key_prefix_local = object_storage->getCommonKeyPrefix();
        /// Slash-tolerant strip: the common prefix usually ends with '/', the pool prefix was
        /// just trimmed of trailing slashes - compare canonical forms.
        String common_trimmed = physical_key_prefix_local;
        while (!common_trimmed.empty() && common_trimmed.back() == '/')
            common_trimmed.pop_back();
        if (!common_trimmed.empty())
        {
            if (pool_prefix == common_trimmed)
                pool_prefix.clear();
            else if (pool_prefix.starts_with(common_trimmed + "/"))
                pool_prefix = pool_prefix.substr(common_trimmed.size() + 1);
        }
        if (pool_prefix.empty())
            pool_prefix = "ca";
    }

    Cas::PoolConfig pool_config;
    pool_config.pool_prefix = pool_prefix;
    pool_config.server_id = serverIdToU128(server_id);
    pool_config.server_root_id = server_root_id;
    /// A read-only (`<readonly>`) disk opens with no background watermark and no write probe.
    pool_config.background_watermark = (context != nullptr) && !read_only;
    pool_config.read_only = read_only;
    pool_config.skip_access_check = skip_access_check;
    /// The node-local write algorithm: `PoolMeta::createOrValidate` accepts it with no write once
    /// it is a member of the pool's `algos_used`; a not-yet-admitted algo is admitted via
    /// `blob_hash_allow_new` or refused (BAD_ARGUMENTS, the default).
    pool_config.blob_hash_algo = blob_hash_algo;
    pool_config.blob_hash_allow_new = blob_hash_allow_new;
    pool_config.manifest_decode_cache_bytes = manifest_decode_cache_bytes;
    pool_config.gc_snapshot_generations_to_keep = gc_snapshot_generations_to_keep;
    pool_config.gc_shards = gc_shards;
    pool_config.manifest_sweep_list_budget_keys = manifest_sweep_list_budget_keys;
    pool_config.manifest_sweep_delete_budget_keys = manifest_sweep_delete_budget_keys;
    pool_config.gc_round_graduation_budget = gc_round_graduation_budget;
    pool_config.gc_round_redelete_budget = gc_round_redelete_budget;
    pool_config.gc_round_sweep_namespace_budget = gc_round_sweep_namespace_budget;
    pool_config.gc_round_sweep_recovery_op_budget = gc_round_sweep_recovery_op_budget;
    pool_config.gc_round_ref_cleanup_budget = gc_round_ref_cleanup_budget;
    pool_config.gc_round_prefix_wholesale_budget = gc_round_prefix_wholesale_budget;
    pool_config.gc_round_handoff_prefix_wholesale_budget = gc_round_handoff_prefix_wholesale_budget;
    pool_config.gc_round_outcome_entry_budget = gc_round_outcome_entry_budget;
    pool_config.gc_meta_pool_size = gc_meta_pool_size;
    pool_config.event_sink = makeCasEventSink();

    PoolView view;
    view.physical_key_prefix = physical_key_prefix_local;
    view.pool_prefix = pool_prefix;
    view.native_token_type = backend_token_type;
    view.pool = Cas::Pool::open(std::move(backend), std::move(pool_config));
    return view;
}

void ContentAddressedMetadataStorage::startup()
{
    if (cas_store)
        return;

    /// Observe-only mode (the disk's <readonly> config): skip the probe (a probe write would fail on
    /// a read-only backend), run no watermark, start no GC, and fail the mutating surface closed.
    read_only = object_storage->isReadOnly();

    /// Explicit S3 staging can publish only by provider-native same-store copy. Validate the actual
    /// object-storage configuration before opening a writable mount; never infer capability from an
    /// endpoint/provider name and never substitute local staging for an unsupported explicit choice.
    /// Observe-only mounts cannot enter staged publication, so they do not require this capability.
    if (staging_backend == Cas::StagingBackend::S3
        && !read_only
        && !object_storage->supportsCopyMode(ObjectStorageCopyMode::NativeOnly))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "staging_backend=s3 requires native-only same-store copy, but object storage {} does not support it",
            object_storage->getName());

    /// Everything below builds into LOCALS -- nothing is published to `cas_store`/`part_access`/
    /// `gc_scheduler`/`pool_uuid` until the single publish step at the
    /// very end. This makes a mid-startup throw leave the object exactly as unstarted as it was on
    /// entry (the `if (cas_store) return;` head above still sees an empty pool), so a caller can
    /// retry `startup` after a transient failure instead of being stuck with a half-built mount.
    PoolView view = openPoolView();
    physical_key_prefix = view.physical_key_prefix;
    auto pool = std::move(view.pool);
    auto uuid = Cas::u128ToHex(pool->poolMeta().pool_id);
    auto facade = std::make_shared<Cas::CachedPartFolderAccess>(pool,
        Cas::CachedPartFolderAccess::CacheParams{
            .cache_bytes = cas_part_folder_cache_bytes,
            .max_entries = cas_part_folder_cache_max_entries,
            .max_entry_bytes = cas_part_folder_cache_max_entry_bytes,
            .validate = part_folder_validate});

    /// Reclaim this mount's leaked `staging/<server_root_id>/` debris after an explicit, writable S3
    /// staging mount has passed the native-copy check above. The prefix is keyed by this mount's own
    /// `server_root_id`, matching every staging key the writer mints. GC excludes `staging/` entirely,
    /// so this mount-scoped sweep is its only reclaimer. Read-only and default-local mounts do not
    /// write into this prefix and skip the sweep.
    if (staging_backend == Cas::StagingBackend::S3 && !read_only)
    {
        Cas::sweepOwnMountStaging(
            *object_storage,
            physicalKey(view.pool_prefix + "/staging/" + server_root_id) + "/");
    }

    /// The background GC scheduler runs only on the disk-factory path (context non-null) and when
    /// enabled - the lease makes concurrent schedulers across mounters safe (work dedup), so no
    /// further gating is needed because the scheduler's lease coordinates concurrent mounters.
    /// `CasGcScheduler` holds its own `PoolPtr` (see its `store` member), so starting it against the
    /// LOCAL `pool` before publish is safe -- the scheduler keeps the pool alive on its own. It is
    /// also safe on the unwind path below: `scheduler` here is a local `shared_ptr`, so if something
    /// after this point throws (only the fault-injection hook can, in production nothing does),
    /// its destructor runs during stack unwinding, which drops the last reference and destroys the
    /// `CasGcScheduler`; its destructor calls `stop()`, which joins both worker threads before the
    /// exception continues propagating. No explicit `SCOPE_EXIT` is needed for that.
    std::shared_ptr<Cas::CasGcScheduler> scheduler;
    if (context && gc_enabled && !read_only)
    {
        scheduler = std::make_shared<Cas::CasGcScheduler>(
            pool, gc_interval, fmt::format("{}::ContentAddressedGC", storage_path_full),
            disk_name, makeGcRoundLogger());
        scheduler->start();
    }

    /// Test-only: lets a test prove that a failure here (after everything above has succeeded, but
    /// before publish) leaves nothing published and a retry can still succeed. A no-op in production.
    if (startup_fault_injection_for_test)
        startup_fault_injection_for_test();

    /// The single publish step: as the LAST action of `startup`, atomically hand the fully-built
    /// pool, part-folder facade, and GC scheduler to the members other threads observe through
    /// `store`/`partAccess`/the GC entry points, in ONE `pointer_mutex` acquisition -- so no caller of
    /// `poolAccess()` can ever observe a half-published mount. Everything above only ever touched locals,
    /// so any throw before this point (including from the fault-injection hook above) leaves those
    /// members (a null `cas_store`) exactly as they were on entry.
    {
        std::lock_guard lock(pointer_mutex);
        cas_store = std::move(pool);
        part_access = std::move(facade);
        gc_scheduler = std::move(scheduler);
    }
    pool_uuid = std::move(uuid);
    native_token_type = view.native_token_type;

    /// Freeze the object storage's conditional-ops dialect for the rest of its life. Everything above
    /// has now derived persistent state from it -- token normalisation, whether a listing may supply a
    /// token at all, and the preconditions a generation store had to satisfy to mount -- and a reload
    /// that swapped the client under that state would leave persisted tokens uncomparable. The refusal
    /// has to happen in the object storage: only there is the effective `http_client` known, merged from
    /// the storage's current settings, any endpoint-level block and the disk's own section.
    object_storage->pinConditionalOpsGenerationDialect(native_token_type == Cas::TokenType::Generation);
}

void ContentAddressedMetadataStorage::shutdown()
{
    /// Wait for any in-flight synchronous round to finish cleanly first (gc_scheduler_mutex is held
    /// for a round's whole duration) -- unchanged priority: clean GC completion over fast shutdown.
    std::lock_guard round_lock(gc_scheduler_mutex);
    shutdown_called = true;
    std::shared_ptr<Cas::CasGcScheduler> old_scheduler;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        old_scheduler = std::move(gc_scheduler);
        gc_scheduler.reset();
        part_access.reset();
        /// Terminal server-shutdown semantics: a one-way trip (no server-lifecycle "remount" after
        /// shutdown). Nulling `cas_store` puts the storage back into the null-pool (ShutDown) lifecycle,
        /// so `poolAccess()`/the gate report the same operational refusal post-shutdown as pre-startup.
        cas_store.reset();
    }
    /// `stop` joins the background threads. Runs outside pointer_mutex (no reset left to race:
    /// gc_scheduler is already null) but still inside round_lock, so a NEW round can't start here.
    /// old_scheduler keeps the object alive regardless.
    if (old_scheduler)
        old_scheduler->stop();
}

namespace
{
/// A human-readable UTC decommission stamp for the FORGET [D5] message (an operator asserted this, so the
/// message must be traceable to the audit log by wall time). Format: "YYYY-MM-DD HH:MM:SS UTC".
String utcStampNow()
{
    const std::time_t now = std::time(nullptr);
    std::tm tm_utc{};
    gmtime_r(&now, &tm_utc);
    char buf[32];
    const size_t n = std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S UTC", &tm_utc);
    return String(buf, n);
}
}

void ContentAddressedMetadataStorage::forgetDisk()
{
    /// SYSTEM CAS FORGET (spec §5): the operator force-Vanish. A lifecycle verb, NOT a
    /// store()-class op — it must work on a NOT-live disk (a stuck transient / IdentityLost pool), so it
    /// reaches the pool DIRECTLY, never through `poolAccess()`/`checkOpAdmitted` (which refuse a not-live
    /// disk). Serialized against FSCK / GC STOP / GC START by `lifecycle_mutex`, and against a concurrent
    /// synchronous GC round by `gc_scheduler_mutex` (a round holds the latter, so this waits it out).
    std::lock_guard lifecycle(lifecycle_mutex);
    std::lock_guard round_lock(gc_scheduler_mutex);

    Cas::PoolPtr pool;
    std::shared_ptr<Cas::CasGcScheduler> scheduler;
    {
        std::lock_guard lock(pointer_mutex);
        pool = cas_store;
        /// Detach the scheduler from the member under `pointer_mutex` (as shutdown/unmount do): no new
        /// synchronous round can adopt it, and `gcHealth` reports "no GC" for a forgotten disk immediately.
        /// The actual stop()+join runs below, inside the pool's protocol
        /// (OUTSIDE `pointer_mutex`, since it joins threads).
        scheduler = std::move(gc_scheduler);
    }

    if (!pool)
    {
        /// No published pool to forget (never started / shut down). The disk is already not serving; a
        /// restart re-registers the name. Idempotent no-op. (The detached `scheduler` is null here too —
        /// `cas_store`/`gc_scheduler` are published and cleared together.)
        LOG_WARNING(getLogger("ContentAddressedMetadataStorage"),
            "SYSTEM CAS FORGET on content-addressed disk '{}': no published pool — nothing "
            "to decommission (a restart re-registers the name).", disk_name);
        return;
    }

    /// The [D5] forgotten message, carrying the actual decommission timestamp (an operator ASSERTION, not
    /// an erasure proof — the wording says so). `Pool::throwIfLifecycleTerminal` surfaces it verbatim to
    /// every store-class caller after the transition, and the WARN at the transition logs it too.
    const String reason = fmt::format(
        "decommissioned by SYSTEM CAS FORGET at {} — erasure was NOT verified; if this was a "
        "mistake the data may be intact (restart re-registers the name)", utcStampNow());

    /// Run the fence-first protocol on the pool. The GC-stop callback stops+joins the (detached) scheduler
    /// at spec §5 step 3/4; the scheduler is destroyed when the local `scheduler` leaves this scope.
    pool->forgetDisk([&scheduler] { if (scheduler) scheduler->stop(); }, reason);
}

void ContentAddressedMetadataStorage::gcStop()
{
    /// SYSTEM CAS GC STOP (spec §6): stop ONLY the background GC scheduler. STOP-IN-PLACE --
    /// the scheduler object is RETAINED in the member (contrast `forgetDisk`/`shutdown`, which `std::move`
    /// it out and destroy it): a later `gcStart` must re-enter the SAME instance so its `gc_id` + lease
    /// observation history survive. Keeping it in the member also keeps `gcHealth` reading the (stopped)
    /// state truthfully, rather than "no GC".
    /// A lifecycle-control verb: serialized against FSCK / forget / GC START by `lifecycle_mutex`, and
    /// against a concurrent synchronous GC round by `gc_scheduler_mutex` (a round holds the latter, so this
    /// waits it out). It does NOT consult `checkOpAdmitted` -- stopping GC works on ANY disk state, including
    /// a not-live/Vanished one (stopping the reclaimer on a sick disk is a legitimate operator action).
    std::lock_guard lifecycle(lifecycle_mutex);
    std::lock_guard round_lock(gc_scheduler_mutex);

    /// Snapshot the scheduler under `pointer_mutex` by COPY (never `std::move`): leave it in the member.
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (!snapshot)
    {
        /// No scheduler at all (GC disabled / read-only / not started / already forgotten). Stopping GC on a
        /// disk that runs none is a no-op success -- the operator's intent ("no GC background activity")
        /// already holds.
        LOG_INFO(getLogger("ContentAddressedMetadataStorage"),
            "SYSTEM CAS GC STOP on content-addressed disk '{}': no GC scheduler "
            "(disabled/read-only/not started) -- nothing to stop.", disk_name);
        return;
    }
    /// `stop()` joins the worker + heartbeat threads and clears the in-process leadership hint. Runs OUTSIDE
    /// `pointer_mutex` (it joins threads). Idempotent: a second STOP finds an already-stopped scheduler and
    /// `stop()` is a safe no-op.
    snapshot->stop();
}

void ContentAddressedMetadataStorage::gcStart()
{
    /// SYSTEM CAS GC START (spec §6): restart the background GC scheduler stopped by `gcStop`.
    /// Serialized like `gcStop`. Unlike it, START refuses on a decommissioned/uncertain pool: restarting GC
    /// there would only spin failing rounds, so it goes through the uniform GC gate.
    std::lock_guard lifecycle(lifecycle_mutex);
    std::lock_guard round_lock(gc_scheduler_mutex);

    checkNotReadOnly("GC start");
    if (!gc_enabled)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Garbage collection is not enabled on this content-addressed disk");
    /// Admin class (rev.7 spec §1): refuse on a transient / `IdentityLost` / `Vanished` pool (typed 668 /
    /// [D5]) and on a null pool (`throwStorageNotStarted`). Only a `Live` pool proceeds -- the same uniform
    /// gate every GC entry point uses (`runGarbageCollectionRoundNow` / `runGcRebuildNow`).
    checkOpAdmitted(CasOpClass::Admin);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot start garbage collection after ContentAddressedMetadataStorage shutdown has begun");

    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            /// `Live` + `gc_enabled` + not read-only but no scheduler: reachable only when the disk was
            /// started in a context that started none (e.g. a unit-test null context) or after a GC RUN that
            /// created a lazy one was never started. Create a STABLE instance now, mirroring the GC RUN entry
            /// points, so START is meaningful. (`checkOpAdmitted` above already proved `cas_store` is live.)
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = std::make_shared<Cas::CasGcScheduler>(
                cas_store, gc_interval, fmt::format("{}::ContentAddressedGC", storage_path_full),
                disk_name, makeGcRoundLogger());
        }
        snapshot = gc_scheduler;
    }
    /// `start()` is a no-op if already running (idempotent) and re-enters the SAME instance after a stop --
    /// the persistent `gc` observer + `gc_id` are preserved, and leadership is re-acquired only by the next
    /// round's normal `gc/state` acquisition, never restored here. Runs outside `pointer_mutex` for symmetry
    /// with `stop()` (it spawns threads but joins nothing, so it does not block).
    snapshot->start();
}

Cas::FsckReport ContentAddressedMetadataStorage::runFsckNow(bool detail) const
{
    /// Outermost lock (see its own doc comment): held for the WHOLE scan, so a concurrent lifecycle-control
    /// verb (FORGET / GC STOP / GC START) cannot race the disk out from under an in-flight FSCK.
    std::lock_guard lifecycle(lifecycle_mutex);

    /// FSCK scans the LIVE running pool directly (rev.8). Admin class -- refuse on a transient /
    /// IdentityLost / Vanished / null pool before touching it, exactly like the GC entry points
    /// (`runGarbageCollectionRoundNow`/`runGcRebuildNow`). The scan is read-only and its findings are
    /// revalidated against a fresh authoritative read (`CasFsck`'s Dangling / missing-manifest rechecks),
    /// so concurrent writers never yield a phantom finding.
    checkOpAdmitted(CasOpClass::Admin);
    return Cas::runFsck(*store(), detail);
}

ContentAddressedMetadataStorage::PoolAccessSnapshot ContentAddressedMetadataStorage::poolAccess() const
{
    PoolAccessSnapshot snap;
    {
        std::lock_guard lock(pointer_mutex);
        snap.pool = cas_store;
        snap.part_access = part_access;
    }
    /// A null pool covers before-first-startup and after-shutdown uniformly -- the storage-level
    /// Constructing/ShutDown lifecycle. Fail loud (spec §1's null-pool fail-loud); there is no benign
    /// answer for a storage that has not published a pool.
    if (!snap.pool)
        throwStorageNotStarted();
    /// rev.7 §1: a published pool that has entered a terminal lifecycle condition (`IdentityLost` or any
    /// `Vanished`) must ALSO refuse store()-class access, so nothing silently proceeds against an erased
    /// or replaced data root. This is the store()-class terminal check; the full six-class operation gate
    /// (which also gates the transient state and answers truth-absent on removes/enumeration) is
    /// `checkOpAdmitted`.
    snap.pool->throwIfLifecycleTerminal();
    return snap;
}

void ContentAddressedMetadataStorage::throwStorageNotStarted() const
{
    /// No pool is published: the storage-level lifecycle is Constructing (before `startup`) or ShutDown
    /// (after `shutdown`). `pool_uuid` is empty ONLY before the first successful startup (written once at
    /// its end, never reset by `shutdown`), so it distinguishes the two, exactly as `lifecycleSnapshot()`
    /// reports `constructing`/`shutdown`. Immutable-after-startup, so read without `pointer_mutex`.
    const char * phase = pool_uuid.empty() ? "constructing" : "shutdown";
    throw Exception(ErrorCodes::INVALID_STATE,
        "content-addressed disk '{}' is not started (storage lifecycle: {})", disk_name, phase);
}

Cas::PoolPtr ContentAddressedMetadataStorage::store() const
{
    return poolAccess().pool;
}

std::shared_ptr<Cas::CachedPartFolderAccess> ContentAddressedMetadataStorage::partAccess() const
{
    return poolAccess().part_access;
}

void ContentAddressedMetadataStorage::checkNotReadOnly(std::string_view what) const
{
    if (read_only)
        throw Exception(ErrorCodes::READONLY,
            "Content-addressed disk is opened read-only: {} is rejected", what);
}

CasOpAdmission ContentAddressedMetadataStorage::checkOpAdmitted(CasOpClass op) const
{
    /// `Factory` is never routed here (its call sites are I/O-free and work in every state); a Factory
    /// arg is a call-site bug. This unreachable path is genuinely unreachable, so the LOGICAL_ERROR never
    /// constructs (never aborts a debug/ASan build).
    if (op == CasOpClass::Factory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "checkOpAdmitted must not be called for the Factory class");

    Cas::PoolPtr pool;
    {
        std::lock_guard lock(pointer_mutex);
        pool = cas_store;
    }

    /// A null pool = the storage-level lifecycle is Constructing (before `startup`) or ShutDown (after
    /// `shutdown`): fail loud for EVERY class, `Probe` included. There is no benign "absent" answer for a
    /// storage that has never published a pool (or torn one down) -- only a genuinely `Vanished` POOL
    /// (below) answers truth-absent. This is the spec §1 null-pool fail-loud contract.
    if (!pool)
        throwStorageNotStarted();

    /// The rev.7 six-class gate keyed on the pool lifecycle condition (spec §1).
    const Cas::PoolLifecycle lc = pool->lifecycle();
    if (lc == Cas::PoolLifecycle::Live)
        return CasOpAdmission::Proceed;

    if (lc == Cas::PoolLifecycle::TransientNotLive)
        /// A lease blip: uncertain but AUTO-RECOVERING. No class but Factory proceeds, and the refusal is
        /// minted TRANSIENT -- this is the READ plane, where a consumer that cannot tell unavailability
        /// from damage acts destructively: `ReplicatedMergeTreePartCheckThread` detaches a part whose read
        /// throws an error its retryable-classifier does not list. `IdentityLost` gets its own richer,
        /// TERMINAL 668 below -- it does not auto-recover, so both "temporarily unreachable" and the
        /// retryable class would misdiagnose it. The wait-and-retry guidance is actionable for every caller
        /// here, and specifically for `SYSTEM CAS GC START` run mid-recovery by an operator
        /// who STOPped GC pre-maintenance: this is a wait, not a dead end.
        Cas::throwCasTransientUnavailable(
            fmt::format("content-addressed disk '{}'", disk_name),
            "mount lease not held; backing may be temporarily unreachable; the operation is admitted "
            "again once the disk recovers to Live");

    /// `IdentityLost` and the terminal `Vanished*` states carry the typed per-reason [D5] message the pool
    /// owns (single source). A SETTLED `Vanished` pool answers `Probe`/`Remove` truthfully without touching
    /// it; `IdentityLost` (sentinels absent, no auto-recovery) has NO benign answer -- every class
    /// fails loud with the "recover by restart or FORGET; a matching-sentinel restore does not auto-revive"
    /// diagnosis. So the truth-absent short-circuit is gated on the Vanished states specifically.
    const bool settled_vanished = lc == Cas::PoolLifecycle::VanishedReplaced
        || lc == Cas::PoolLifecycle::VanishedForgotten;
    if (settled_vanished && (op == CasOpClass::Probe || op == CasOpClass::Remove))
        return CasOpAdmission::TruthAbsent;
    pool->throwIfLifecycleTerminal();
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "checkOpAdmitted: unreachable -- non-Live pool did not throw for content-addressed disk '{}'", disk_name);
}

void ContentAddressedMetadataStorage::confirmPoolIdentityForEmptyEnumeration(const std::string & path) const
{
    /// EMPTY-PROOF RULE (rev.7 spec §1 [B3]). Reached ONLY when `listDirectory` computed an EMPTY listing
    /// at a `TableDir`/`DetachedContainer` root on a NON-terminal pool -- `checkOpAdmitted` already ran
    /// (admitted as `Live`, and NOT a settled `Vanished` state, which would have short-circuited `Probe` to
    /// `TruthAbsent` before any classification). This is the last silent-empty-load killer: an empty table
    /// root is exactly what a silently-erased backing looks like, and a read-only pool (no lease, no
    /// erasure observer) has no other line of defense. So the empty answer is authorized ONLY by an
    /// AUTHORITATIVE, UNCACHED positive on the pool identity object -- a cached positive never suffices.
    const Cas::PoolPtr pool = store();   /// Live here (past the op gate's Live, non-terminal admission).

    ++empty_proof_probe_count_for_test;
    const Cas::SentinelProbeResult probe = empty_proof_probe_override_for_test
        ? empty_proof_probe_override_for_test()
        : Cas::probeSentinel(pool->backend(), pool->layout().poolMetaKey());

    switch (probe.outcome)
    {
        case Cas::ProbeOutcome::Present:
            /// The pool identity is authoritatively present -- the empty listing is the truth.
            return;
        case Cas::ProbeOutcome::KeyAbsent:
        case Cas::ProbeOutcome::ContainerAbsent:
            /// A clean authoritative miss on `_pool_meta`: the backing is (or is being) erased. Refuse
            /// the empty answer rather than silently attaching an empty table over an erased pool.
            throw Exception(ErrorCodes::INVALID_STATE,
                "content-addressed disk '{}' -- pool identity object absent while enumerating '{}' -- "
                "refusing the empty answer; the backing may be erased",
                disk_name, path);
        case Cas::ProbeOutcome::AccessDenied:
        case Cas::ProbeOutcome::Indeterminate:
            /// Absence was NEVER established (a transport/permission fault). Fail closed and TRANSIENT --
            /// never promote an unproven probe into an empty answer, and never let a consumer read an
            /// unreachable pool identity as damage. The arm above, where absence IS proven, keeps its
            /// terminal 668: an erased backing does not heal by retrying. This arm promises no particular
            /// recovery either: `AccessDenied` is a credential/policy fault that a return to `Live` does
            /// not clear, so "retry" is the only honest guidance for the pair.
            Cas::throwCasTransientUnavailable(
                fmt::format("content-addressed disk '{}'", disk_name),
                fmt::format("pool identity object could not be confirmed while enumerating '{}' "
                            "(transport or permission fault) -- refusing the empty answer; retry", path));
    }
}

MetadataTransactionPtr ContentAddressedMetadataStorage::createTransaction()
{
    checkNotReadOnly("writes");
    return std::make_shared<ContentAddressedTransaction>(*this);
}

String ContentAddressedMetadataStorage::stagingKeyPrefix() const
{
    /// This is the writer-owned `staging/<server_root_id>/` subtree. `store` throws `INVALID_STATE`
    /// when no pool is published; every writer caller runs after `startup`.
    return physicalKey(store()->poolConfig().pool_prefix + "/staging/" + server_root_id);
}

/// ==== namespace mapping ====

std::string ContentAddressedMetadataStorage::serverPrefix() const
{
    /// Live namespaces and mirrored live-tree files are rooted by the configured
    /// `server_root_id`, not by the ClickHouse ServerUUID-derived token. `ServerUUID` is only the
    /// mount owner token; `server_root_id` is the persistent layout identity.
    return server_root_id;
}

std::vector<std::string> ContentAddressedMetadataStorage::listLiveTreeChildren(const std::string & path) const
{
    /// Probe: a Vanished disk enumerates empty (truth). Its callers (`listDirectory`) already gate, but
    /// this public helper is gated too so a direct call is truthful.
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return {};
    const std::string canonical = canonicalDiskPath(path);
    const std::string scope = serverPrefix() + "/" + (canonical.empty() ? "" : canonical + "/");
    std::unordered_set<std::string> result;
    for (const auto & child : store()->listMirroredChildren(scope))
        result.emplace(stripCasArchiveSuffix(child));
    return toVector(std::move(result));
}

bool ContentAddressedMetadataStorage::liveTreeDirHasChildren(const std::string & path) const
{
    /// Probe gate FIRST, ahead of the hardcoded disk-root short-circuit below (the rev.7 offender): on a
    /// Vanished disk even the disk root reads absent (truth), never the unconditional `true`.
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return false;
    const std::string canonical = canonicalDiskPath(path);
    /// The disk root always exists; otherwise a non-empty server-root-scoped mirrored LIST is the signal.
    if (canonical.empty())
        return true;
    const std::string scope = serverPrefix() + "/" + canonical + "/";
    return !store()->listMirroredChildren(scope).empty();
}

Cas::RootNamespace ContentAddressedMetadataStorage::liveNamespace(const std::string & table_uuid) const
{
    /// Path mirroring: the namespace is the table's canonical disk path with the
    /// content-addressed boundary marked by `@cas@` on the table-dir segment, prefixed by the
    /// configured `server_root_id`. e.g. `<server_root_id>/store/3f2/3f2a…@cas@`.
    return Cas::RootNamespace{serverPrefix() + "/" + Cas::mirroredArchiveNamespace(table_uuid)};
}

std::optional<Cas::NamespaceLifeId>
ContentAddressedMetadataStorage::readableNamespaceFilesLife(const Cas::RootNamespace & ns) const
{
    return store()->namespaceFilesLifeIfReadable(ns);
}

Cas::RootNamespace ContentAddressedMetadataStorage::shadowNamespace(const std::string & shadow_table_dir) const
{
    /// The LITERAL shadow table dir (shadow/<backup>/store/<u3>/<uuid> or .../data/<db>/<tbl>) is
    /// bijective with the disk path for both layouts, and the disk path itself is unchanged by this
    /// prefix. Canonicalize because the unfreezer can hand the directory a trailing slash.
    return Cas::RootNamespace{serverPrefix() + "/" + canonicalDiskPath(shadow_table_dir)};
}

std::string ContentAddressedMetadataStorage::shadowScope(const std::string & path) const
{
    const std::string canonical = canonicalDiskPath(path);
    return serverPrefix() + "/" + (canonical.empty() ? "shadow/" : canonical + "/");
}

std::optional<ContentAddressedMetadataStorage::Route>
ContentAddressedMetadataStorage::route(const Cas::PartFilePath & p) const
{
    Route r;
    if (!p.backup_name.empty())
    {
        r.ns = shadowNamespace(p.shadow_table_dir);
        r.ref = p.part_name;
        r.file = p.file;
        return r;
    }
    if (p.part_name == Cas::kDetachedDirName)
    {
        /// The parser reports detached paths with part_name == "detached" and the real detached
        /// part dir as the first component of `file`. Detached parts share the table namespace and
        /// INTO the table's OWN archive namespace: each detached part is a ref keyed by
        /// `detached/<part>` (vs a live `<part>`), so the re-split here keeps the table namespace
        /// and prepends the `detached/` ref prefix. An empty `p.file` (the bare `<table>/detached`
        /// container dir) yields an empty ref → the filtered-container listing path.
        r.ns = liveNamespace(p.table_uuid);
        auto [part, file] = splitFirstComponent(p.file);
        r.ref = part.empty() ? "" : std::string(Cas::kDetachedRefPrefix) + part;
        r.file = file;
        return r;
    }
    if (p.part_name == Cas::kMovingDirName)
    {
        /// L1 (MOVE-to-CA fix): re-split exactly like detached, folding onto a `moving/`-PREFIXED
        /// ref (kMovingRefPrefix) -- NOT the part's final ref directly. Publishing the clone under
        /// the final ref before the mover's swap would break move crash-atomicity: a crash between
        /// the clone publication and swapClonedPart would leave a committed LIVE ref that never went
        /// through the swap, and moving/'s own startup cleanup couldn't distinguish that premature
        /// ref from a real live part. The staging ref keeps the pre-swap clone un-live; the mover's
        /// rename does a real ref repoint moving/<part> -> <part> (the same committed-ref-repoint
        /// path merge-result/delete_tmp renames already use). An empty p.file (the bare
        /// <table>/moving container dir) yields an empty ref, same convention as detached.
        r.ns = liveNamespace(p.table_uuid);
        auto [part, file] = splitFirstComponent(p.file);
        r.ref = part.empty() ? "" : std::string(Cas::kMovingRefPrefix) + part;
        r.file = file;
        return r;
    }
    r.ns = liveNamespace(p.table_uuid);
    r.ref = p.part_name;
    r.file = p.file;
    return r;
}

std::vector<std::string> ContentAddressedMetadataStorage::detachedRefNames(const Cas::RootNamespace & ns) const
{
    std::vector<std::string> refs;
    for (const auto & [ref, _] : store()->listRefs(ns))
        if (ref.starts_with(Cas::kDetachedRefPrefix))
            refs.push_back(ref);
    return refs;
}

std::vector<std::string> ContentAddressedMetadataStorage::movingRefNames(const Cas::RootNamespace & ns) const
{
    std::vector<std::string> refs;
    for (const auto & [ref, _] : store()->listRefs(ns))
        if (ref.starts_with(Cas::kMovingRefPrefix))
            refs.push_back(ref);
    return refs;
}

/// ==== read surface ====

bool ContentAddressedMetadataStorage::existsFile(const std::string & path) const
{
    /// Probe gate (rev.7 §1): real while live, throws while uncertain (incl. a null/unstarted pool),
    /// truthfully absent once Vanished.
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return false;
    if (!Cas::isPartFilePath(path))
    {
        if (auto tf = Cas::parseTableFilePath(path))
        {
            const auto life = readableNamespaceFilesLife(liveNamespace(tf->table_uuid));
            return life && store()->getNamespaceFile(*life, tf->tail).has_value();
        }
        /// A loose mountpoint object is a plain object at roots/<server_root_id>/<path>.
        /// Use a HEAD-based existence check (directory-safe), NOT a body read: the traversal in
        /// system.remote_data_paths probes existsFile on directory-shaped pool paths (e.g. `store`), and a
        /// body read (getMountpointObject) throws "Is a directory". A directory is not a file.
        return store()->mountpointObjectExists(serverPrefix() + "/" + path);
    }

    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        return false;
    auto r = route(*p);
    if (!r || r->file.empty())
        return false;

    /// Per-part files flow through the ordinary content path like any other file; no ForceFresh
    /// special case is needed.
    /// Safe to serve a CACHED view here: every committed-ref write that could have moved this entry
    /// (`repointRef`/`promoteBuild`) erases the cached view on success, so a stale hit is impossible
    /// by construction, not by freshness policy.
    auto view = partAccess()->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    return view && view->findFile(r->file);
}

ContentAddressedMetadataStorage::DirRoute ContentAddressedMetadataStorage::classifyDirectory(const std::string & path) const
{
    DirRoute dr;

    /// FREEZE shadow namespace — routed BEFORE the live branches (a shadow table dir also
    /// satisfies parseTableUuid).
    if (Cas::isShadowPath(path))
    {
        if (auto p = Cas::parsePartFilePath(path); p && !p->backup_name.empty() && p->file.empty())
        {
            dr.shape = DirShape::ShadowPart;
            dr.p = std::move(p);
            return dr;
        }
        if (Cas::endsWithTableUuidPair(path))
        {
            dr.shape = DirShape::ShadowTable;
            return dr;
        }
        dr.shape = DirShape::ShadowIntermediate;
        return dr;
    }

    /// The Atomic `store/<u3>` shard dir (see listDirectory): route to the generic existence signal
    /// before parseTableUuid/parseTableFilePath misclaim it as a non-Atomic table.
    if (Cas::isAtomicShardDir(path))
    {
        dr.shape = DirShape::AtomicShard;
        return dr;
    }

    if (auto uuid = Cas::parseTableUuid(path))
    {
        dr.shape = DirShape::TableDir;
        dr.uuid = std::move(uuid);
        return dr;
    }

    if (auto p = Cas::parsePartFilePath(path))
    {
        auto r = route(*p);
        /// The detached CONTAINER dir <table>/detached.
        if (r && r->ref.empty() && p->part_name == Cas::kDetachedDirName)
        {
            dr.shape = DirShape::DetachedContainer;
            dr.p = std::move(p);
            dr.r = std::move(r);
            return dr;
        }
        /// The moving CONTAINER dir <table>/moving (MOVE-to-CA fix): the mover's crash-cleanup
        /// (MergeTreeData.cpp, MOVING_DIR_NAME) existsDirectory/removeRecursive's this bare path
        /// at every table load to reclaim a staging ref left behind by an interrupted move.
        if (r && r->ref.empty() && p->part_name == Cas::kMovingDirName)
        {
            dr.shape = DirShape::MovingContainer;
            dr.p = std::move(p);
            dr.r = std::move(r);
            return dr;
        }
        /// A part dir (live, detached, or shadow).
        if (r && !r->ref.empty() && r->file.empty())
        {
            dr.shape = DirShape::PartDir;
            dr.p = std::move(p);
            dr.r = std::move(r);
            return dr;
        }
        /// A projection dir.
        if (r && !r->ref.empty())
        {
            if (auto prefix = Cas::PartFolderView::projectionDirPrefix(r->file))
            {
                dr.shape = DirShape::ProjectionDir;
                dr.p = std::move(p);
                dr.r = std::move(r);
                dr.projection_prefix = std::move(prefix);
                return dr;
            }
        }
        /// No sub-shape matched: fall through, identical to today's post-`if (p)` continuation.
    }

    /// A table-level SUBDIRECTORY (deduplication_logs/...).
    if (auto tf = Cas::parseTableFilePath(path))
    {
        dr.shape = DirShape::TableSubdir;
        dr.tf = std::move(tf);
        return dr;
    }

    /// A generic INTERMEDIATE live-tree directory (disk root, `store`, ...).
    dr.shape = DirShape::GenericIntermediate;
    return dr;
}

bool ContentAddressedMetadataStorage::existsDirectory(const std::string & path) const
{
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return false;
    const DirRoute dr = classifyDirectory(path);
    switch (dr.shape)
    {
        case DirShape::ShadowPart:
            return partAccess()->existsRef(Route{shadowNamespace(dr.p->shadow_table_dir), dr.p->part_name, ""}.refKey(),
                                          Cas::Freshness::CachedForLoad);
        case DirShape::ShadowTable:
            return store()->hasAnyRefWithPrefix(shadowNamespace(path), "");
        case DirShape::ShadowIntermediate:
        {
            /// Intermediate dir (shadow/<bk>, shadow/<bk>/store, ...): exists iff SOME shadow namespace
            /// under this path still has a LIVE ref. A raw object LIST of the mirrored subtree would count
            /// tombstoned-but-not-yet-GC'd shard/manifest objects — CA removal is tombstone + deferred GC
            /// (`removeRecursive`/`dropNamespace` only tombstone; `Cas::Gc` physically deletes later) — so a
            /// just-`UNFREEZE`d backup dir would spuriously "exist" until a GC round runs. Instead
            /// enumerate the namespaces exactly as `removeRecursive` does (`listNamespaces(scope)`) and
            /// consult the tombstone-aware `listRefs` (as the `endsWithTableUuidPair` case above does), so
            /// existence is consistent with the ref-level signal and independent of GC timing.
            const Cas::NamespaceListing listing = store()->listNamespaces(shadowScope(path));
            for (const auto & ns : listing.namespaces)
                if (store()->hasAnyRefWithPrefix(Cas::RootNamespace{ns}, ""))
                    return true;
            /// A key this scope's enumeration could not attribute leaves emptiness UNPROVEN, and the
            /// fail-close answer for an existence probe is "present": reporting absent is what would let
            /// a caller treat the subtree as gone. Answering present is bounded -- an already-unfrozen
            /// directory keeps showing up until the key is cleared -- and it never claims absence that
            /// was not established.
            ///
            /// The answer alone would leave an operator with a directory that will not go away and
            /// nothing naming the key that holds it, so the key and the refusal are LOGGED here too --
            /// the other three consumers of this enumeration each surface the skip to a human, and a
            /// boolean is not that. Rate-limited because this is an existence probe on a browse path:
            /// `LogSeriesLimiter` keys on the LOGGER NAME, so one message per window prints regardless of
            /// which key it was about.
            if (!listing.skipped.empty())
            {
                LogSeriesLimiter log(getLogger("CasShadowScopeLifelessKey"), /*allowed_count=*/1, /*interval_s=*/60);
                LOG_WARNING(log,
                    "existsDirectory('{}'): {} key(s) under this scope name no namespace life, so it "
                    "cannot be proven empty and is reported as PRESENT. First such key: '{}' ({}). Run "
                    "`cas-fsck` to enumerate them all.",
                    path, listing.skipped.size(), listing.skipped.front().key, listing.skipped.front().reason);
                return true;
            }
            return false;
        }
        case DirShape::AtomicShard:
            return liveTreeDirHasChildren(path);
        case DirShape::TableDir:
            /// A table directory exists iff its logical namespace still has foreground removal work
            /// outstanding, or has never proven completion: present while `Creating`, for every `Live`
            /// row (even zero parts and zero namespace files -- an empty live table is still a table),
            /// and while `Removing` before its terminal `remove_namespace` transaction is durable;
            /// absent only once no catalog row exists at all, or the terminal is durably proven. This is
            /// deliberately NOT "has a committed ref": a table that removed its last part, or that never
            /// wrote one, must stay present until an actual namespace-drop admits and completes removal
            /// -- otherwise `DROP TABLE` on such a table would silently skip physical cleanup and leak
            /// its catalog row forever.
            return store()->namespaceStillLogicallyPresent(liveNamespace(*dr.uuid));
        case DirShape::DetachedContainer:
            /// Exists iff it has at least one reference.
            return store()->hasAnyRefWithPrefix(dr.r->ns, Cas::kDetachedRefPrefix);
        case DirShape::MovingContainer:
            /// Exists iff it has at least one staging ref (MOVE-to-CA fix, mirrors DetachedContainer).
            return store()->hasAnyRefWithPrefix(dr.r->ns, Cas::kMovingRefPrefix);
        case DirShape::PartDir:
            /// Exists iff its ref is present.
            return partAccess()->existsRef(dr.r->refKey(), Cas::Freshness::CachedForLoad);
        case DirShape::ProjectionDir:
        {
            /// At least one tree entry (or mutable file) under its prefix.
            auto view = partAccess()->getView(dr.r->refKey(), Cas::Freshness::CachedForLoad);
            return view && view->hasDirectory(*dr.projection_prefix);
        }
        case DirShape::TableSubdir:
        {
            /// At least one verbatim file under it.
            const auto life = readableNamespaceFilesLife(liveNamespace(dr.tf->table_uuid));
            if (!life)
                return false;
            const std::string prefix = dr.tf->tail + "/";
            for (const auto & name : store()->listNamespaceFiles(*life))
                if (name.starts_with(prefix))
                    return true;
            return false;
        }
        case DirShape::GenericIntermediate:
            /// Exists iff a server-root-scoped mirrored LIST finds any object. Keeps `cd`/existence
            /// consistent with listDirectory so `clickhouse-disks` traversal behaves like a normal disk.
            return liveTreeDirHasChildren(path);
    }
    return liveTreeDirHasChildren(path);   /// unreachable
}

bool ContentAddressedMetadataStorage::existsFileOrDirectory(const std::string & path) const
{
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return false;
    if (Cas::isPartFilePath(path))
    {
        auto p = Cas::parsePartFilePath(path);
        auto r = p ? route(*p) : std::nullopt;
        if (r && !r->ref.empty() && !r->file.empty())
        {
            auto view = partAccess()->getView(r->refKey(), Cas::Freshness::CachedForLoad);
            if (!view)
                return false;
            return view->hasFile(r->file) || view->hasDirectory(r->file + "/");
        }
    }
    return existsFile(path) || existsDirectory(path);
}

uint64_t ContentAddressedMetadataStorage::getFileSize(const std::string & path) const
{
    /// ContentRead: a size query resolves a specific file; on a Vanished disk it fails loud with the typed
    /// error rather than silent-absent (never let a reader mistake erased backing for "file not there").
    checkOpAdmitted(CasOpClass::ContentRead);
    if (!Cas::isPartFilePath(path))
    {
        if (auto bytes = tryGetInManifestBytes(path))   /// verbatim table-level file
            return bytes->size();
        if (auto bytes = store()->getMountpointObject(serverPrefix() + "/" + path))
            return bytes->size();
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
    }

    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: not a part file path: {}", path);
    auto r = route(*p);
    if (!r || r->file.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: not a part file path: {}", path);

    auto view = partAccess()->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    if (!view)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no ref for {}", path);
    if (auto size = view->fileSize(r->file))
        return *size;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: file {} not in manifest of {}", r->file, path);
}

Poco::Timestamp ContentAddressedMetadataStorage::getLastModified(const std::string & path) const
{
    /// ContentRead: resolves a specific part's stamp; loud typed error on a Vanished disk.
    checkOpAdmitted(CasOpClass::ContentRead);
    /// Timestamps are DERIVED for content addressing: the part's publish wall-clock, stamped by
    /// the transaction into the typed `RefPayload.published_at_ms` field (epoch milliseconds).
    /// Every shape (part dir, detached part dir, projection dir, part file) reports its part's
    /// stamp; a part published without a stamp (published_at_ms == 0) reports the epoch (harmless:
    /// stamps only feed cleanup TTLs and system tables).
    auto resolve_stamp = [&](const Route & r) -> Poco::Timestamp
    {
        auto resolved = partAccess()->resolve(r.refKey(), Cas::Freshness::CachedForLoad);
        if (!resolved)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no ref for {}", path);
        if (resolved->published_at_ms == 0)
            return Poco::Timestamp(0);
        /// published_at_ms is epoch milliseconds; Poco::Timestamp::fromEpochTime takes seconds.
        return Poco::Timestamp::fromEpochTime(static_cast<time_t>(resolved->published_at_ms / 1000));
    };

    if (auto p = Cas::parsePartFilePath(path))
    {
        auto r = route(*p);
        if (r && !r->ref.empty())
            return resolve_stamp(*r);
    }
    /// Table-level / generic verbatim files: no per-object mtime is kept — epoch.
    if (existsFile(path))
        return Poco::Timestamp(0);
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
}

std::vector<std::string> ContentAddressedMetadataStorage::listDirectory(const std::string & path) const
{
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return {};
    const DirRoute dr = classifyDirectory(path);
    switch (dr.shape)
    {
        case DirShape::ShadowPart:
        {
            /// Shadow PART dir: the frozen part's file names (first components).
            auto view = partAccess()->getView(Route{shadowNamespace(dr.p->shadow_table_dir), dr.p->part_name, ""}.refKey(),
                                             Cas::Freshness::CachedForLoad);
            return view ? view->listChildren("") : std::vector<std::string>{};
        }
        case DirShape::ShadowTable:
        {
            /// Shadow TABLE dir: the frozen part names.
            std::vector<std::string> result;
            for (const auto & [ref, _] : store()->listRefs(shadowNamespace(path)))
                result.push_back(ref);
            return result;
        }
        case DirShape::ShadowIntermediate:
        {
            /// Enumerate children via a scoped LIST of the mirrored subtree. A
            /// mirrored LIST naturally surfaces intermediate path segments AND `@cas@`-suffixed
            /// table dirs; strip the trailing `@cas@` for the logical view. Loose LIST is fine: the
            /// existing listRefs re-check filters out dropped-but-registered archives so they don't
            /// appear as false children.
            std::unordered_set<std::string> result;
            for (const auto & child : store()->listMirroredChildren(shadowScope(path)))
                result.emplace(stripCasArchiveSuffix(child));
            return toVector(std::move(result));
        }
        case DirShape::AtomicShard:
            /// A pure intermediate dir whose only child is the uuid-anchored table dir. Its path
            /// shape collides with the non-Atomic `data/<db>` fallback of both parseTableUuid and
            /// parseTableFilePath, so it MUST be routed to the generic mirrored LIST BEFORE those
            /// branches claim it (see classifyDirectory).
            return listLiveTreeChildren(path);
        case DirShape::TableDir:
        {
            /// Part names (live and `detached/<part>` references) plus table-level verbatim
            /// file names; addFirstComponent collapses both to their first path segment (live part
            /// names and the single `detached` subdir, exactly like a nested verbatim file).
            const auto ns = liveNamespace(*dr.uuid);
            std::unordered_set<std::string> result;
            for (const auto & [ref, _] : store()->listRefs(ns))
                addFirstComponent(result, ref);
            /// A dropped table lists empty for its namespace files too (its refs are already gone
            /// via the ref state); only surface verbatim file names while the table is not removed.
            if (const auto life = readableNamespaceFilesLife(ns))
                for (const auto & name : store()->listNamespaceFiles(*life))
                    addFirstComponent(result, name);
            /// EMPTY-PROOF RULE (Task 9, spec §1 [B3]): an empty table root is exactly what a
            /// silently-erased backing looks like -- authorize the empty answer only against an
            /// authoritative, uncached `_pool_meta` positive (see the helper).
            if (result.empty())
                confirmPoolIdentityForEmptyEnumeration(path);
            return toVector(std::move(result));
        }
        case DirShape::DetachedContainer:
        {
            /// Detached part names (prefix stripped; never files).
            std::vector<std::string> result;
            for (const auto & ref : detachedRefNames(dr.r->ns))
                result.push_back(ref.substr(Cas::kDetachedRefPrefix.size()));
            /// EMPTY-PROOF RULE (Task 9, spec §1 [B3]): same for an empty detached container root.
            if (result.empty())
                confirmPoolIdentityForEmptyEnumeration(path);
            return result;
        }
        case DirShape::MovingContainer:
        {
            /// Staging part names (prefix stripped), mirrors DetachedContainer.
            std::vector<std::string> result;
            for (const auto & ref : movingRefNames(dr.r->ns))
                result.push_back(ref.substr(Cas::kMovingRefPrefix.size()));
            return result;
        }
        case DirShape::PartDir:
        {
            /// A part dir (live, detached part, shadow handled separately): logical file names,
            /// nested keys collapsed to their first component (projections surface as ONE
            /// <proj>.proj entry).
            auto view = partAccess()->getView(dr.r->refKey(), Cas::Freshness::CachedForLoad);
            return view ? view->listChildren("") : std::vector<std::string>{};
        }
        case DirShape::ProjectionDir:
        {
            /// Inner names with the <proj>.proj/ prefix stripped.
            auto view = partAccess()->getView(dr.r->refKey(), Cas::Freshness::CachedForLoad);
            return view ? view->listChildren(*dr.projection_prefix) : std::vector<std::string>{};
        }
        case DirShape::TableSubdir:
        {
            /// Verbatim files under <subdir>/, first-component collapsed.
            std::unordered_set<std::string> result;
            if (const auto life = readableNamespaceFilesLife(liveNamespace(dr.tf->table_uuid)))
                for (const auto & name : store()->listNamespaceFiles(*life))
                    if (name.starts_with(dr.tf->tail + "/"))
                        addFirstComponent(result, name.substr(dr.tf->tail.size() + 1));
            return toVector(std::move(result));
        }
        case DirShape::GenericIntermediate:
            /// The disk root "", `store`, or any loose-file container above a table dir: a
            /// server-root-scoped mirrored LIST. (`store/<u3>` is handled by AtomicShard above,
            /// since its non-Atomic-table ambiguity would otherwise misroute it here too late,
            /// after parseTableUuid/parseTableFilePath have already claimed it.)
            return listLiveTreeChildren(path);
    }
    return listLiveTreeChildren(path);   /// unreachable
}

DirectoryIteratorPtr ContentAddressedMetadataStorage::iterateDirectory(const std::string & path) const
{
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return std::make_unique<StaticDirectoryIterator>(std::vector<fs::path>{});
    /// Mirror MetadataStorageFromPlainObjectStorage: iterateDirectory includes the path.
    auto names = listDirectory(path);
    std::vector<fs::path> fs_paths;
    fs_paths.reserve(names.size());
    for (const auto & child : names)
        fs_paths.push_back(fs::path(path) / child);
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

bool ContentAddressedMetadataStorage::isDirectoryEmpty(const std::string & path) const
{
    /// A Vanished disk reports every directory empty too (truth): the ref-unlink removal path then
    /// proceeds, letting a vanished-disk table's DROP complete rather than throwing CANNOT_RMDIR.
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return true;
    /// A part directory's files are virtual (derived from the tree): report it EMPTY so
    /// DiskObjectStorage::removeDirectory proceeds straight to the ref-unlink instead of throwing
    /// CANNOT_RMDIR per removal. The same applies to a projection subdirectory. The detached
    /// CONTAINER and TABLE dirs keep the listing-based emptiness (DROP TABLE's non-empty guard).
    if (auto p = Cas::parsePartFilePath(path))
    {
        auto r = route(*p);
        if (r && !r->ref.empty() && r->file.empty())
            return true;
        if (r && !r->ref.empty() && Cas::PartFolderView::projectionDirPrefix(r->file))
            return true;
    }
    return !iterateDirectory(path)->isValid();
}

StoredObjects ContentAddressedMetadataStorage::getStorageObjects(const std::string & path) const
{
    /// ContentRead: resolves an object; loud typed error on a Vanished disk (never silent-empty).
    checkOpAdmitted(CasOpClass::ContentRead);
    /// In-manifest bytes (mutable per-part files, inline entries, verbatim namespace files) have
    /// no object of their own: DiskObjectStorage::prepareRead serves them via tryGetInManifestBytes
    /// BEFORE asking for storage objects. The sized empty-key placeholder below keeps size-only
    /// consumers working and makes any bypassing reader fail LOUDLY (never silently wrong bytes).
    if (auto bytes = tryGetInManifestBytes(path))
        return {StoredObject("", path, bytes->size())};

    if (!Cas::isPartFilePath(path))
    {
        if (Cas::parseTableFilePath(path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                "ContentAddressed: table-level verbatim file is in-manifest, not a storage object: {}", path);
        /// A loose mountpoint object: a real plain object at roots/<server_root_id>/<path>. The
        /// StoredObject key must be the PHYSICAL path (physicalKey-adjusted for Local backends).
        /// Probe with a HEAD (directory-safe), not a body read: `system.remote_data_paths`
        /// may reach here on a directory-shaped pool path and a GET would throw "Is a directory".
        const std::string pool_key = store()->layout().mountpointObjectKey(serverPrefix() + "/" + path);
        if (store()->mountpointObjectExists(serverPrefix() + "/" + path))
            return {StoredObject(physicalKey(pool_key), path, getFileSize(path))};
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
    }

    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: not a part file path: {}", path);
    auto r = route(*p);
    if (!r || r->file.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: not a part file path: {}", path);

    /// ONE snapshot for both the facade lookup and the pool `locate` below, so the two can never
    /// straddle two different mount generations (see `poolAccess()`).
    const auto snap = poolAccess();
    auto view = snap.part_access->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    if (!view)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no ref for {}", path);
    if (const auto * entry = view->findFile(r->file))
    {
        const auto location = snap.pool->locate(*entry);
        /// StoredObject carries no range (the recorded upstream delta) — the PAYLOAD length is the
        /// size (what every size consumer wants); the header offset is applied by
        /// getBlobViewPlan's view window, the only byte-reading path.
        return {StoredObject(location.key, path, location.length)};
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: file {} not in manifest of {}", r->file, path);
}

std::optional<StoredObjects> ContentAddressedMetadataStorage::getStorageObjectsIfExist(const std::string & path) const
{
    /// A Vanished disk answers absent (truth). Probe first so the non-part `getStorageObjects` fallback
    /// below (ContentRead) is never reached on a Vanished disk.
    if (checkOpAdmitted(CasOpClass::Probe) == CasOpAdmission::TruthAbsent)
        return std::nullopt;
    /// Non-part shapes (verbatim table files, loose mountpoint objects) are rare paths — the
    /// generic two-step is fine for them.
    if (!Cas::isPartFilePath(path))
    {
        if (existsFile(path))
            return getStorageObjects(path);
        return std::nullopt;
    }
    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        return std::nullopt;
    auto r = route(*p);
    if (!r || r->file.empty())
        return std::nullopt;

    /// ONE snapshot for both the facade lookup and the pool `locate` below (see `poolAccess()`).
    const auto snap = poolAccess();
    auto view = snap.part_access->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    if (!view)
        return std::nullopt;
    const auto * entry = view->findFile(r->file);
    if (!entry)
        return std::nullopt;
    if (entry->placement == Cas::EntryPlacement::Inline)
        return StoredObjects{StoredObject("", path, entry->size())};
    const auto location = snap.pool->locate(*entry);
    return StoredObjects{StoredObject(location.key, path, location.length)};
}

std::optional<String> ContentAddressedMetadataStorage::tryGetInManifestBytes(const std::string & path) const
{
    /// Speculative in-manifest probe: `DiskObjectStorage::prepareRead`/`getStorageObjects` call it before
    /// falling back to a real storage-object lookup, and both treat "not in-manifest" (`std::nullopt`) as a
    /// normal outcome. But this is a ContentRead: a disk in a terminal/uncertain/unstarted lifecycle must
    /// PROPAGATE the typed 668 rather than convert it into a silent-absent `std::nullopt` -- so the gate
    /// REPLACES the old catch-all that swallowed `poolAccess()`'s `INVALID_STATE` (which had hidden an
    /// erased/replaced/transient backing behind a FILE_DOESNT_EXIST-shaped answer).
    checkOpAdmitted(CasOpClass::ContentRead);
    const PoolAccessSnapshot snap = poolAccess();

    if (!Cas::isPartFilePath(path))
    {
        if (auto tf = Cas::parseTableFilePath(path))
        {
            const auto life = readableNamespaceFilesLife(liveNamespace(tf->table_uuid));
            return life ? snap.pool->getNamespaceFile(*life, tf->tail) : std::nullopt;
        }
        return std::nullopt;   /// loose files are plain objects, not in-manifest bytes
    }

    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        return std::nullopt;
    auto r = route(*p);
    if (!r || r->file.empty())
        return std::nullopt;

    auto view = snap.part_access->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    if (!view)
        return std::nullopt;
    return view->inlineBytes(r->file);
}

bool ContentAddressedMetadataStorage::prepareInManifestRead(
    const std::string & path, const ReadSettings & settings, ReadPipeline & pipeline) const
{
    /// In-manifest bytes (mutable per-part files, inline entries, verbatim namespace files):
    /// served from memory — there is no object to read.
    auto bytes = tryGetInManifestBytes(path);
    if (!bytes)
        return false;

    const auto size = bytes->size();
    auto creator = [path, data = std::move(*bytes)](
        const StoredObject &, const ReadSettings &, bool, bool) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromOwnMemoryFile>(path, data);
    };
    pipeline.setSource(std::move(creator), {StoredObject("", path, size)}, settings);
    return true;
}

std::optional<ContentAddressedMetadataStorage::BlobViewPlan> ContentAddressedMetadataStorage::getBlobViewPlan(
    const std::string & path) const
{
    /// ContentRead: resolves a blob-backed path to its physical window; loud typed error on a Vanished
    /// disk rather than a silent `std::nullopt` (which a reader would take as "not blob-backed").
    checkOpAdmitted(CasOpClass::ContentRead);
    if (!Cas::isPartFilePath(path))
        return std::nullopt;
    auto p = Cas::parsePartFilePath(path);
    if (!p || p->file.empty())
        return std::nullopt;
    auto r = route(*p);
    if (!r || r->file.empty())
        return std::nullopt;
    /// ONE snapshot for both the facade lookup and the pool `locate` below, instead of the previous
    /// `partAccess()` then `store()` pair (each an independent `pointer_mutex` acquisition) -- see
    /// `poolAccess()`.
    const auto snap = poolAccess();
    auto view = snap.part_access->getView(r->refKey(), Cas::Freshness::CachedForLoad);
    if (!view)
        return std::nullopt;
    if (const auto * entry = view->findFile(r->file))
    {
        const auto location = snap.pool->locate(*entry);
        BlobViewPlan plan;
        /// bytes_size is the readable extent of THIS file's window, NOT the whole blob: a
        /// right-bounded read stops at payload_end, and a shared blob's bytes beyond it belong
        /// to other files. The caches key on the physical blob key, so payload ranges are
        /// shared between every part that references the same blob.
        plan.object = StoredObject(physicalKey(location.key), path, location.offset + location.length);
        plan.payload_offset = location.offset;
        plan.payload_end = location.offset + location.length;
        return plan;
    }
    return std::nullopt;
}

std::unique_ptr<ReadBufferFromFileBase> ContentAddressedMetadataStorage::readBlobPayload(
    const Cas::BlobLocation & location, const std::string & path, const ReadSettings & settings) const
{
    /// ContentRead: the actual byte read; loud typed error on a Vanished disk instead of a raw backend
    /// "no such key" from the erased object.
    checkOpAdmitted(CasOpClass::ContentRead);
    auto impl = object_storage->readObject(
        StoredObject(physicalKey(location.key), path, location.offset + location.length), settings);
    return std::make_unique<ReadBufferFromFileView>(
        std::move(impl), path, location.offset, location.offset + location.length);
}

/// ==== `IContentAddressedExchange` ====

bool ContentAddressedMetadataStorage::ownsNamespace(const String & other_server_root_id, const String & root_namespace) const
{
    /// Routing for the relink confirm (spec §wire-protocol). `pool_uuid` says which POOL a token refers
    /// to and is compared by the caller; every server root writing into that pool shares it, so the
    /// namespace's owner is decided here. `liveNamespace` and `shadowNamespace` build every owned
    /// namespace under `<server_root_id>/`, so ownership is exactly "rooted at MY server root".
    /// The strict prefix (not a bare equality, not `starts_with(server_root_id)`) is what keeps
    /// `srv1` from claiming `srv10/...`; the `FREEZE` tree follows the same ownership rule as live
    /// and detached content.
    ///
    /// Factory-class: no `store()`, no gate, no I/O, no throw. A misrouted question must come back as
    /// an unproven answer, never as an error.
    if (other_server_root_id.empty() || other_server_root_id != server_root_id)
        return false;
    return root_namespace.starts_with(server_root_id + "/");
}

CasConfirmAnswer ContentAddressedMetadataStorage::confirmExactRef(
    const String & root_namespace, const String & ref_name, const String & manifest_ref_text) const
{
    /// Gate 1 of the relink confirm: a thin forward to the ledger, whose declaration
    /// (`CasRefLedger::confirmExactRef`) carries the six-rule snapshot and the zero-I/O contract. This
    /// layer adds exactly two things: the token text is decoded here, and the disk's own lifecycle is
    /// answered as `Unknown` instead of as an exception.
    const auto manifest_ref = Cas::tryParseManifestRef(manifest_ref_text);
    if (!manifest_ref)
    {
        LOG_DEBUG(getLogger("ContentAddressedMetadataStorage"),
            "Relink confirm for ref '{}' in namespace '{}' is unanswerable: manifest reference '{}' is not "
            "the canonical epoch:build:ordinal form", ref_name, root_namespace, manifest_ref_text);
        return CasConfirmAnswer::Unknown;
    }

    Cas::ConfirmAnswer answer = Cas::ConfirmAnswer::Unknown;
    try
    {
        /// ContentRead: the confirm reads this disk's committed view. A disk that never started, was shut
        /// down, is transiently not live, or has reached a terminal lifecycle has no committed view to
        /// speak for -- and `checkOpAdmitted` says so by throwing.
        checkOpAdmitted(CasOpClass::ContentRead);
        answer = store()->confirmExactRef(Cas::RootNamespace{root_namespace}, ref_name, *manifest_ref);
    }
    catch (const Exception & e)
    {
        /// This is NOT a fallback path: `Unknown` is the typed refusal this primitive is built around,
        /// not an alternate behavior substituted for a failed one. Nothing consequential happens on it --
        /// the receiver aborts its prepared relink and retries later -- so swallowing the lifecycle
        /// refusal costs a retry and can never authorize anything. Only `Yes` authorizes, and no `catch`
        /// can produce a `Yes`.
        LOG_DEBUG(getLogger("ContentAddressedMetadataStorage"),
            "Relink confirm for ref '{}' in namespace '{}' is unanswerable on disk '{}': {}",
            ref_name, root_namespace, disk_name, e.message());
        return CasConfirmAnswer::Unknown;
    }

    switch (answer)
    {
        case Cas::ConfirmAnswer::Yes:
            return CasConfirmAnswer::Yes;
        case Cas::ConfirmAnswer::No:
            return CasConfirmAnswer::No;
        case Cas::ConfirmAnswer::Unknown:
            return CasConfirmAnswer::Unknown;
    }
}

std::optional<IContentAddressedExchange::RelinkOffer>
ContentAddressedMetadataStorage::getRelinkOffer(const String & part_path) const
{
    /// Sender side: the committed part's encoded `PartManifest` body — the opaque payload the
    /// receiver decodes — and the confirm token for it. Resolve the part path to its (ns, ref) exactly
    /// as the read surface does (route), resolve the committed ref to its ManifestId, read the
    /// immutable manifest, and re-encode it canonically. nullopt when the path is not a committed
    /// content-addressed part here (no ref => no relink offer; the sender streams bytes). A live ref to
    /// a missing/corrupt manifest throws (INV-NO-DANGLE surfaced, never substituted) — the same
    /// fail-loud contract as partAccess()->getView.
    /// ContentRead: reading a committed manifest; loud typed error on a Vanished disk.
    checkOpAdmitted(CasOpClass::ContentRead);
    auto p = Cas::parsePartFilePath(part_path);
    if (!p)
        return std::nullopt;
    auto r = route(*p);
    if (!r || r->ref.empty())
        return std::nullopt;

    auto view = partAccess()->getView(r->refKey(), Cas::Freshness::ForceFresh);
    if (!view)
        return std::nullopt;

    /// The token names the manifest THIS view resolved, so the offer and the question the receiver will
    /// ask are about one and the same object by construction. `manifestId` is the journal identity the
    /// ledger compares in gate 1, and it is already proven to agree with the body: `readManifest`
    /// enforces `refMatchesBody`/`manifestNamespaceMatches` and throws `CORRUPTED_DATA` otherwise, so
    /// there is no disagreement left for this function to discover or to quietly turn into a byte fetch.
    ///
    /// `ref_name` is what this mount publishes the part under and `part_name` is what gate 0 looks up
    /// in the parts set; they coincide for every offer the sender can actually make, because it offers
    /// only a live part. A staging path (`detached/`, `moving/`) reports the reserved directory name as
    /// `part_name`, so a token minted for one selects no part at all — `Unknown`, which is the safe
    /// direction — rather than selecting the wrong one.
    const auto token = encodeCasRelinkSourceToken(CasRelinkSourceToken{
        .pool_uuid = pool_uuid,
        .server_root_id = server_root_id,
        .root_namespace = r->ns.string(),
        .ref_name = r->ref,
        .part_name = p->part_name,
        .manifest_ref_text = Cas::manifestRefDebugString(view->manifestId().ref)});
    if (!token)
        return std::nullopt;

    return RelinkOffer{.manifest_bytes = Cas::encodePartManifest(*view->manifest()), .confirm_token = *token};
}

namespace
{

/// The exchange's view of one durable-but-unpromoted relink: a `Cas::PreparedPartWrite` with the two
/// content-addressed-free verbs `DataPartsExchange` is allowed to know about.
///
/// It also OWNS a `shared_ptr` snapshot of the part-folder facade, and that is load-bearing rather than
/// tidy: `PreparedPartWrite` holds its owner as a raw pointer, and this handle deliberately outlives
/// the call that made it -- it spans an interserver round trip -- so a concurrent `shutdown` resetting
/// the disk's facade would dangle that pointer. The snapshot is declared BEFORE the write, so the write
/// (and any abort its destructor runs) is destroyed first, while the facade is still alive.
class PreparedRelinkOverPartWrite : public ICaPreparedRelink
{
public:
    PreparedRelinkOverPartWrite(std::shared_ptr<Cas::CachedPartFolderAccess> access_, Cas::PreparedPartWrite write_,
                                String ref_name_)
        : access(std::move(access_)), write(std::move(write_)), ref_name(std::move(ref_name_))
    {
    }

    CaRelinkPromote promote() override
    {
        try
        {
            write.promote();
            return CaRelinkPromote::Committed;
        }
        catch (const Exception & e)
        {
            /// UNCERTAINTY IS CHECKED FIRST, and it outranks the error code. A `NETWORK_ERROR` out of
            /// the promote means one of two entirely different things: the promote was rejected before
            /// its ref-log append (nothing committed), or the append itself was attempted and did not
            /// resolve -- in which case the promotion PUT may have landed and the ref may be live. The
            /// error code cannot tell them apart, which is why the transaction records the distinction
            /// as it happens. Reporting the second case as a mechanism fallback would have the receiver
            /// fetch the bytes and publish a second time over a relink that already committed.
            if (write.commitIsUnresolved())
            {
                LOG_INFO(getLogger("ContentAddressedMetadataStorage"),
                    "Relink of part {} could not be resolved: the promotion append may or may not have "
                    "committed ({}); the caller must retry the whole fetch later, NOT fetch the bytes",
                    ref_name, e.message());
                return CaRelinkPromote::Unresolved;
            }
            /// The same retryable class the staging half classifies: a body-absent precommit, a
            /// precommit binding that is no longer the live owner, or a ref conflict. `promote` has
            /// already abandoned the build on its way out, so the `+1` is released and the sender's
            /// bytes are a sound recovery. Anything else propagates -- it is not a known-safe
            /// mechanism failure, and the receiver must not silently turn it into a byte fetch.
            if (e.code() != ErrorCodes::ABORTED && e.code() != ErrorCodes::NETWORK_ERROR)
                throw;
            LOG_INFO(getLogger("ContentAddressedMetadataStorage"),
                "Relink of part {} could not be promoted (body-absent precommit, precommit not the live "
                "owner, or a ref conflict): {}; the caller may fetch the bytes from the same source",
                ref_name, e.message());
            return CaRelinkPromote::MechanismFallbackAllowed;
        }
    }

    void abort() noexcept override
    {
        /// Not defensive noise: a `promote` that FAILED discharges the duty itself (its catch abandons
        /// the build), so the scope guard that always runs finds a terminal handle on exactly that path.
        if (write.isTerminal())
            return;
        try
        {
            write.abort();
        }
        catch (...)
        {
            /// The removal append did not land. `PreparedPartWrite` stays non-terminal, so its own
            /// destructor retries it; beyond that the durable backstop is the ref lane's wedge, exactly
            /// as for every other abandon path.
            tryLogCurrentException(getLogger("ContentAddressedMetadataStorage"),
                fmt::format("aborting the prepared relink of part {}", ref_name));
        }
    }

private:
    std::shared_ptr<Cas::CachedPartFolderAccess> access;
    Cas::PreparedPartWrite write;
    String ref_name;
};

}

/// TRUST MODEL: adopting a part from a peer-supplied manifest is exactly as trusted as an ordinary
/// ReplicatedMergeTree interserver part fetch. The interserver HTTP channel — not a per-blob ACL — is
/// the trust boundary: a malicious or MITM peer on that channel can already serve arbitrary part bytes
/// that the receiver adopts, in both the byte-streaming and the relink path. Table-level RBAC never
/// defended against a hostile peer, so relink-by-manifest adds no new trust surface. (See the retracted
/// umbrella "RBAC bypass" finding.)
CaRelinkPrepare ContentAddressedMetadataStorage::prepareAdoptFromManifest(
    const String & part_path, const String & manifest_bytes,
    std::unique_ptr<ICaPreparedRelink> & out)
{
    checkNotReadOnly("prepareAdoptFromManifest (interserver relink receiver)");
    /// Write class: publishing a receiver-local ref; throws typed on a Vanished disk, 668 while uncertain.
    checkOpAdmitted(CasOpClass::Write);

    /// Receiver side. Sender identity is non-authoritative: we ignore the decoded ManifestRef,
    /// root_namespace_id and payload_digest, and use ONLY the entries. We run a normal LOCAL build over
    /// the SHARED-pool blobs — adopted by hash via adoptEvidence, NO blob body transferred — then stage a
    /// FRESH receiver-local ManifestId in the receiver namespace and `precommitAdd` it.
    ///
    /// The promote does NOT happen here, and that split is the fix for the commit-before-release gap
    /// this function used to carry (codex-6). The sender's relink response is fire-and-forget: it
    /// releases the source part when `processQuery` returns, so if this `+1` were not yet durable while
    /// the source's now-`Outdated` part was collected, the receiver would commit a manifest whose blobs
    /// are gone. Publishing FIRST and asking SECOND closes THAT window: any removal of the source
    /// binding is appended strictly after this `+1` is durable in the ref log.
    ///
    /// It does NOT establish that every subsequent GC fold SEES the `+1`, and the ordering must not be
    /// read as if it did: `CaRelinkConfirmCore.tla` config `_sab_holeylist` shows a fold that misses it
    /// (BACKLOG `{#list-as-journal-dataloss-2026-07-25}`), which is why the caller-side taxonomy states
    /// plainly that a confirmed relink is not proven dangle-free (`DataPartsExchange.cpp`, "What a
    /// `yes` does NOT prove"). Durable-before-asking is necessary, not sufficient.
    ///
    /// Promotion trusts the adopted leaves via the durable manifest edge (no per-file HEAD/loadMeta
    /// probe); a genuinely-absent adopted blob is an invariant violation caught by fsck, not here — the
    /// ordinary ReplicatedMergeTree interserver trust.
    out.reset();

    /// The RECEIVER's own (namespace, ref) for the target path — never the sender's `root_namespace_id`,
    /// which is foreign to this server's path-mirroring identity. Routing rather than composing is what
    /// gives B66b its detached target for free: `TABLE/detached/DIR` folds onto `detached/DIR` in
    /// the table's OWN namespace, and a live target onto `DIR`, through the same `route` the reads use.
    /// A path that is not a part DIRECTORY here is a caller error, not a fallback: a byte fetch to the
    /// same place would be just as wrong, so it must not be quietly substituted. Shadow (FREEZE) paths
    /// are rejected for the same reason — a backup namespace is never a fetch target.
    auto p = Cas::parsePartFilePath(part_path);
    auto r = p ? route(*p) : std::nullopt;
    if (!p || !r || !p->backup_name.empty() || r->ref.empty() || !r->file.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Relink target '{}' does not address a content-addressed part directory of a live table", part_path);

    Cas::PartManifest decoded;
    try
    {
        decoded = Cas::decodePartManifest(manifest_bytes);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::CORRUPTED_DATA)
            throw;
        LOG_INFO(getLogger("ContentAddressedMetadataStorage"), "Relink of part {} not possible: transferred manifest failed to decode ({}); "
            "caller falls back to a byte fetch", part_path, e.message());
        return CaRelinkPrepare::MechanismFallbackAllowed;
    }

    auto access = partAccess();
    try
    {
        out = std::make_unique<PreparedRelinkOverPartWrite>(
            access, access->prepareEntries(r->refKey(), decoded.entries, Cas::ProvenanceOp::Attach), r->ref);
        return CaRelinkPrepare::Prepared;
    }
    catch (const Exception & e)
    {
        /// `ABORTED` or `NETWORK_ERROR` means a body-absent precommit, a precommit binding that is no
        /// longer the live owner, a ref conflict, or — since the transient-classifier round — this node's
        /// own mount fence refusing the work (`throwCasTransientUnavailable`): all retryable, and the
        /// sender still has the part, so the caller may fetch its bytes. `prepareEntries` abandons its own
        /// build before propagating, so nothing is staged and no `+1` is left behind. The fence case needs
        /// no special handling and stays fail-close by construction: the byte fetch it falls back to writes
        /// through the SAME fenced disk and is refused in turn, so the fallback cannot smuggle a write past
        /// a lost incarnation. Any other error propagates — an unclassified local failure is not evidence
        /// that a byte fetch would do better.
        if (e.code() != ErrorCodes::ABORTED && e.code() != ErrorCodes::NETWORK_ERROR)
            throw;
        LOG_INFO(getLogger("ContentAddressedMetadataStorage"), "Relink of part {} deferred (body-absent precommit, "
            "precommit not the live owner, or a ref conflict): {}; caller falls back to a byte fetch", part_path, e.message());
        return CaRelinkPrepare::MechanismFallbackAllowed;
    }
}

}
