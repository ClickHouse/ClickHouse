#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Interpreters/Context_fwd.h>
#include <base/defines.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
}

namespace DB::Cas
{

/// Selects where a content-addressed blob is staged before it is published.
///
/// `Local` is the default and preserves the existing local scratch-file path byte for byte; it does
/// not run the conditional-copy probe. `S3` is opt-in and can stream large blobs to an object-store
/// staging key. That path is usable only after the mount-time probe has demonstrated write-once
/// conditional copy semantics. If the backend does not enforce those semantics, callers must stay
/// on `Local`: an unconditional copy could overwrite a live content-addressed blob.
enum class StagingBackend
{
    Local,
    S3,
};

}

namespace DB
{

/// The SIX operation classes the central `ContentAddressedMetadataStorage::checkOpAdmitted` gate keys on
/// (rev.7 spec §1). Every public metadata/transaction entry declares its class so a single place decides
/// what happens per pool-lifecycle condition:
///   - `Factory`     — I/O-free construction / capability / introspection (`createTransaction`, the
///                     `getType`/`getPath`/capability getters, `gcHealth`, `lifecycleSnapshot`). NEVER
///                     gated: it works in every state. Such call sites do not invoke `checkOpAdmitted`.
///   - `Probe`       — existence / enumeration (`existsFile`/`existsDirectory`/`listDirectory`/
///                     `iterateDirectory`/`isDirectoryEmpty`/`getStorageObjectsIfExist`). Answers the
///                     truth: real while live, throws while uncertain, absent/empty once `Vanished`.
///   - `ContentRead` — resolving/serving bytes or per-file metadata (`getStorageObjects`/`getFileSize`/
///                     `getLastModified`/`getBlobViewPlan`/`readBlobPayload`/`getRelinkOffer`/
///                     `tryGetInManifestBytes`/`prepareInManifestRead`). Never silent-absent on `Vanished`
///                     — a loud typed error instead.
///   - `Write`       — create / write / rename, INCLUDING the previously-no-op sites and a publishing
///                     `commit`. Throws typed on `Vanished`.
///   - `Remove`      — ref/file removal (`removeRecursive`/`removeDirectory`/`unlinkFile`, and an
///                     empty/pure-remove `commit`). No-op SUCCESS on `Vanished` so a vanished-disk table's
///                     `DROP` completes.
///   - `Admin`       — `store()`-reaching admin + GC round entry points.
enum class CasOpClass : uint8_t
{
    Factory,
    Probe,
    ContentRead,
    Write,
    Remove,
    Admin,
};

/// The disposition `checkOpAdmitted` returns for the classes that have a truthful short-circuit answer in
/// the terminal `Vanished` state (Probe / Remove). `Proceed` means run the operation normally against the
/// live pool; `TruthAbsent` means answer the truth WITHOUT touching the pool (absent/empty for a Probe,
/// no-op success for a Remove). Every other outcome is a throw, so a caller only ever sees these two.
enum class CasOpAdmission : uint8_t
{
    Proceed,
    TruthAbsent,
};

/// A non-gated lifecycle snapshot of one content-addressed disk for `system.cas_mounts`
/// (rev.7 spec §7, [C5]-visibility). It is a Factory-class read (§1): I/O-free, no `store()`/`poolAccess`,
/// truthful in EVERY state — including a not-live / vanished pool the store()-class surface refuses, and a
/// null pool (before `startup` / after `shutdown`). This is what keeps a disappearing disk VISIBLE to the
/// operator instead of silently missing from the table.
///   - `lifecycle`      — one of `live` / `not_live` / `identity_lost` / `vanished` (a live pool), or
///                        `constructing` / `shutdown` (no pool published).
///   - `reason`         — the ENUM-CLEAN sub-state word: `replaced` / `forgotten` for a
///                        `vanished` pool, empty otherwise. Kept a small closed vocabulary so a downstream
///                        `lifecycle || '(' || reason || ')'` yields e.g. exactly `vanished(forgotten)` —
///                        the [D5] free text lives in `detail`, never here.
///   - `detail`         — the full [D5] reason text naming the actual failure (the replaced
///                        diagnosis, the timestamped `FORGET` message, or the identity-loss message); spec §1
///                        requires it appear verbatim in the snapshot. Empty while `live` and for a null pool.
///   - `since`          — wall-clock second the current non-`live` state was entered; 0 while `live`/no pool.
///   - `pool_id`        — last-known pool UUID (empty before the first `startup`); the disk stays
///                        introspectable under its identity even once the pool is gone.
///   - `server_root_id` — this server's node-local root id owning the mount slot.
struct CasLifecycleSnapshot
{
    String lifecycle;
    String reason;
    String detail;
    time_t since = 0;
    String pool_id;
    String server_root_id;
};

/// Adapts ClickHouse's `IMetadataStorage` path-based interface to the content-addressed pool.
///
/// The class owns the pool and its cached part-folder facade for the lifetime of an opened disk. It
/// parses disk paths, maps them to pool namespaces and references, and translates manifest entries
/// into `StoredObjects` or in-memory read sources. Transaction and GC entry points are exposed here
/// because disk lifecycle and system-query code own those operations; the CAS protocol itself stays
/// in `Cas::Pool`, `Cas::PartWriteTxn`, and `Cas::Gc`.
///
/// Namespace mapping:
///   live part      SERVER_ID/TABLE_UUID            ref = PART_DIR
///   detached part  SERVER_ID/TABLE_UUID            ref = detached/DETACHED_PART_DIR
///   FREEZE shadow  the LITERAL shadow table dir     ref = PART_DIR
///                  (shadow/BACKUP/store/U3/UUID or shadow/BACKUP/data/DB/TBL — bijective with
///                  the disk path for both Atomic and non-Atomic layouts, so the shadow tree
///                  enumerates from `Pool::listNamespaces("shadow/...")`)
///   generic files  SERVER_ID/_disk                  verbatim namespace files (access probes)
///
/// Small per-part files (`uuid.txt`, `metadata_version.txt`, `txn_version.txt`, `checksums.txt`, ...)
/// are inline-placement manifest tree entries, not sidecar objects. Their bytes are served through
/// `DiskObjectStorage::prepareRead`'s CA branch via `tryGetInManifestBytes`; `getStorageObjects` returns a
/// sized placeholder with an EMPTY remote key for them (any consumer bypassing the prepareRead branch
/// fails loudly, never reads wrong bytes).
class ContentAddressedMetadataStorage final : public IMetadataStorage, public IContentAddressedExchange
{
public:
    /// Constructs an unopened storage adapter. `settings_` carries every tunable that used to be a
    /// positional parameter (see `ContentAddressedSettings`) -- it is the single source of defaults, so
    /// the constructor itself declares none. `server_root_id`/`scratch_path` (the local-scratch
    /// directory used when a write buffer must spill before hashing and upload, independent of the
    /// object-storage key prefix) are read from `settings_` rather than taken as their own parameters. A
    /// non-null `context_` enables the background GC scheduler on the disk-factory path; tests may pass
    /// null to disable system-log integration and scheduling. `disk_name_` falls back to
    /// `storage_path_prefix_` when empty, exactly as before this constructor collapsed.
    ContentAddressedMetadataStorage(
        ObjectStoragePtr object_storage_,
        String storage_path_prefix_,
        String server_id_,
        String disk_name_,
        ContextPtr context_,
        const ContentAddressedSettings & settings_);

    /// Parses a `staging_backend` value (`local` | `s3`). Throws `BAD_ARGUMENTS` for an unrecognized
    /// value rather than silently selecting a backend.
    static Cas::StagingBackend parseStagingBackend(const std::string & value);

    /// Reads `staging_backend` from `config`, defaulting to `local`, and parses it. Kept only as a
    /// thin wrapper around the string-taking overload for callers that still hold a config reference.
    static Cas::StagingBackend parseStagingBackend(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    /// Parses a `part_folder_validate` value (`always` | `never` | `age <seconds>`). The `age` form
    /// accepts only a non-negative integer number of seconds; malformed input and unknown modes throw
    /// `BAD_ARGUMENTS` instead of silently selecting a policy.
    static Cas::PartFolderValidate parsePartFolderValidate(const std::string & value);

    /// Reads `part_folder_validate` from `config`, defaulting to `always`, and parses it. Kept only as
    /// a thin wrapper around the string-taking overload for callers that still hold a config reference.
    static Cas::PartFolderValidate parsePartFolderValidate(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    /// Returns the content-addressed metadata storage backing `disk`, or nullptr if `disk` is not
    /// content-addressed. Plain (non-object-storage) disks do not implement `getMetadataStorage` at
    /// all and throw `NOT_IMPLEMENTED`; that is treated as "not content-addressed" rather than
    /// propagated. Any other exception from `getMetadataStorage` is rethrown. Centralizes the
    /// detection lambda duplicated across `InterpreterSystemQuery` and
    /// `StorageSystemContentAddressedMounts`; callers there have not yet been migrated to it.
    static ContentAddressedMetadataStorage * tryFromDisk(const DiskPtr & disk);

    /// Runs one synchronous GC round for tests and diagnostics. If the scheduler is not running,
    /// this lazily creates one so repeated calls retain the same lease-observation history.
    void runOneGcRoundForTest();

    /// Runs one synchronous GC round on the caller's thread and emits Start and Finish rows to
    /// `system.cas_gc_log`. Throws `BAD_ARGUMENTS` when GC is disabled
    /// by read-only mode or configuration.
    Cas::RoundReport runGarbageCollectionRoundNow();

    /// Coalesce an administrative liveness hint into the existing periodic GC worker. If this disk has
    /// no running scheduler, the normal operator-visible retry path remains unchanged.
    void requestGcRoundSoon();

    /// Rebuilds the GC baseline for the `SYSTEM` disaster-recovery command. Each invocation uses a
    /// fresh GC identity because `rebuildBaseline` performs its own lease check. A refused rebuild
    /// (`report.performed == false`) writes nothing; the `SYSTEM` interpreter surfaces the refusal.
    /// Throws `BAD_ARGUMENTS` when GC is disabled by read-only mode or configuration.
    Cas::RebuildReport runGcRebuildNow(bool force) const;

    /// The `SYSTEM CAS FSCK` handler: a read-only, independent reachability audit.
    ///
    /// FSCK scans the LIVE running pool directly: Admin class -- it routes through
    /// `checkOpAdmitted(CasOpClass::Admin)` (refuses on a transient / `IdentityLost` / `Vanished` pool,
    /// consistent with `SYSTEM CAS GC RUN`), because an FSCK of a not-live disk is
    /// meaningless -- the operator has the snapshot / FORGET path. The scan tolerates concurrent writers:
    /// its ref-walk findings (missing manifest, dangling blob) are revalidated against a FRESH
    /// authoritative read before being reported, so a legitimate concurrent republish/drop + GC delete
    /// never surfaces as a phantom. Held under `lifecycle_mutex` for the whole scan so a concurrent
    /// lifecycle-control verb (FORGET / GC STOP / GC START) cannot race it.
    Cas::FsckReport runFsckNow(bool detail) const;

    /// Returns per-disk GC health for `system.cas_mounts`. Returns nullopt when this
    /// disk has no scheduler because GC is disabled, the disk is read-only, or startup has not run.
    /// Holds `gc_scheduler_mutex` for the entire call so a concurrent system-table query cannot
    /// observe a scheduler while `shutdown` destroys it.
    std::optional<Cas::CasGcScheduler::GcHealth> gcHealth() const;

    /// The non-gated lifecycle snapshot for `system.cas_mounts` (spec §7, Factory class):
    /// I/O-free, reachable in EVERY state — a live pool (forwards `Pool::lifecycleSnapshot`), a terminal
    /// pool the store()-class surface refuses, and a null pool (before `startup`/after `shutdown`, reported
    /// as `constructing`/`shutdown`). Never calls `store()`/`poolAccess()`, never touches the backend, so
    /// the very disk that vanished stays visible. Takes only a brief `pointer_mutex` snapshot of the pool
    /// pointer; the identity fields are read from immutable-after-startup storage members.
    CasLifecycleSnapshot lifecycleSnapshot() const;

    MetadataStorageType getType() const override { return MetadataStorageType::CAS; }
    const std::string & getPath() const override { return storage_path_full; }
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return read_only; }
    bool isContentAddressed() const override { return true; }


    /// Fail-close gate shared by every mutating entry point (transactions, GC round, GC rebuild,
    /// pool-member decommission): an observe-only (`<readonly>`) disk must reject them all.
    void checkNotReadOnly(std::string_view what) const;

    /// THE central six-class operation gate (rev.7 spec §1), consulted at EVERY public metadata/
    /// transaction entry (see the `CasOpClass` doc above and the method->class inventory at the top of
    /// the .cpp). Given `op`'s class it inspects the pool lifecycle ONCE and decides:
    ///   - `Live`                            -> `Proceed` for every class.
    ///   - null pool (the storage-level Constructing/ShutDown lifecycle -- before `startup`/after
    ///     `shutdown`) -> throws "not started" for EVERY class, `Probe` included: a storage that has
    ///     never published a pool (or torn one down) has no benign "absent" answer to give.
    ///   - `TransientNotLive` / `IdentityLost` -> throws 668 for every class but `Factory` (uncertain
    ///     backing; the sub-state distinction is surfaced in `system.cas_mounts`, not the
    ///     op error).
    ///   - `Vanished*` -> `Probe`/`Remove` answer `TruthAbsent` (absent-empty / no-op success);
    ///     `ContentRead`/`Write`/`Admin` throw the typed per-reason [D5] message (via
    ///     `Pool::throwIfLifecycleTerminal`, the single home of those strings).
    /// `Factory` is never passed here. Public because `ContentAddressedTransaction` funnels its own
    /// mutating entries through it.
    CasOpAdmission checkOpAdmitted(CasOpClass op) const;

    /// Content-addressed transactions are eager staging overlays: each mutating disk-transaction
    /// method reaches the metadata transaction immediately rather than entering the FIFO queue.
    bool transactionIsStagingOverlay() const override { return true; }
    bool supportsAtomicFileWrites() const override { return true; }
    bool supportsTransactionalMutableFiles() const override { return true; }
    bool areBlobPathsRandom() const override { return false; }
    uint32_t getHardlinkCount(const std::string &) const override { return 0; }

    /// Creates a write transaction bound to this storage. Throws `READONLY` before allocating one
    /// when the disk was opened read-only.
    MetadataTransactionPtr createTransaction() override;

    /// Opens the pool, validates its format and starts the optional GC scheduler. Read-only disks
    /// skip write probes and GC; failures in startup propagate. Runs exactly once, single-threaded,
    /// strictly before this object is exposed to any other thread (no other method can be called
    /// concurrently with it) -- TSA_NO_THREAD_SAFETY_ANALYSIS is deliberate here, not a bypass of a
    /// real risk: pointer_mutex/gc_scheduler_mutex exist to guard concurrent access AFTER startup,
    /// which is definitionally impossible during it.
    void startup() TSA_NO_THREAD_SAFETY_ANALYSIS override;
    /// Stops the GC scheduler before releasing the part-folder facade and pool. Their destruction is
    /// synchronized with the accessors and synchronous GC entry points.
    void shutdown() override;

    /// `SYSTEM CAS FORGET` handler (Task 10, spec §5): the operator force-Vanish. Drives the
    /// live pool to `Vanished(forgotten)` node-locally via `Pool::forgetDisk`'s fence-first protocol, and
    /// stops + joins the GC scheduler as part of it. Unlike the store()-class verbs, this must work on a
    /// NOT-live disk (a stuck transient / `IdentityLost` pool) — that is its purpose — so it reaches the
    /// pool DIRECTLY, never through `poolAccess()`/`checkOpAdmitted` (which refuse a not-live disk); it is a
    /// lifecycle verb, like the Factory class. FORGET is an operator ASSERTION, not an erasure proof: the
    /// resulting [D5] error message (which carries the decommission timestamp) says so. The disk stays
    /// registered; the six-class gate then answers the truth (Probe/Remove truth-absent, reads throw the
    /// [D5] message). Idempotent; a disk with no published pool is a no-op. Serialized by `lifecycle_mutex`
    /// (against FSCK / GC STOP / GC START) and `gc_scheduler_mutex` (against a synchronous round).
    void forgetDisk() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// `SYSTEM CAS GC STOP` handler (Task 11, spec §6): stops ONLY the background GC scheduler.
    /// The disk stays fully usable -- reads/writes are unaffected; this is granular operator control of the
    /// GC pacer alone, NOT a lifecycle transition. Unlike `forgetDisk` this is
    /// STOP-IN-PLACE: the scheduler object is RETAINED in the member (not detached/destroyed), so `gcHealth`
    /// keeps reading its (now stopped) state truthfully and a later `gcStart` re-enters the SAME instance
    /// with its `gc_id` + lease-observation history preserved. `stop()`
    /// joins the worker+heartbeat threads and clears the in-process leadership hint. Idempotent (a second
    /// STOP is a no-op); a no-op success when no scheduler exists (GC disabled / read-only / not started).
    /// Works on a not-live/Vanished disk too -- stopping GC on a sick disk is legitimate operator action, so
    /// this does NOT consult `checkOpAdmitted`. Serialized by `lifecycle_mutex` and `gc_scheduler_mutex`.
    void gcStop() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// `SYSTEM CAS GC START` handler (Task 11, spec §6): restarts the background GC scheduler
    /// stopped by `gcStop`, re-entering the SAME instance (its `start()` is re-enterable after a join).
    /// Leadership is NOT auto-restored -- the scheduler re-acquires the durable `gc/state` lease through the
    /// next round's normal acquisition. Idempotent (a no-op on a running scheduler). Unlike `gcStop`, it goes
    /// through the uniform GC gate (`checkOpAdmitted(Admin)`): it refuses on a transient / `IdentityLost` /
    /// `Vanished` pool with the typed error (668 / [D5]) and on a not-mounted disk -- restarting GC on a
    /// decommissioned/uncertain pool is meaningless and would only spin failing rounds. Serialized by
    /// `lifecycle_mutex` and `gc_scheduler_mutex`.
    void gcStart() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Test-only fault-injection hook. When set, `startup` invokes it right before it publishes
    /// `cas_store`/`part_access`/`gc_scheduler`/`pool_uuid`/`conditional_copy_supported` -- everything
    /// up to that point (opening the pool, building the part-folder facade, running the capability
    /// probe, starting the GC scheduler) has already happened into locals, so throwing here proves a
    /// late startup failure publishes nothing and a retry can still succeed. Left empty (a no-op) in
    /// production.
    std::function<void()> startup_fault_injection_for_test;

    /// Tests whether a path is represented by an inline manifest entry, namespace file, or loose
    /// mountpoint object.
    bool existsFile(const std::string & path) const override;
    /// Tests whether a path names a virtual part, table, shadow, or mirrored live-tree directory.
    bool existsDirectory(const std::string & path) const override;
    /// Tests both file and directory interpretations of a path.
    bool existsFileOrDirectory(const std::string & path) const override;
    /// Returns the logical payload size, excluding a blob envelope.
    uint64_t getFileSize(const std::string & path) const override;
    /// Returns the part publication time; other existing files use epoch time because their mtime is
    /// not retained by the content-addressed namespace.
    Poco::Timestamp getLastModified(const std::string & path) const override;
    /// Lists logical children of a virtual or mirrored directory.
    std::vector<std::string> listDirectory(const std::string & path) const override;
    /// Iterates over `listDirectory` results with each child joined to `path`.
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;
    /// Reports virtual part and projection directories as empty so removal unlinks their ref; table
    /// and container directories use their listing.
    bool isDirectoryEmpty(const std::string & path) const override;
    /// Maps a logical path to its storage object. Inline entries return a sized empty-key placeholder
    /// and must be served by the CA read branch.
    StoredObjects getStorageObjects(const std::string & path) const override;
    /// Performs one manifest lookup for part files instead of the inherited `existsFile` plus
    /// `getStorageObjects` sequence.
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    /// ==== `IContentAddressedExchange` (interserver relinking facade) ====
    const String & getPoolUUID() const override { return pool_uuid; }
    /// Routing predicate for the confirm action: this mount owns a namespace iff the namespace is rooted
    /// at ITS `server_root_id` (`liveNamespace` builds every live/detached namespace as
    /// `<server_root_id>/<mirrored table dir>`). Factory-class: I/O-free, ungated, never throws.
    bool ownsNamespace(const String & other_server_root_id, const String & root_namespace) const override;
    /// Gate 1 of the relink confirm, forwarded to the pool's ref ledger. Answers `Unknown` for an
    /// unparsable token and for a disk that has not started or has reached a terminal lifecycle.
    CasConfirmAnswer confirmExactRef(const String & root_namespace, const String & ref_name,
                                     const String & manifest_ref_text) const override;
    /// Returns the canonical encoded manifest for a committed part plus the confirm token minted from
    /// that same resolution, or nullopt when the path is not a committed CA part or the token cannot be
    /// minted. Missing or corrupt committed state propagates as an exception.
    std::optional<RelinkOffer> getRelinkOffer(const String & part_path) const override;
    /// Stages a peer-supplied manifest into this server's namespace without transferring blob bodies and
    /// hands back the durable-but-unpromoted handle. The receiver's `part_path` is routed exactly as any
    /// other part path, so a live target and a `detached/` one (B66b) differ only in the ref the router
    /// derives. Answers `MechanismFallbackAllowed` for a decode failure or a retryable staging failure so
    /// the caller can byte-fetch instead; read-only disks throw `READONLY`.
    CaRelinkPrepare prepareAdoptFromManifest(
        const String & part_path, const String & manifest_bytes,
        std::unique_ptr<ICaPreparedRelink> & out) override;

    /// ==== wiring-internal surface (the transaction + the disk's prepareRead CA branch) ====

    /// Returns a shared-ownership snapshot of the opened pool. Throws `INVALID_STATE` when no pool is
    /// published (before the first `startup`, or after `shutdown`). A thin wrapper over `poolAccess()`.
    Cas::PoolPtr store() const;
    /// Returns a shared-ownership snapshot of the cached part-folder facade. Throws `INVALID_STATE`
    /// under the same not-started condition as `store()`. Committed part-folder reads and mutations
    /// go through this facade so cache validation remains centralized. Returning a `shared_ptr`
    /// snapshot (not a reference) means the returned handle keeps the facade alive via its own
    /// refcount even if `shutdown` concurrently resets the member -- unlike a reference, which would
    /// dangle the instant `shutdown`'s reset runs. A thin wrapper over `poolAccess()`.
    std::shared_ptr<Cas::CachedPartFolderAccess> partAccess() const;
    const std::string & serverRootId() const { return server_root_id; }
    const std::string & scratchPath() const { return local_scratch_path; }
    /// Returns the configured staging backend. `Local` is the behavior-preserving default; callers
    /// must also check `conditionalCopySupported` before using S3 promotion.
    Cas::StagingBackend stagingBackend() const { return staging_backend; }
    /// Returns the mount-time conditional-copy capability result. It starts false and becomes true
    /// only after the backend proves write-once copy semantics, so S3 promotion fails closed.
    bool conditionalCopySupported() const { return conditional_copy_supported; }
    /// Returns the underlying object storage for an S3 staging writer. It is meaningful only when
    /// `stagingBackend` is `S3` and `conditionalCopySupported` is true.
    const ObjectStoragePtr & objectStorage() const { return object_storage; }
    /// Returns the physical prefix for this pool's writer-owned staging area. It is the same
    /// `pool_prefix/staging/server_root_id` subtree used by the capability probe; callers append a
    /// unique leaf and must not use the probe object itself.
    String stagingKeyPrefix() const;

    /// Bytes that live INSIDE pool metadata rather than as their own object: an Inline-placement
    /// manifest tree entry, or a verbatim namespace file. nullopt = the path is blob-backed (a real
    /// storage object).
    std::optional<String> tryGetInManifestBytes(const std::string & path) const;

    /// The CA read entry called by `DiskObjectStorage::prepareRead` before the generic
    /// storage-objects path: serves in-manifest bytes (mutable per-part files, inline entries,
    /// verbatim namespace files) from memory. Returns false when the path is not in-manifest.
    /// Declared on `IContentAddressedExchange` (the narrow seam `prepareRead` casts to); `BlobViewPlan`
    /// is likewise inherited from there.
    bool prepareInManifestRead(const std::string & path, const ReadSettings & settings, ReadPipeline & pipeline) const override;

    /// Resolves a blob-backed path to its physical object and payload window. Returns nullopt for
    /// in-manifest, loose, directory, or otherwise unresolved paths.
    std::optional<BlobViewPlan> getBlobViewPlan(const std::string & path) const override;

    /// Creates a seekable reader over one blob payload, excluding its envelope. Transactions use
    /// this for read-your-writes; committed reads use `getBlobViewPlan` and the normal pipeline.
    std::unique_ptr<ReadBufferFromFileBase> readBlobPayload(
        const Cas::BlobLocation & location, const std::string & path, const ReadSettings & settings) const;

    /// Maps a live table UUID to its pool namespace. Detached parts share that namespace and use
    /// `detached/`-prefixed references rather than a sibling namespace.
    Cas::RootNamespace liveNamespace(const std::string & table_uuid) const;
    /// Canonicalizes a literal shadow-table directory into the pool namespace used by freeze and
    /// unfreeze paths. A trailing slash is ignored.
    static Cas::RootNamespace shadowNamespace(const std::string & shadow_table_dir);

    /// The LIFE under which `ns`'s table-level namespace files — `format_version.txt` and the other
    /// verbatim files — must be read, or `nullopt` when there are none to read.
    ///
    /// It answers two things at once because they are one question. A dropped-and-not-recreated table
    /// (ref-table lifecycle durably `Removed`) is GONE for readers: its files must read as absent even
    /// while GC has not yet physically reclaimed them (namespace removal is deferred to GC), mirroring
    /// how its parts already vanish via the ref state. A never-born namespace is likewise empty. And a
    /// readable namespace's files live under ITS OWN incarnation (Stage B Task 4b), never under a
    /// previous life's — which is why the readable answer is a life rather than a `true`: a reader that
    /// has no life cannot form a key at all, so a previous life's surviving objects are unreachable by
    /// construction rather than by remembering to check something.
    std::optional<Cas::NamespaceLifeId> readableNamespaceFilesLife(const Cas::RootNamespace & ns) const;

    /// Returns the root prefix for mirrored live-tree objects. The persistent layout identity is
    /// `server_root_id`; `ServerUUID` remains only the mount-owner token.
    std::string serverPrefix() const;

    /// Enumerate the children of a GENERIC intermediate live-tree directory (the disk root "",
    /// `store`, the `store/<u3>` shard dir, or any loose-file container above a table dir) via a
    /// server-root-scoped mirrored S3 LIST of `roots/<server_root_id>/<path>/`. `@cas@`-suffixed table-dir
    /// segments are surfaced under their logical (unsuffixed) name. This is what makes top-down
    /// `clickhouse-disks` traversal of the live tree behave like a normal disk; concrete
    /// `store/<u3>/<uuid>/<part>/<file>` navigation is still served by the exact-shape branches.
    std::vector<std::string> listLiveTreeChildren(const std::string & path) const;
    /// Tests whether the server-root-scoped mirrored subtree has at least one child. The disk root is
    /// always considered present.
    bool liveTreeDirHasChildren(const std::string & path) const;

    /// Resolves one parsed path to its namespace, reference, and in-tree file. Detached paths are
    /// re-split here so their references remain in the table namespace with a `detached/` prefix;
    /// shadow paths map to a namespace derived from the literal shadow directory.
    struct Route
    {
        Cas::RootNamespace ns{""};
        /// empty => the path is the namespace's container dir. For a detached part this is
        /// `detached/<part>` (a ref inside the table namespace, not a separate namespace).
        std::string ref;
        std::string file;   /// empty => the path is the part dir itself

        /// The (ns, ref) identity subset — what the part-folder access layer keys on.
        Cas::PartRefKey refKey() const { return {ns, ref}; }
    };
    /// Converts a parsed path into the namespace/reference/file tuple used by the part-folder
    /// facade. Returns nullopt only when the parsed path cannot be routed.
    std::optional<Route> route(const Cas::PartFilePath & p) const;

    /// Returns full `detached/<part>` reference names in a namespace.
    std::vector<std::string> detachedRefNames(const Cas::RootNamespace & ns) const;

    /// Returns full `moving/<part>` staging-reference names in a namespace. Move recovery enumerates
    /// these names and removes entries left by an interrupted move.
    std::vector<std::string> movingRefNames(const Cas::RootNamespace & ns) const;

    /// `existsDirectory` and `listDirectory` use one fixed dispatch order to route a path through
    /// (shadow -> atomic-shard -> table-uuid -> part -> subdir -> generic), previously implemented
    /// twice and kept in sync by hand. `classifyDirectory` (private, below) computes it once; both
    /// callers then switch on the resulting shape. `DirShape` and `DirRoute` remain public only so
    /// `classifyDirectoryForTest` can expose the classification to wiring tests; the logic stays
    /// private.
    enum class DirShape
    {
        ShadowPart,
        ShadowTable,
        ShadowIntermediate,
        AtomicShard,
        TableDir,
        DetachedContainer,
        MovingContainer,
        PartDir,
        ProjectionDir,
        TableSubdir,
        GenericIntermediate,
    };

    struct DirRoute
    {
        /// Defaulted so a future classifyDirectory return path that forgets to set it fails safe
        /// (a defined shape) instead of switching on an indeterminate enum (UB). Matches the
        /// existing unreachable-fallthrough choice at the bottom of existsDirectory/listDirectory.
        DirShape shape = DirShape::GenericIntermediate;
        std::optional<Cas::PartFilePath> p;
        std::optional<Route> r;
        std::optional<std::string> uuid;
        std::optional<Cas::TableFilePath> tf;
        std::optional<std::string> projection_prefix;
    };

    /// Test-only accessor exposing the private directory classification so wiring tests can pin the
    /// dispatch order directly.
    DirRoute classifyDirectoryForTest(const std::string & path) const { return classifyDirectory(path); }

    /// Test seams for the EMPTY-PROOF RULE (Task 9, spec §1 [B3]). The counter is bumped on every
    /// authoritative pool-identity probe the empty-proof issues, so a test can assert it fires EXACTLY
    /// once per EMPTY `TableDir`/`DetachedContainer` enumeration and NEVER on the non-empty hot path,
    /// a deeper part-dir, or a terminal (`Vanished`) pool. `setEmptyProofProbeOverrideForTest` replaces
    /// the real backend probe so a test can inject a transport/permission fault (`Indeterminate`/
    /// `AccessDenied`) deterministically -- the storage builds its own `ObjectStorageBackend` internally,
    /// so a backend decorator cannot reach it otherwise. Both are inert in production.
    uint64_t emptyProofProbeCountForTest() const { return empty_proof_probe_count_for_test.load(); }
    void resetEmptyProofProbeCountForTest() { empty_proof_probe_count_for_test.store(0); }
    void setEmptyProofProbeOverrideForTest(std::function<Cas::SentinelProbeResult()> fn)
    {
        empty_proof_probe_override_for_test = std::move(fn);
    }

    /// Test-only seam (inert in production): invoked at each manual GC verb's FORGET-race juncture so a test
    /// can deterministically interleave a concurrent FORGET. For the round verbs
    /// (`runOneGcRoundForTest`/`runGarbageCollectionRoundNow`) it fires PRE-lock, in the admission->lock
    /// TOCTOU window (I-1: the call is admitted while `Live`, parks here until FORGET drives the pool
    /// `Vanished`, then hits the under-lock re-check). For `runGcRebuildNow` it fires WHILE the rebuild HOLDS
    /// `gc_scheduler_mutex` (I-2: the in-flight window a concurrent FORGET must serialize behind). Empty by
    /// default; production installs none.
    void setGcVerbAdmitWindowHookForTest(std::function<void()> fn) { gc_verb_admit_window_hook_for_test = std::move(fn); }

    /// Test-only fault-injection/hook seam for `ContentAddressedTransaction::publishStaging`'s
    /// promote/repoint call, keyed by the full `(ns, ref)` routed identity via `PartRefKey::cacheKey()`
    /// (mirrors `CasRefLedger::setRefPreCarveHookForTest`'s no-op-in-production shape) -- a bare ref
    /// name would misfire across a future multi-namespace fixture where two namespaces coincidentally
    /// share a ref name. `armPromoteFailureForTest` makes the NEXT `promoteBuild`/`repointRef` call for
    /// `key` throw instead of committing, modeling a transient promote-time backend failure.
    /// `setAfterPromoteHookForTest` installs a one-shot callback run synchronously immediately after a
    /// successful promote/repoint for `key` (before the caller's own post-commit bookkeeping), modeling
    /// a concurrent writer racing in right after this transaction's confirm -- e.g. repointing the same
    /// ref to a different manifest so a later rollback's `dropRefIfMatches` must see it changed.
    void armPromoteFailureForTest(const Cas::PartRefKey & key) { promote_failure_refs_for_test.insert(key.cacheKey()); }
    bool shouldFailPromoteForTest(const Cas::PartRefKey & key) const { return promote_failure_refs_for_test.contains(key.cacheKey()); }
    void setAfterPromoteHookForTest(const Cas::PartRefKey & key, std::function<void()> hook)
    {
        after_promote_hooks_for_test[key.cacheKey()] = std::move(hook);
    }
    /// Invokes and removes `key`'s one-shot hook, if any registered. A no-op when none is installed
    /// (the production hot path never installs one).
    void runAfterPromoteHookForTest(const Cas::PartRefKey & key)
    {
        auto it = after_promote_hooks_for_test.find(key.cacheKey());
        if (it == after_promote_hooks_for_test.end())
            return;
        auto hook = std::move(it->second);
        after_promote_hooks_for_test.erase(it);
        hook();
    }

private:
    const ObjectStoragePtr object_storage;
    const std::string storage_path_prefix;
    const std::string storage_path_full;
    const std::string server_id;
    const std::string server_root_id;
    const std::string disk_name;
    const std::string local_scratch_path;
    const ContextPtr context;

    const bool gc_enabled;
    const std::chrono::seconds gc_interval;
    const uint64_t deduplication_cache_bytes;            /// P1 known-present cache byte cap (0=off)
    const uint64_t deduplication_head_first_min_bytes;   /// P2 HEAD-before-PUT size threshold (0=off)
    const uint64_t gc_snapshot_generations_to_keep;  /// Number of GC snapshots retained (0 means keep all).
    const uint64_t gc_shards;                    /// Blob-hash-prefix reducer shard count, fixed at pool creation.
    const uint64_t manifest_sweep_list_budget_keys;
    const uint64_t manifest_sweep_delete_budget_keys;
    const uint64_t gc_round_graduation_budget;
    const uint64_t gc_round_redelete_budget;
    const uint64_t gc_round_sweep_namespace_budget;
    const uint64_t gc_round_sweep_recovery_op_budget;
    const uint64_t gc_round_ref_cleanup_budget;
    const uint64_t gc_round_prefix_wholesale_budget;
    const uint64_t gc_round_handoff_prefix_wholesale_budget;
    const uint64_t gc_round_outcome_entry_budget;
    /// GCS single-PUT budget for every token-producing write, conditional or not (generation-token
    /// stores only): threaded into the ObjectStorageBackend construction site in startup().
    /// Irrelevant on ETag stores (AWS et al).
    const uint64_t gcs_max_token_producing_put_bytes;
    /// Part-folder view cache settings. `cas_part_folder_cache_bytes == 0` disables retention.
    const uint64_t cas_part_folder_cache_bytes;
    const uint64_t cas_part_folder_cache_max_entries;
    const uint64_t cas_part_folder_cache_max_entry_bytes;
    /// Byte bound for the manifest decode cache in `Cas::Pool`. Zero disables decode caching.
    const uint64_t manifest_decode_cache_bytes;
    /// Bounded pool size for GC's per-hash freshness-metadata writes.
    const uint64_t gc_meta_pool_size;
    /// Configured staging backend; `Local` preserves the existing write path.
    const Cas::StagingBackend staging_backend;
    /// Blob content-hash function passed to `Cas::PoolConfig`.
    const Cas::BlobHashAlgo blob_hash_algo;
    /// Whether `blob_hash_algo` may be admitted into the pool's persisted `algos_used` set.
    const bool blob_hash_allow_new;
    /// Per-disk `<skip_access_check>` policy passed to `Cas::PoolConfig`.
    const bool skip_access_check;
    /// Policy controlling when retained part-folder views revalidate their manifest body.
    const Cas::PartFolderValidate part_folder_validate;
    /// Set by the mount-time conditional-copy capability probe — not const because the result is
    /// unavailable until startup.
    /// Defaults to false (fail-close): assumed unsupported until the probe proves otherwise.
    bool conditional_copy_supported = false;

    /// A single coherent snapshot of the pool and its cached part-folder facade, taken under ONE
    /// `pointer_mutex` acquisition (see `poolAccess()`) so no caller can observe `pool` from one mount
    /// generation and `part_access` from another -- the two used to be fetched by two separate calls
    /// to `store()`/`partAccess()` at some call sites, each taking its own `pointer_mutex` snapshot.
    struct PoolAccessSnapshot
    {
        Cas::PoolPtr pool;
        std::shared_ptr<Cas::CachedPartFolderAccess> part_access;
    };

    /// Set by startup (Pool::open is fail-closed; empty store == not started). shared_ptr so
    /// store()/partAccess() can return a by-value snapshot under `pointer_mutex` (see below) instead
    /// of a reference that could dangle across a concurrent `shutdown` reset.
    Cas::PoolPtr cas_store TSA_GUARDED_BY(pointer_mutex);
    /// The part-folder access facade: the normal path
    /// for committed part/projection reads and committed part-ref mutations. Constructed in
    /// startup right after Pool::open; reset in shutdown before cas_store. shared_ptr for the same
    /// snapshot-safety reason as cas_store.
    std::shared_ptr<Cas::CachedPartFolderAccess> part_access TSA_GUARDED_BY(pointer_mutex);
    String pool_uuid;
    /// The backend's incarnation-token dialect recorded at `startup` (see `openPoolView`'s `PoolView::
    /// native_token_type`) -- immutable afterwards. `startup` also hands it to the object storage as a
    /// pin, which is what refuses a reload that would flip the dialect under this live pool; the check
    /// belongs there because only the object storage knows the effective `http_client`.
    Cas::TokenType native_token_type = Cas::TokenType::ETag;
    /// shared_ptr so `runGarbageCollectionRoundNow`/`runOneGcRoundForTest` can take a snapshot under
    /// `pointer_mutex`, release it, and run the (long) round via the snapshot -- never holding
    /// `pointer_mutex` itself for the round's duration, so `gcHealth`/`store`/`partAccess` never
    /// block behind an in-flight round.
    std::shared_ptr<Cas::CasGcScheduler> gc_scheduler TSA_GUARDED_BY(pointer_mutex);
    /// Outermost lock, taken by the lifecycle-control verbs: the FSCK handler, `forgetDisk`, and
    /// `gcStop`/`gcStart`. Serializes them against each other. Lock order when nested locks are needed:
    /// `lifecycle_mutex` -> `gc_scheduler_mutex` -> `pointer_mutex`, never the reverse.
    mutable std::mutex lifecycle_mutex;
    /// Serializes ONE synchronous GC round at a time and makes `shutdown` wait for an in-flight round
    /// to finish cleanly (clean GC completion has priority over fast shutdown) -- held for the WHOLE
    /// round. Deliberately NOT the same mutex as `pointer_mutex` below: this one can be held for a
    /// long time, so nothing that only needs a brief pointer snapshot may share it.
    mutable std::mutex gc_scheduler_mutex;
    bool shutdown_called TSA_GUARDED_BY(gc_scheduler_mutex) = false;
    /// Guards ONLY reads/writes of `cas_store`/`part_access`/`gc_scheduler` themselves
    /// (snapshot, create-if-absent, reset) -- always held briefly. Lock ordering when more than one of
    /// these is needed (the round entry points, `shutdown`, and -- outermost -- `lifecycle_mutex`):
    /// `lifecycle_mutex` first (if held at all), then `gc_scheduler_mutex`, then `pointer_mutex`
    /// nested inside, never the reverse.
    mutable std::mutex pointer_mutex;
    /// Derived from object_storage->isReadOnly() at startup (the disk's <readonly> config). When set:
    /// the probe is skipped, no watermark, no GC scheduler, and the mutating surface fails closed.
    bool read_only = false;
    /// Joined in front of core keys for DIRECT object_storage reads. The Emulated (Local) backend
    /// maps bare pool keys under getCommonKeyPrefix; Native passes keys through - this member
    /// mirrors that rule so readBlobPayload reads exactly where the backend wrote ("" for Native).
    String physical_key_prefix;

    /// Adds the local-backend common prefix to a pool key when direct object-storage I/O requires
    /// the physical key; native backends use the key unchanged.
    String physicalKey(const String & key) const
    {
        if (physical_key_prefix.empty())
            return key;
        if (physical_key_prefix.back() == '/')
            return physical_key_prefix + key;
        return physical_key_prefix + "/" + key;
    }

    /// The one place that takes a `{pool, facade}` snapshot under a SINGLE `pointer_mutex`
    /// acquisition. Throws `INVALID_STATE` (via `throwStorageNotStarted`) when no pool is published
    /// (before the first `startup` or after `shutdown` -- the storage-level Constructing/ShutDown
    /// lifecycle), then refuses a terminal pool via `throwIfLifecycleTerminal`. `store()`/`partAccess()`
    /// are thin wrappers over this; every other caller that needs BOTH the pool and the facade for one
    /// logical operation must call this ONCE and use both fields from the same snapshot, rather than
    /// calling `store()` and `partAccess()` separately -- otherwise it could straddle a startup/shutdown
    /// that changes `cas_store`/`part_access`.
    PoolAccessSnapshot poolAccess() const;

    /// Builds and throws the `INVALID_STATE` "disk is not started" exception `poolAccess()`, the gate,
    /// and the synchronous GC round entry points throw when no pool is published -- the storage-level
    /// Constructing (before `startup`) / ShutDown (after `shutdown`) lifecycle. `pool_uuid` (written once
    /// at the end of a successful `startup`, never reset) distinguishes the two in the message, exactly
    /// as `lifecycleSnapshot()` reports `constructing`/`shutdown`. A normal, operator-facing refusal,
    /// never a `LOGICAL_ERROR` (which would abort under debug/ASan builds).
    [[noreturn]] void throwStorageNotStarted() const;

    /// EMPTY-PROOF RULE (rev.7 spec §1 [B3]): called by `listDirectory` when a `TableDir`/
    /// `DetachedContainer` enumeration on a NON-terminal (Live or read-only) pool is about to answer
    /// empty. "Empty at a table root" is exactly what a silently-erased backing looks like, and a
    /// read-only pool has no lease/observer to detect that erasure any other way (MergeTree skips both
    /// directory creation and the `format_version.txt` write on a read-only disk) -- enumeration is its
    /// ONLY line of defense against ATTACHing an empty table over an erased pool. So before answering
    /// empty, this confirms the pool identity object (`_pool_meta`) with an AUTHORITATIVE, UNCACHED
    /// probe: `Present` authorizes the empty answer (the pool is genuinely there and genuinely empty);
    /// `KeyAbsent`/`ContainerAbsent` throw the typed 668 "backing may be erased"; `AccessDenied`/
    /// `Indeterminate` throw the typed transient 668 (fail-closed, retryable). NEVER reached on a
    /// `Vanished` pool -- `checkOpAdmitted`'s `Probe`->`TruthAbsent` short-circuit answers truth-empty
    /// before any classification runs, so the terminal path never pays the probe. Cost: one extra backend
    /// HEAD per EMPTY table-root enumeration only (attach/load-time); the non-empty hot path is untouched.
    void confirmPoolIdentityForEmptyEnumeration(const std::string & path) const;

    /// A pool opened as a standalone, UNPUBLISHED view: never touches `cas_store`/`part_access`/
    /// `gc_scheduler` -- the caller owns it entirely and drops it when done.
    struct PoolView
    {
        Cas::PoolPtr pool;
        /// The direct-object-storage key prefix for this view's backend (see `physicalKey`'s own
        /// doc comment): populated for the Emulated (Local) backend, empty ("") for Native. Returned
        /// rather than written to the `physical_key_prefix` member so this helper stays side-effect-free
        /// and callable from a `const` method (`runFsckNow`).
        String physical_key_prefix;
        /// The resolved (bucket-relative, trailing-slash-trimmed) pool prefix passed into
        /// `Cas::PoolConfig::pool_prefix` -- `startup()`'s S3-staging capability probe below needs the
        /// SAME resolved value to build its own probe key, so it is returned here rather than
        /// recomputed a second time.
        String pool_prefix;
        /// The backend's native incarnation-token dialect (`Cas::ObjectStorageBackend::nativeTokenType`),
        /// captured while the concrete backend is still in scope. `startup()`'s S3-staging capability
        /// probe needs this same fact to decide whether the probe is meaningful at all, and reading it
        /// back out through `pool` would mean unwrapping the instrumentation decorator `Pool::open`
        /// wraps the backend in -- returned here instead so there is exactly one place that reads it.
        Cas::TokenType native_token_type = Cas::TokenType::ETag;
    };

    /// Builds the backend + `Cas::PoolConfig` and opens a pool exactly as `startup()` does. A read-only
    /// (`<readonly>`) disk opens with no write probe, no background watermark, and no GC scheduler. Never
    /// touches `cas_store`/`part_access`/`gc_scheduler`/`physical_key_prefix`/`pool_uuid`/
    /// `conditional_copy_supported`; `startup()` applies its own result to those members itself, in its
    /// single publish step.
    PoolView openPoolView() const;

    /// Classifies `path`'s directory shape by running the fixed dispatch order once (shadow ->
    /// atomic-shard -> table-uuid -> part -> subdir -> generic), including the part-branch
    /// fall-through when no sub-shape matches. Pure path classification — consults no lifecycle
    /// state (e.g. `readableNamespaceFilesLife`); `existsDirectory`/`listDirectory` apply that gate
    /// themselves in their per-shape arms, exactly as before this refactor.
    DirRoute classifyDirectory(const std::string & path) const;

    /// Build the GC round sink: the std::function the scheduler calls per Start/Finish. Captures the
    /// ContextPtr, converts the POD GcRoundLogRecord into a ContentAddressedGarbageCollectionLogElement,
    /// and appends it to the SystemLog (best-effort). Returns an empty sink when context is null.
    Cas::GcRoundLogger makeGcRoundLogger() const;

    /// Builds the per-event CAS audit sink: the `std::function` the pool calls on every
    /// content-addressed decision. Captures the ContextPtr, converts the decoupled Core POD
    /// `Cas::CasEvent` into a ContentAddressedLogElement, and appends it to the SystemLog
    /// (best-effort). Returns an empty sink when context is null (unit tests).
    Cas::CasEventSink makeCasEventSink() const;

    /// Backing state for the `*ForTest` promote fault-injection/hook seam declared above. Empty in
    /// production (no test ever arms them); consulted only by `ContentAddressedTransaction::publishStaging`.
    std::unordered_set<std::string> promote_failure_refs_for_test;
    std::unordered_map<std::string, std::function<void()>> after_promote_hooks_for_test;

    /// Backing state for the EMPTY-PROOF RULE `*ForTest` seams (Task 9), declared above. The counter is
    /// bumped on every empty-proof probe; the override, when set, replaces the real backend probe. Both
    /// are inert in production (no test ever sets the override; the counter is write-only there).
    mutable std::atomic<uint64_t> empty_proof_probe_count_for_test{0};
    std::function<Cas::SentinelProbeResult()> empty_proof_probe_override_for_test;

    /// Backing state for the `setGcVerbAdmitWindowHookForTest` seam declared above (the I-1/I-2 admission
    /// TOCTOU tests). Empty in production; a `const` GC verb reads it and calls the const-qualified
    /// `std::function::operator()`, so it needs no `mutable`.
    std::function<void()> gc_verb_admit_window_hook_for_test;
};

}
