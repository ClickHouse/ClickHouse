#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <base/types.h>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::Cas
{

/// Stable identity of a committed part or projection folder: its owning root namespace and
/// committed-ref name (for example, a part name or `detached/<part>`). The key is used for both
/// storage operations and retained-view indexing, so its equality and cache-key representations
/// must describe exactly the same ref.
struct PartRefKey
{
    Cas::RootNamespace ns{""};
    String ref;

    bool operator==(const PartRefKey & o) const { return ns.string() == o.ns.string() && ref == o.ref; }

    /// Canonical map key. '\0' cannot occur in namespace strings or ref names (both derive from
    /// disk paths), so the join is unambiguous even though refs may contain '/'.
    String cacheKey() const { return ns.string() + '\0' + ref; }
};

/// Exact record of what one commit-shaped primitive (`promoteBuild`/`repointRef`) durably committed,
/// derived INSIDE the primitive's own `appendRefOps` builder rather than read back with a separate
/// call afterward -- a post-read would race against a concurrent repoint of the SAME ref and could
/// observe someone else's commit instead of this caller's own. `created` distinguishes a first-time
/// bind (no prior committed row for `ref`) from a repoint of an already-committed one; `manifest_ref`
/// is the manifest now bound to `ref` (unchanged from before the call on a `repointRef` byte-equal
/// no-op). A caller that needs to roll back its own commit later can drop `ref` conditionally on this
/// exact `manifest_ref` (`dropRefIfMatches`) instead of unconditionally (`dropRef`), which would
/// remove whatever manifest currently occupies the name -- unsafe once a concurrent writer may have
/// repointed it since.
struct CommitOutcome
{
    RootNamespace ns;
    String ref;
    ManifestRef manifest_ref;
    bool created = false;
};

/// Read-freshness policy at the part-folder access boundary. The mutable-read-vs-write-evidence
/// distinction is carried by the METHOD, not a fourth value: mutable per-part reads call `resolve`
/// (no manifest involved); write-path source reads call `getView`, which under ForceFresh always
/// resolves fresh and bypasses the retained view.
enum class Freshness
{
    CachedForLoad,   /// repeated load-window reads; stale-tolerant resolve (allow_stale=true)
    ForceFresh,      /// mutable per-part reads and write-path source reads; resolve fresh
    StrictValidate,  /// fsck/debug: bypass retained views entirely; fresh resolve + validated read
};


/// Immutable snapshot of one resolved committed part/projection folder. Index-free: the decoder guarantees strictly
/// ascending canonical path order, so file lookup is a binary search and directory listing is a
/// contiguous range scan over the SHARED decode (`manifest` is the same object the Pool's
/// manifest cache holds). No I/O; never mutated after construction. All answers are pure functions
/// of the members. Every per-part file is an ordinary manifest tree entry, so a content change is
/// a manifest change through `repointRef`; comparing manifest IDs is therefore sufficient to detect
/// a stale retained view. The view never performs I/O or mutates the shared manifest.
class PartFolderView
{
public:
    /// Creates a view from a resolved ref and its validated shared manifest decode. The manifest
    /// must be non-null and its entries must be strictly ascending by canonical path; this is the
    /// ordering required by the binary-search and range-scan helpers.
    PartFolderView(PartRefKey key_, Cas::ManifestId manifest_id_, uint64_t manifest_size_,
                   std::shared_ptr<const Cas::PartManifest> manifest_);

    /// Joins a fresh `Resolved` with its validated shared decode.
    static std::shared_ptr<const PartFolderView> make(
        PartRefKey key, const Cas::Resolved & resolved,
        std::shared_ptr<const Cas::PartManifest> manifest);

    /// Recognizes a projection directory by its last path component, `.proj` or `.tmp_proj`, and
    /// returns the corresponding in-tree prefix. The input is the routed file path; unrelated paths
    /// return nullopt.
    static std::optional<std::string> projectionDirPrefix(const std::string & file);

    const PartRefKey & refKey() const { return key; }
    const Cas::ManifestId & manifestId() const { return manifest_id; }
    const std::shared_ptr<const Cas::PartManifest> & manifest() const { return manifest_body; }

    /// Finds an entry by canonical path using the manifest's sorted-entry invariant.
    const Cas::ManifestEntry * findFile(const String & path) const;
    /// Returns whether the manifest contains an entry at `path`.
    bool hasFile(const String & path) const;
    /// Returns the logical size of an inline or blob entry, or nullopt when absent.
    std::optional<uint64_t> fileSize(const String & path) const;
    /// Returns inline bytes for an inline entry, or nullopt for absent and blob entries.
    std::optional<String> inlineBytes(const String & path) const;
    /// Lists immediate child names below `dir_prefix`; the result is not required to be sorted.
    std::vector<String> listChildren(const String & dir_prefix) const;
    /// Returns whether any manifest entry lies below `dir_prefix`.
    bool hasDirectory(const String & dir_prefix) const;
    /// Estimates the retained-cache weight, conservatively including the encoded manifest size.
    size_t estimatedBytes() const;

private:
    PartRefKey key;
    Cas::ManifestId manifest_id;
    uint64_t manifest_size = 0;
    std::shared_ptr<const Cas::PartManifest> manifest_body;
};

}

namespace DB::Cas { class PartWriteTxn; }

namespace DB::Cas
{

class CachedPartFolderAccess;

/// A part write that has been staged and PRECOMMITTED but not yet promoted -- the durable-but-
/// unpromoted state made into an owned object rather than an interval inside one call
/// (spec §relink-handle). It exists because the relink confirm has to interpose between the receiver's
/// `+1` becoming durable and the promote, so that the source can be asked whether it still holds the
/// manifest before the receiver commits to it.
///
/// The handle OWNS an open `PartWriteTxn`, and DESTRUCTION IS NOT CLEANUP at the transaction level:
/// `~PartWriteTxn` only retires the build sequence, so a dropped precommit would keep a live-epoch
/// binding -- one the stale-precommit sweep (prior-epoch-scoped) never reclaims and GC never touches --
/// permanently retaining the manifest's blobs. Exactly one terminal operation is therefore owed:
/// `promote` (commit) or `abort` (append the exact precommit removal).
///
/// Getting that wrong is made impossible rather than merely discouraged:
///  - move-CONSTRUCT-only, and a move leaves the source terminal, so the duty is never held twice.
///    Move ASSIGNMENT is deleted: overwriting a handle that still owes a terminal has no correct
///    implementation. Discharging the duty first can FAIL (`abandon` appends through the ref lane, and
///    the lane can be wedged or fenced), and the assignment cannot report that -- so it would either
///    drop a cleanup owner permanently or refuse to complete an operation the language says cannot
///    fail. Nothing needs it: the one handle that travels (the interserver relink's) is move
///    CONSTRUCTED into place, and a contract that cannot be relied on is worse than no contract;
///  - an explicit terminal flag, set only once the underlying operation has actually completed, so a
///    second `promote`/`abort` is rejected with `LOGICAL_ERROR` instead of re-driving a dead
///    transaction -- while a terminal that FAILED (an append the caller may legitimately retry) leaves
///    the handle non-terminal;
///  - the destructor is the last-resort guard: a handle that reaches it non-terminal aborts
///    best-effort and logs, so a forgotten or exception-skipped terminal still appends the removal.
class PreparedPartWrite
{
public:
    PreparedPartWrite(const PreparedPartWrite &) = delete;
    PreparedPartWrite & operator=(const PreparedPartWrite &) = delete;
    PreparedPartWrite(PreparedPartWrite && other) noexcept;
    PreparedPartWrite & operator=(PreparedPartWrite && other) = delete;
    /// Best-effort abort of a handle that never reached a terminal state; never throws.
    ~PreparedPartWrite();

    /// Completes the write: the atomic precommit-to-committed owner move, plus the facade's cache
    /// invalidation. The handle records the commit INSIDE the allocation-free region that immediately
    /// follows the durable append, so nothing between "the ref is committed" and "this handle knows it"
    /// can throw. On a failure that is PROVEN to have committed nothing the build is abandoned (the
    /// catch-abandon-rethrow discipline the atomic `publishEntries` has always applied); the original
    /// error propagates in either case.
    CommitOutcome promote(bool allow_repoint = false);
    /// Durably abandons the write: appends the EXACT precommit removal so the manifest's `+1` is
    /// released. Propagates an append failure -- the caller may retry, and the destructor is the
    /// backstop if it does not.
    void abort();

    /// Whether the owed terminal operation has already completed. A moved-from handle is terminal.
    bool isTerminal() const { return terminal; }
    /// Whether a `promote` attempt reached the ref lane's append and did not come back with a verdict,
    /// i.e. the commit MAY be durable. Only meaningful after `promote` threw; false everywhere else,
    /// including on a promote that failed its pre-append validation (proof of the negative) and on a
    /// moved-from handle. A caller that treats a failed promote as "nothing was published" -- the
    /// interserver relink's byte fallback -- must consult this first: doing that after a commit that
    /// actually landed publishes the same part twice.
    bool commitIsUnresolved() const;
    /// The ref this write will commit to, and the manifest staged for it.
    const PartRefKey & refKey() const { return key; }
    const ManifestId & manifestId() const { return id; }

private:
    friend class CachedPartFolderAccess;
    PreparedPartWrite(CachedPartFolderAccess & owner_, PartWriteTxnPtr build_, PartRefKey key_, ManifestId id_);

    CachedPartFolderAccess * owner = nullptr;
    PartWriteTxnPtr build;
    PartRefKey key;
    ManifestId id;
    bool terminal = false;
};

/// Single facade for committed content-addressed part-folder access. Reads build immutable
/// `PartFolderView`s; committed-ref mutations are facade methods so cache invalidation is write-through
/// rather than a caller responsibility. A bounded retained-view map is consulted for `CachedForLoad`
/// and checked against every fresh ref resolve. `cache_bytes == 0` disables retention while preserving
/// the uncached read path. The facade is thread-safe and shared by all readers and transactions of one disk.
class CachedPartFolderAccess
{
public:
    /// Retention knobs. `cache_bytes == 0` (the unit-test
    /// default) disables retention entirely — the disk factory default is 64 MiB.
    struct CacheParams
    {
        uint64_t cache_bytes = 0;            /// 0 = retention disabled (unit-test default;
                                             /// the DISK default is 64 MiB, set in the factory)
        uint64_t max_entries = 10000;
        uint64_t max_entry_bytes = 16ULL << 20;
        /// The explain decision journal is test/log-only and its `recordDecision`
        /// path takes a per-disk global mutex and allocates on EVERY read. Off by default so the read
        /// hit path never pays for it; the disk factory / tests turn it on when they consult `explain`.
        bool explain_enabled = false;
    };

    /// `CacheParams params_ = {}` cannot be a default argument here — Clang's complete-class-
    /// context rule requires the enclosing class (`CachedPartFolderAccess`) to be complete before a
    /// nested class's (`CacheParams`) default member initializers can be evaluated, and a default
    /// argument written inside the class body is evaluated too early. Two overloads sidestep it; the
    /// single-arg form default-constructs `CacheParams` (retention disabled) out-of-line.
    explicit CachedPartFolderAccess(Cas::PoolPtr store_);
    CachedPartFolderAccess(Cas::PoolPtr store_, CacheParams params_);

    /// Resolves the ref and, when present, reads and validates its manifest into an immutable view.
    /// `nullptr` means the ref is absent. `ForceFresh` and `StrictValidate` bypass the retained view
    /// and read through the pool's manifest cache; a manifest is immutable per id, so a retained view
    /// can be stale only by naming a different manifest id, which the fresh resolve detects.
    std::shared_ptr<const PartFolderView> getView(const PartRefKey & key, Freshness freshness) const;

    /// Ref-only resolution (per-part reads, part-dir existence, publish stamps): no
    /// manifest is read. `CachedForLoad` = stale-tolerant; other modes force-fresh. `audit` defaults to
    /// `Emit` so every caller other than `getView` keeps emitting `RefResolve` unchanged; `getView`
    /// passes `Deferred` and re-emits the event itself once it knows whether a warm view-cache hit
    /// served the call without doing any real resolve work.
    std::optional<Cas::Resolved> resolve(const PartRefKey & key, Freshness freshness,
                                         Cas::ResolveAudit audit = Cas::ResolveAudit::Emit) const;
    bool existsRef(const PartRefKey & key, Freshness freshness) const;

    /// ==== committed part-ref writes ====
    /// Each primitive performs the protocol operation and owns the cache side effect:
    /// erase the affected view on success; on exception cache state is untouched — except
    /// dropRefBestEffort, which erases even on a swallowed failure: in its destructor/rollback
    /// context the ref's durable state is unknown, so dropping the view is the conservative
    /// direction). Committed-ref mutations anywhere else in wiring are style-check failures.

    /// Completes a staged transaction with the atomic owner move and invalidates the affected view.
    /// `allow_repoint` permits replacing a committed ref that names a different manifest. The returned
    /// `CommitOutcome` is exact: `created` is derived INSIDE `PartWriteTxn::promote`'s `appendRefOps`
    /// builder (the same in-closure-output pattern as that builder's own `repoint_old`), not read back
    /// afterward.
    ///
    /// `commit_recorded`, when supplied, is set to `true` inside the allocation-free region that
    /// immediately follows the durable append -- before the `CommitOutcome` is finished and before the
    /// cache invalidation, both of which allocate and may therefore throw. It exists so a caller
    /// holding a terminal duty (`PreparedPartWrite`) can record "committed" with nothing throwable in
    /// between; without it an allocation failure in the post-commit work lands in the caller's
    /// failed-promote handler with the ref already published.
    CommitOutcome promoteBuild(Cas::PartWriteTxn & build, const PartRefKey & key, UInt128 build_id,
                      const Cas::ManifestId & manifest_id, bool allow_repoint = false,
                      bool * commit_recorded = nullptr);
    /// The first half of the committed-publish sequence: adopt evidence over `entries`, stage a fresh
    /// manifest, and precommit it -- then STOP. The receiver's `+1` is durable, the promote is deferred
    /// until the caller has proven the source still holds the manifest (spec §relink-handle). The
    /// returned handle OWNS the open transaction and must be either `promote`d or `abort`ed --
    /// destruction alone is not cleanup at the transaction level (`~PartWriteTxn` only retires the
    /// build sequence), which is why the handle's own destructor aborts as a last resort. A failure
    /// inside `prepareEntries` itself abandons the build before propagating, so no handle is returned
    /// and no precommit is leaked.
    PreparedPartWrite prepareEntries(const PartRefKey & dst, const std::vector<Cas::ManifestEntry> & entries,
                        Cas::ProvenanceOp op);
    /// Performs the shared committed-publish sequence: adopt evidence over
    /// `entries`, stage a fresh manifest, precommit it, and promote it. The new manifest is fully
    /// prepared before the committed ref is moved. Returns `promoteBuild`'s exact `CommitOutcome`.
    /// Implemented as `prepareEntries` immediately followed by `promote`, so the atomic callers and the
    /// confirm-interposed relink path share one protocol sequence.
    CommitOutcome publishEntries(const PartRefKey & dst, const std::vector<Cas::ManifestEntry> & entries,
                        Cas::ProvenanceOp op, bool allow_repoint = false);
    /// Moves a committed ref by publishing the source entries at `dst` and then dropping `src`.
    /// Returns false when the source is absent; a pre-existing destination with different content
    /// is rejected rather than silently discarding the source.
    bool republishRef(const PartRefKey & src, const PartRefKey & dst);
    /// Republishes an already committed part with `entries` (for a standalone write or removal):
    /// republishes `key`'s manifest with `entries`. Byte-equal candidate (same decoded entries as the
    /// currently committed manifest) is a ZERO-pool-mutation no-op, returns false. Otherwise republishes
    /// a byte-equal candidate returns false without pool mutation; an effective repoint publishes
    /// through `publishEntries(allow_repoint=true)`, emits the repoint audit signals, invalidates the
    /// cached view, and returns the exact `CommitOutcome` (`created` is false on both the byte-equal
    /// no-op path -- `manifest_ref` names the manifest ALREADY committed, unchanged -- and on an
    /// effective repoint of an existing ref). `key` must already resolve.
    CommitOutcome repointRef(const PartRefKey & key, std::vector<Cas::ManifestEntry> entries, Cas::ProvenanceOp op);
    /// Drops a committed ref and invalidates its retained view after the drop succeeds.
    void dropRef(const PartRefKey & key);
    /// Idempotent removal: absent ref is success; a drop racing between resolve and the shard
    /// re-read (FILE_DOESNT_EXIST) is success too — the removal unit is replay-safe.
    void dropRefIfPresent(const PartRefKey & key);
    /// Best-effort destructor/rollback cleanup: never throws, logs failure, and relies on GC to reclaim
    /// lingering debris. The view is invalidated even when the durable ref state becomes unknown.
    void dropRefBestEffort(const PartRefKey & key) noexcept;
    /// Conditional rollback drop: removes `key`'s committed ref ONLY if its CURRENT committed manifest
    /// binding equals `expected` (typically the `manifest_ref` from a `CommitOutcome` this caller
    /// itself just produced). A ref repointed by someone else since then -- or already absent -- is
    /// left untouched; the caller's stale rollback attempt must never clobber a newer commit. Reads
    /// the committed binding and emits the removal op inside ONE `appendRefOps` builder (mirrors
    /// `dropRef`'s underlying protocol call, guarded by the equality check inside the same closure).
    /// `noexcept`: best-effort rollback context, like `dropRefBestEffort` -- swallows and logs a
    /// failure rather than propagating it. Returns whether it actually removed the ref.
    bool dropRefIfMatches(const PartRefKey & key, const ManifestRef & expected) noexcept;
    /// Drops all refs in a namespace and removes every retained view belonging to that namespace.
    void dropNamespace(const Cas::RootNamespace & ns);

    /// ==== diagnostics ====
    enum class LastDecision : uint8_t
    { Hit, Miss, OversizedBypass, StrictBypass, ForceFreshRead, Invalidated };
    struct ExplainResult
    {
        bool retained = false;             /// whether the last-served view is currently retained
        LastDecision last_decision = LastDecision::Miss;
        String manifest_ref;               /// manifestRefDebugString of the last-served view
        size_t estimated_bytes = 0;
    };
    /// Returns the test/log-only decision record for `key`; an absent key yields the default result.
    ExplainResult explain(const PartRefKey & key) const;

    /// Test-only seam (nothing installs it in production): fires in `promoteBuild` immediately after
    /// the commit has been recorded and before the throwable post-commit work (`CommitOutcome`
    /// assembly's copies, `eraseView`). A test uses it to model an allocation failure in exactly that
    /// window and assert that the caller's terminal duty is already discharged.
    void setPostCommitProbeForTest(std::function<void()> fn) { post_commit_probe_for_test = std::move(fn); }
    /// Test-only: number of entries in the decision journal (0 whenever explain is disabled).
    size_t explainJournalSizeForTest() const;

private:
    Cas::PoolPtr store;
    CacheParams params;

    /// Supplies the conservative encoded-manifest weight used by `CacheBase` for eviction decisions.
    struct ViewWeight
    {
        size_t operator()(const PartFolderView & v) const { return v.estimatedBytes(); }
    };
    using ViewCache = CacheBase<String, PartFolderView, std::hash<String>, ViewWeight>;

    /// nullptr <=> retention disabled (cache_bytes == 0): same call graph, no retained map.
    std::unique_ptr<ViewCache> view_cache;

    /// Single-flight per PartRefKey for the build path: concurrent cold builders of the same key
    /// share ONE readManifestShared. NEVER held across I/O — the map only hands out futures.
    mutable std::mutex inflight_mutex;
    mutable std::unordered_map<String, std::shared_future<std::shared_ptr<const PartFolderView>>> inflight;

    /// Reads a manifest and constructs a view. Cold `CachedForLoad` builds are single-flight per key;
    /// fresh modes perform their own read.
    std::shared_ptr<const PartFolderView> buildView(
        const PartRefKey & key, const Cas::Resolved & resolved, Freshness freshness) const;
    /// Removes a retained view and records the invalidation for diagnostics.
    void eraseView(const PartRefKey & key);
    /// Emits the same `RefResolve` `CasEvent` `CasRefLedger::resolveRef` would have emitted for
    /// `resolved`, a no-op when no sink is installed. Used by `getView`, which defers the emit from its
    /// own `resolve(..., ResolveAudit::Deferred)` call so it can skip it on a warm view-cache hit that
    /// served without any real resolve work.
    void emitResolveEvent(const PartRefKey & key, const Cas::Resolved & resolved) const;

    /// Decision journal for `explain` (test/log-only). Bounded by wholesale
    /// clear — debug state, never consulted by the read/write paths.
    static constexpr size_t EXPLAIN_MAX_ENTRIES = 10000;
    mutable std::mutex explain_mutex;
    mutable std::unordered_map<String, ExplainResult> explain_map;
    /// Records the latest diagnostic decision without affecting read or write behavior.
    void recordDecision(const String & cache_key, LastDecision decision,
                        const PartFolderView * view, bool retained) const;

    /// See `setPostCommitProbeForTest`. Empty in production.
    std::function<void()> post_commit_probe_for_test;
};

}
