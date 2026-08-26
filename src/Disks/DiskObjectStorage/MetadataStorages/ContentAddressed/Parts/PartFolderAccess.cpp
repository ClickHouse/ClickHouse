#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Common/DateLUT.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}
}

namespace ProfileEvents
{
    extern const Event CASPartFolderViewHits;
    extern const Event CASPartFolderViewValidationMismatches;
    extern const Event CASPartFolderViewMisses;
    extern const Event CASPartFolderViewOversizedBypasses;
    extern const Event CASPartFolderViewInvalidations;
    extern const Event CASRefRollbackBestEffortDropFailed;
    extern const Event CASPartFolderValidateSkipped;
    extern const Event CASRefRepoint;
}

namespace CurrentMetrics
{
    extern const Metric CASPartFolderCacheBytes;
    extern const Metric CASPartFolderCacheEntries;
}

namespace DB::Cas
{

PartFolderView::PartFolderView(PartRefKey key_, Cas::ManifestId manifest_id_, uint64_t manifest_size_,
                               std::shared_ptr<const Cas::PartManifest> manifest_, uint64_t validated_at_ms_)
    : key(std::move(key_))
    , manifest_id(std::move(manifest_id_))
    , manifest_size(manifest_size_)
    , manifest_body(std::move(manifest_))
    , validated_at_ms(validated_at_ms_)
{
    chassert(manifest_body);
    /// The binary-search contract: entries must be strictly ascending by `path` (sorted and unique) —
    /// `decodePartManifest` enforces exactly this for every decoded body, and `findEntry`'s binary
    /// search assumes uniqueness. `adjacent_find` with `!(a.path < b.path)` flags any adjacent pair
    /// that is out-of-order OR duplicate (stronger than `is_sorted`, which permits duplicates); a
    /// hand-constructed manifest (tests) must honor it too.
    chassert(std::adjacent_find(manifest_body->entries.begin(), manifest_body->entries.end(),
        [](const Cas::ManifestEntry & a, const Cas::ManifestEntry & b) { return !(a.path < b.path); })
        == manifest_body->entries.end());
}

std::shared_ptr<const PartFolderView> PartFolderView::make(
    PartRefKey key, const Cas::Resolved & resolved, std::shared_ptr<const Cas::PartManifest> manifest,
    uint64_t validated_at_ms)
{
    return std::make_shared<const PartFolderView>(
        std::move(key), resolved.manifest_id, resolved.manifest_size,
        std::move(manifest), validated_at_ms);
}

std::optional<std::string> PartFolderView::projectionDirPrefix(const std::string & file)
{
    if (file.empty())
        return std::nullopt;
    const auto last_slash = file.find_last_of('/');
    const std::string_view last_component
        = last_slash == std::string::npos ? std::string_view(file) : std::string_view(file).substr(last_slash + 1);
    if (last_component.ends_with(".proj") || last_component.ends_with(".tmp_proj"))
        return file + "/";
    return std::nullopt;
}

const Cas::ManifestEntry * PartFolderView::findFile(const String & path) const
{
    return Cas::findEntry(manifest_body->entries, path);
}

bool PartFolderView::hasFile(const String & path) const
{
    return findFile(path) != nullptr;
}

std::optional<uint64_t> PartFolderView::fileSize(const String & path) const
{
    if (const auto * e = findFile(path))
        return e->size();
    return std::nullopt;
}

std::optional<String> PartFolderView::inlineBytes(const String & path) const
{
    const auto * e = findFile(path);
    if (e && e->placement == Cas::EntryPlacement::Inline)
        return e->inline_bytes;
    return std::nullopt;
}

std::vector<String> PartFolderView::listChildren(const String & dir_prefix) const
{
    /// Collapse each entry to its first child component. Projection folders are structurally flat in
    /// `MergeTree`, so this produces the same names as the old projection-specific path handling while
    /// keeping one directory-listing rule for all manifest folders.
    std::unordered_set<String> names;
    auto add = [&](const String & full)
    {
        if (!full.starts_with(dir_prefix) || full.size() <= dir_prefix.size())
            return;
        const std::string_view rest = std::string_view(full).substr(dir_prefix.size());
        const auto slash = rest.find('/');
        names.emplace(slash == std::string_view::npos ? rest : rest.substr(0, slash));
    };
    const auto [first, last] = Cas::entryRange(manifest_body->entries, dir_prefix);
    for (const auto * e = first; e != last; ++e)
        add(e->path);
    return {std::make_move_iterator(names.begin()), std::make_move_iterator(names.end())};
}

bool PartFolderView::hasDirectory(const String & dir_prefix) const
{
    const auto [first, last] = Cas::entryRange(manifest_body->entries, dir_prefix);
    return first != last;
}

size_t PartFolderView::estimatedBytes() const
{
    /// Conservative cache weight: fixed overhead plus `manifest_size`. This deliberately over-counts
    /// the shared decode, which is safe because eviction should happen before the budget is exceeded.
    return 256 + manifest_size;
}

CachedPartFolderAccess::CachedPartFolderAccess(Cas::PoolPtr store_)
    : CachedPartFolderAccess(std::move(store_), CacheParams{})
{
}

CachedPartFolderAccess::CachedPartFolderAccess(Cas::PoolPtr store_, CacheParams params_, std::function<uint64_t()> now_ms_fn_)
    : store(std::move(store_)), params(params_), now_ms_fn(std::move(now_ms_fn_))
{
    if (!now_ms_fn)
        now_ms_fn = []() -> uint64_t { return timeInMilliseconds(std::chrono::system_clock::now()); };
    if (params.cache_bytes > 0)
        view_cache = std::make_unique<ViewCache>(
            "LRU", CurrentMetrics::CASPartFolderCacheBytes, CurrentMetrics::CASPartFolderCacheEntries,
            params.cache_bytes, params.max_entries, ViewCache::DEFAULT_SIZE_RATIO);
}

std::shared_ptr<const PartFolderView>
CachedPartFolderAccess::getView(const PartRefKey & key, Freshness freshness) const
{
    /// Resolve first on every access. Absence is never retained, and the same ref-resolution result
    /// supplies the manifest ID used to validate a retained view. The emit is deferred: a warm
    /// `CachedForLoad` hit below serves the call without doing any real resolve work worth auditing, so
    /// this call site decides itself, per path, whether to re-emit the identical `RefResolve` event.
    auto resolved = resolve(key, freshness, Cas::ResolveAudit::Deferred);
    if (!resolved)
        return nullptr;

    /// Reuse one canonical key for the retained view and the optional diagnostic journal.
    const String cache_key = key.cacheKey();

    /// Retained views serve `CachedForLoad` directly only after their manifest ID matches the fresh
    /// resolve. `ForceFresh` must re-prove the manifest body unless the configured validation policy
    /// explicitly permits a recent retained view; a fresh ref resolve proves ref currency, not body existence.
    if (freshness == Freshness::CachedForLoad && view_cache)
    {
        if (auto cached = view_cache->get(cache_key))
        {
            if (cached->manifestId() == resolved->manifest_id)
            {
                /// The warm hit: the retained view already reflects this exact manifest, so this access
                /// did no real resolve work beyond a cache lookup — no `RefResolve` audit row for it.
                ProfileEvents::increment(ProfileEvents::CASPartFolderViewHits);
                recordDecision(cache_key, LastDecision::Hit, cached.get(), /*retained=*/true);
                return cached;
            }
            ProfileEvents::increment(ProfileEvents::CASPartFolderViewValidationMismatches);
            /// Rebuild below; the stale entry is superseded by the new view when retention is enabled.
        }
    }

    /// With a non-`Always` validation policy, `ForceFresh` may serve a retained view without another
    /// body HEAD when its manifest ID still matches and its validation timestamp is within the age
    /// policy. `StrictValidate` bypasses retention. A manifest-ID mismatch always rebuilds, because all
    /// part content is represented by the manifest.
    if (freshness == Freshness::ForceFresh && view_cache && params.validate.mode != PartFolderValidate::Mode::Always)
    {
        if (auto cached = view_cache->get(cache_key);
            cached && cached->manifestId() == resolved->manifest_id)
        {
            const bool fresh_enough = params.validate.mode == PartFolderValidate::Mode::Never
                || (now_ms_fn() - cached->validatedAtMs()) < params.validate.age_seconds * 1000ULL;
            if (fresh_enough)
            {
                ProfileEvents::increment(ProfileEvents::CASPartFolderViewHits);
                ProfileEvents::increment(ProfileEvents::CASPartFolderValidateSkipped);
                recordDecision(cache_key, LastDecision::Hit, cached.get(), /*retained=*/true);
                emitResolveEvent(key, *resolved);
                return cached;
            }
        }
    }

    auto view = buildView(key, *resolved, freshness);

    /// Retain eligible views. `StrictValidate` never populates the cache, and oversized views are
    /// served but not retained.
    /// `oversized` is tracked separately from `retained`: with retention disabled (`view_cache ==
    /// nullptr`), `retained` is also false, but that is an ordinary disabled-mode miss rather than
    /// an oversized bypass.
    bool retained = false;
    bool oversized = false;
    if (freshness != Freshness::StrictValidate && view_cache)
    {
        if (view->estimatedBytes() <= params.max_entry_bytes)
        {
            /// CacheBase stores mutable pointers; views are logically const (never mutated).
            view_cache->set(cache_key, std::const_pointer_cast<PartFolderView>(view));
            retained = true;
        }
        else
        {
            oversized = true;
            ProfileEvents::increment(ProfileEvents::CASPartFolderViewOversizedBypasses);
        }
    }
    ProfileEvents::increment(ProfileEvents::CASPartFolderViewMisses);
    recordDecision(cache_key,
        freshness == Freshness::CachedForLoad ? (oversized ? LastDecision::OversizedBypass : LastDecision::Miss)
        : freshness == Freshness::ForceFresh  ? LastDecision::ForceFreshRead
                                              : LastDecision::StrictBypass,
        view.get(), retained);
    emitResolveEvent(key, *resolved);
    return view;
}

void CachedPartFolderAccess::emitResolveEvent(const PartRefKey & key, const Cas::Resolved & resolved) const
{
    /// Mirrors `CasRefLedger::resolveRef`'s own (deferred-here) emit exactly: same event type and
    /// fields, built from the `Resolved` this call already holds.
    Cas::EventEmitter{*store}.emit([&](Cas::CasEvent & e)
    {
        e.type = Cas::CasEventType::RefResolve;
        e.namespace_ = key.ns.string();
        e.ref_name = key.ref;
        e.object_kind = Cas::CasEventObjectKind::Manifest;
        e.object_hash = Cas::manifestRefDebugString(resolved.manifest_id.ref);
        e.outcome = "resolved";
        e.reason = "read-side resolve of a ref to its part manifest";
    });
}

std::shared_ptr<const PartFolderView> CachedPartFolderAccess::buildView(
    const PartRefKey & key, const Cas::Resolved & resolved, Freshness freshness) const
{
    /// Fresh modes do not coalesce: each `ForceFresh`/`StrictValidate` call owns its mandatory HEAD.
    /// Only cold `CachedForLoad` builds use single-flight.
    if (freshness != Freshness::CachedForLoad)
        return PartFolderView::make(key, resolved, store->readManifestShared(resolved.manifest_id), now_ms_fn());

    std::promise<std::shared_ptr<const PartFolderView>> promise;
    std::shared_future<std::shared_ptr<const PartFolderView>> future;
    bool leader = false;
    {
        std::lock_guard lock(inflight_mutex);
        if (auto it = inflight.find(key.cacheKey()); it != inflight.end())
            future = it->second;                          /// Follower: share the leader's build.
        else
        {
            leader = true;
            future = promise.get_future().share();
            inflight.emplace(key.cacheKey(), future);
        }
    }
    if (!leader)
        return future.get();                              /// Rethrows the leader's exception, if any.

    SCOPE_EXIT({
        std::lock_guard lock(inflight_mutex);
        inflight.erase(key.cacheKey());
    });
    try
    {
        auto view = PartFolderView::make(key, resolved, store->readManifestShared(resolved.manifest_id), now_ms_fn());
        promise.set_value(view);
        return view;
    }
    catch (...)
    {
        promise.set_exception(std::current_exception());  /// Followers see the leader's exception.
        throw;
    }
}

void CachedPartFolderAccess::eraseView(const PartRefKey & key)
{
    const String cache_key = key.cacheKey();
    if (view_cache)
        view_cache->remove(cache_key);
    ProfileEvents::increment(ProfileEvents::CASPartFolderViewInvalidations);
    recordDecision(cache_key, LastDecision::Invalidated, nullptr, /*retained=*/false);
}

std::optional<Cas::Resolved>
CachedPartFolderAccess::resolve(const PartRefKey & key, Freshness freshness, Cas::ResolveAudit audit) const
{
    return store->resolveRef(key.ns, key.ref, /*allow_stale=*/freshness == Freshness::CachedForLoad, audit);
}

bool CachedPartFolderAccess::existsRef(const PartRefKey & key, Freshness freshness) const
{
    return resolve(key, freshness).has_value();
}

Cas::CommitOutcome CachedPartFolderAccess::promoteBuild(Cas::PartWriteTxn & build, const PartRefKey & key, UInt128 build_id,
                                          const Cas::ManifestId & manifest_id, bool allow_repoint,
                                          bool * commit_recorded)
{
    /// `build.promote` derives `created` INSIDE its own `appendRefOps` builder (the same in-closure
    /// pattern as that builder's `repoint_old`) and returns it the instant the append confirms.
    ///
    /// The outcome's STRINGS are copied BEFORE the append, so the only thing the post-durable region
    /// has left to do is store a bool -- which makes that region allocation-free, hence non-throwing.
    /// This is Part A's rule applied one layer up: nothing after a durable commit may throw before the
    /// caller has recorded it. Assembling the outcome afterwards (as this did) put two `String` copies
    /// and a cache invalidation between "the ref is committed" and "the handle knows", so a
    /// `MEMORY_LIMIT_EXCEEDED` there landed in the caller's failed-promote handler -- which abandons
    /// the build and, one layer up again, reports a byte fallback for a relink that already published.
    Cas::CommitOutcome outcome{key.ns, key.ref, manifest_id.ref, /*created=*/false};

    const bool created = build.promote(key.ns, key.ref, build_id, manifest_id, allow_repoint);

    /// POST-DURABLE REGION. Two plain stores, no allocation, no call that can fail.
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        outcome.created = created;
        if (commit_recorded)
            *commit_recorded = true;
    }

    /// Test-only (see `setPostCommitProbeForTest`): models an allocation failure in the throwable
    /// post-commit work below. It fires AFTER the region above, because the property under test is that
    /// the commit is already recorded by the time anything here can throw.
    if (post_commit_probe_for_test)
        post_commit_probe_for_test();

    eraseView(key);
    return outcome;
}

namespace
{

/// A failed publish must not leak a live-epoch precommit binding: only `abandon` removes it (the build
/// destructor merely retires the build seq; the stale-precommit sweep is prior-epoch-scoped and GC
/// never touches a live precommit). `abandon` may itself fail on the same broken backend -- log and let
/// whatever error the caller is already carrying stay primary. Returns whether the abandon completed,
/// so a caller tracking a terminal state can tell a finished transaction from one still owing cleanup
/// (`PartWriteTxn::abandon` stays retryable after an append failure).
bool abandonBuildBestEffort(Cas::PartWriteTxn & build, const char * context) noexcept
{
    try
    {
        build.abandon();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CachedPartFolderAccess"), context);
        return false;
    }
}

}

PreparedPartWrite::PreparedPartWrite(CachedPartFolderAccess & owner_, PartWriteTxnPtr build_,
                                     PartRefKey key_, ManifestId id_)
    : owner(&owner_), build(std::move(build_)), key(std::move(key_)), id(std::move(id_))
{
}

PreparedPartWrite::PreparedPartWrite(PreparedPartWrite && other) noexcept
    : owner(other.owner), build(std::move(other.build)), key(std::move(other.key)), id(std::move(other.id))
    , terminal(other.terminal)
{
    /// The move takes over the owed terminal operation in full: the source must never run it again,
    /// so it is left terminal (and holding no transaction) rather than merely emptied.
    other.owner = nullptr;
    other.terminal = true;
}

PreparedPartWrite::~PreparedPartWrite()
{
    if (terminal || !build)
        return;
    /// Last-resort guard. Reaching here means no terminal operation COMPLETED -- it was forgotten, an
    /// exception skipped it, or one was attempted and its append failed -- so the precommit binding is
    /// still live and its removal is appended here rather than left to leak. Best-effort and noexcept:
    /// a destructor that propagated the append failure would terminate the process, and the durable
    /// backstop for a removal that cannot land at all is the same one every other abandon path uses.
    LOG_ERROR(getLogger("CachedPartFolderAccess"),
              "PreparedPartWrite for '{}/{}' was destroyed while still owing its terminal operation; "
              "aborting it now", key.ns.string(), key.ref);
    abandonBuildBestEffort(*build, "aborting a prepared part write destroyed without a terminal operation");
}

bool PreparedPartWrite::commitIsUnresolved() const
{
    return build && build->commitState() == Cas::PartWriteTxn::CommitState::Uncertain;
}

Cas::CommitOutcome PreparedPartWrite::promote(bool allow_repoint)
{
    if (terminal)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PreparedPartWrite for '{}/{}' has already been promoted or aborted; it owes exactly one "
            "terminal operation",
            key.ns.string(), key.ref);
    try
    {
        /// `terminal` is set by `promoteBuild` itself, inside the allocation-free region immediately
        /// after the durable append -- NOT after this call returns. The difference is the whole point:
        /// the outcome assembly and the cache invalidation that follow the append can throw, and a
        /// handle that had not yet recorded the commit would then take the catch below and abandon a
        /// build whose ref is already published.
        return owner->promoteBuild(*build, key, build->buildId(), id, allow_repoint, &terminal);
    }
    catch (...)
    {
        /// The commit is durable and this handle knows it, so the duty is discharged and there is
        /// nothing to abandon -- the error is post-commit work failing, which the caller still hears
        /// about, unchanged.
        if (terminal)
            throw;
        /// The catch-abandon-rethrow discipline the atomic `publishEntries` has always owned. It stays
        /// correct even when the append itself was ambiguous: `abandon` appends a PRECOMMIT removal,
        /// and a promote that did land moved the binding to committed, so the removal is rejected by
        /// the state machine rather than undoing a published ref. The terminal flag flips only if the
        /// abandon actually landed: a failed abandon leaves the handle owing cleanup, which the
        /// destructor then retries.
        terminal = abandonBuildBestEffort(*build, "abandoning the build of a failed PreparedPartWrite::promote");
        throw;
    }
}

void PreparedPartWrite::abort()
{
    if (terminal)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PreparedPartWrite for '{}/{}' has already been promoted or aborted; it owes exactly one "
            "terminal operation",
            key.ns.string(), key.ref);
    /// `abandon` appends the EXACT precommit removal through the reliable append lane -- that append,
    /// not the destruction of the transaction object, is what releases the manifest's `+1`. A failure
    /// propagates: the caller may retry the same handle, and the destructor is the backstop.
    build->abandon();
    terminal = true;
}

PreparedPartWrite CachedPartFolderAccess::prepareEntries(const PartRefKey & dst,
    const std::vector<Cas::ManifestEntry> & entries, Cas::ProvenanceOp op)
{
    auto build = store->beginPartWrite(Cas::PartWriteInfo{.intended_ref = dst.ns.string() + "/" + dst.ref,
                                                  .intended_namespace = dst.ns, .op = op});
    try
    {
        /// Record write evidence for each non-inline entry. No pool HEAD/GET is performed before
        /// precommit; the promote path re-proves each dependency fail-closed. Inline entries need no evidence.
        for (const auto & entry : entries)
            build->adoptEvidence(entry);
        /// Stage a fresh manifest over the same entries. Blobs are content-addressed, but each part owns
        /// its manifest ID, so `dst` receives a distinct manifest before ownership moves to it.
        const Cas::ManifestId id = build->stageManifest(entries);
        build->precommitAdd(dst.ns, dst.ref, id);
        return PreparedPartWrite(*this, std::move(build), dst, id);
    }
    catch (...)
    {
        /// No handle is returned on this path, so the cleanup cannot be deferred to one: abandon here.
        abandonBuildBestEffort(*build, "abandoning the build of a failed prepareEntries");
        throw;
    }
}

Cas::CommitOutcome CachedPartFolderAccess::publishEntries(const PartRefKey & dst,
    const std::vector<Cas::ManifestEntry> & entries, Cas::ProvenanceOp op, bool allow_repoint)
{
    /// The atomic form: prepare and promote back to back, with no window in between. `promote` carries
    /// the catch-abandon-rethrow discipline, so a failure here behaves exactly as it did when this was
    /// one function.
    PreparedPartWrite prepared = prepareEntries(dst, entries, op);
    return prepared.promote(allow_repoint);
}

bool CachedPartFolderAccess::republishRef(const PartRefKey & src, const PartRefKey & dst)
{
    /// Content addressing has no rename, so move a committed ref by reading the source body freshly,
    /// publishing equivalent entries at the destination, and then dropping the source. The source
    /// body is re-proved and is never taken from a retained view.
    auto resolved = store->resolveRef(src.ns, src.ref);
    if (!resolved)
        return false;
    const auto src_manifest = store->readManifestShared(resolved->manifest_id);

    /// If `dst` is already committed, a previous attempt may have completed its promote before the
    /// source drop. Compare content rather than the whole manifest, whose ref/namespace/digest
    /// legitimately differ: equal content completes the move by dropping `src`; different content
    /// is a conflict and leaves the source intact.
    if (auto dst_resolved = store->resolveRef(dst.ns, dst.ref))
    {
        const auto dst_manifest = store->readManifestShared(dst_resolved->manifest_id);
        if (dst_manifest->entries != src_manifest->entries)
            throw Exception(ErrorCodes::ABORTED,
                "republishRef: destination '{}' is already committed with different content — refusing "
                "(rename/attach conflict)", dst.ns.string() + "/" + dst.ref);
        dropRef(src);
        return true;
    }

    publishEntries(dst, src_manifest->entries, Cas::ProvenanceOp::Other);
    dropRef(src);
    return true;
}

Cas::CommitOutcome CachedPartFolderAccess::repointRef(const PartRefKey & key, std::vector<Cas::ManifestEntry> entries, Cas::ProvenanceOp op)
{
    /// Compare the candidate `entries` against the currently committed manifest's
    /// decoded entries. This must NOT stage a candidate manifest first: `stageManifest` mints a
    /// non-content-derived `ManifestRef` (epoch/build_seq/ordinal) AND durably PUTs the encoded body
    /// on every call (CasPartWriteTxn.cpp), so staging-then-comparing IDs would itself be a pool mutation on
    /// the byte-equal path — violating the "ZERO pool mutations" contract this primitive exists to
    /// provide.
    ///
    /// The comparison must be symmetric. `committed_manifest->entries` already went
    /// through one `decodePartManifest` round-trip, which does not carry `blob_size` for Inline
    /// entries on the wire (it is redundant with `inline_bytes.size()`, use `ManifestEntry::size()` for
    /// the logical size instead, and it is excluded from both the canonical encoding and the payload
    /// digest — see `CasPartManifestFormat.cpp`'s `writeEntryRecord`/`decodePartManifest`). The
    /// freshly constructed `entries` may not have the same incidental fields (the inline write path no longer sets
    /// `blob_size`), but a straight struct compare against them is still not guaranteed byte-identical
    /// (canonical path ordering, etc.), so route the candidate through the identical encode/decode
    /// round-trip before comparing regardless — this is the same content comparison used by `republishRef`,
    /// which is symmetric for the same reason (both sides there are already decoded).
    auto resolved = resolve(key, Freshness::ForceFresh);
    if (resolved)
    {
        const auto committed_manifest = store->readManifestShared(resolved->manifest_id);
        Cas::PartManifest probe;
        probe.ref = committed_manifest->ref;
        probe.root_namespace_id = committed_manifest->root_namespace_id;
        probe.entries = entries;
        probe.payload_digest = Cas::computePayloadDigest(probe);
        const Cas::PartManifest canonical_candidate = Cas::decodePartManifest(Cas::encodePartManifest(probe));
        if (committed_manifest->entries == canonical_candidate.entries)
            /// ZERO pool mutations: the outcome describes the manifest ALREADY committed, unchanged.
            return Cas::CommitOutcome{key.ns, key.ref, resolved->manifest_id.ref, /*created=*/false};
    }
    /// `publishEntries` takes `entries` by const reference, so the caller-owned vector remains valid.
    /// Capture the exact outcome IMMEDIATELY -- before the ProfileEvent/logging below -- so it is
    /// published ahead of any further (even if non-throwing in practice) post-commit work.
    const Cas::CommitOutcome oc = publishEntries(key, entries, op, /*allow_repoint=*/true);
    ProfileEvents::increment(ProfileEvents::CASRefRepoint);
    if (resolved)
    {
        /// Repoint is the normal mechanism for effective standalone writes/removes on committed parts,
        /// so this routine event is logged at debug level while the counter remains an operator-facing signal.
        LOG_DEBUG(getLogger("CachedPartFolderAccess"),
            "Repointed committed ref {}/{} ({} entries) — standalone write/remove on a committed part",
            key.ns.string(), key.ref, entries.size());
    }
    else
    {
        /// A repoint normally targets an existing ref. Keep an unexpected create-shaped call visible
        /// at warning level rather than silently treating it as ordinary publication.
        LOG_WARNING(getLogger("CachedPartFolderAccess"),
            "repointRef published {}/{} ({} entries) with no prior committed ref to repoint — "
            "unexpected call shape (repointRef requires an existing committed ref)",
            key.ns.string(), key.ref, entries.size());
    }
    return oc;
}

void CachedPartFolderAccess::dropRef(const PartRefKey & key)
{
    store->dropRef(key.ns, key.ref);
    eraseView(key);
}

void CachedPartFolderAccess::dropRefIfPresent(const PartRefKey & key)
{
    /// resolveRef gates the common case (a temporary ref that was never committed is a no-op, not an
    /// error); dropRef re-reads the shard inside its own CAS loop, so a concurrent drop can land in
    /// the window between our resolve and that re-read — surfacing as FILE_DOESNT_EXIST. Removal is
    /// replay-safe, so an already-gone ref is success; any other exception still propagates. The view
    /// is also erased on the early-return absent path.
    if (!store->resolveRef(key.ns, key.ref, /*allow_stale=*/true))
    {
        eraseView(key);
        return;
    }
    try
    {
        store->dropRef(key.ns, key.ref);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
            throw;
        eraseView(key);
        return;   /// Raced away between the gate and dropRef; nothing was actually dropped here.
    }
    eraseView(key);
}

void CachedPartFolderAccess::dropRefBestEffort(const PartRefKey & key) noexcept
{
    try
    {
        store->dropRef(key.ns, key.ref);
    }
    catch (...)
    {
        /// Best-effort destructor/rollback cleanup: debris is GC-reclaimed, but swallowing the
        /// exception without a diagnostic could leave a live phantom ref after a backend outage.
        ProfileEvents::increment(ProfileEvents::CASRefRollbackBestEffortDropFailed);
        tryLogCurrentException(getLogger("CachedPartFolderAccess"),
            fmt::format("CA best-effort rollback dropRef failed (ns={} ref={}); the ref may remain live",
                        key.ns.string(), key.ref));
    }
    /// In destructor/rollback context the ref's durable state is unknown, so invalidate the view even
    /// after a swallowed cleanup exception.
    eraseView(key);
}

bool CachedPartFolderAccess::dropRefIfMatches(const PartRefKey & key, const Cas::ManifestRef & expected) noexcept
{
    /// One `appendRefOps` builder does both the read and the conditional removal, mirroring
    /// `CasRefLedger::dropRef`'s own protocol shape (read the committed binding, emit ONE
    /// `OwnerTransition` removal op) but with the removal guarded on `expected` inside the SAME
    /// closure -- the leader-thread read of `state.getCommitted()` is the authoritative committed
    /// binding at append time, so this is race-free the same way `PartWriteTxn::promote`'s
    /// `repoint_old`/idempotent-guard reads are: `build_ops` runs at most once, on the flush leader,
    /// against the batch-validated state. A mismatch (repointed since `expected` was observed, or
    /// already absent) returns an empty op list -- a legitimate no-op, not an error, exactly like
    /// `promote`'s own idempotent-redrive branch.
    bool removed = false;
    try
    {
        const RefTxnId txn_id = store->appendRefOps(key.ns, MutationScope::ref(key.ref),
            [&](const RefTableState & state) -> std::vector<RefOp>
            {
                const auto it = state.getCommitted().find(key.ref);
                if (it == state.getCommitted().end() || !(it->second.manifest_ref == expected))
                    return {};   /// absent, or repointed away from `expected` -- leave it alone

                removed = true;
                RefOp op;
                op.kind = RefOpKind::OwnerTransition;
                op.old_binding = RefOwnerBinding{RefOwnerKind::Committed, key.ref, expected};
                return {op};
            },
            RootMutationOrigin::Writer, RootMutationKind::Drop);

        /// Audit a successful conditional removal exactly like `CasRefLedger::dropRef` audits its own
        /// unconditional one -- otherwise a rollback drop (this method's only caller, as of Task 3)
        /// would be invisible in `system.cas_log`. Byte-neutral to the ref-log: this is
        /// an audit event only, emitted after the removal is already durable.
        if (removed && store->hasEventSink())
        {
            Cas::CasEvent ev;
            ev.type = Cas::CasEventType::RefDrop;
            ev.namespace_ = key.ns.string();
            ev.ref_name = key.ref;
            ev.object_kind = Cas::CasEventObjectKind::Manifest;
            ev.object_hash = Cas::manifestRefDebugString(expected);
            ev.at_version = txn_id.ref_sequence;
            ev.outcome = "ok";
            ev.reason = "dropRefIfMatches: conditional rollback removed the exact manifest this caller committed";
            store->emitEvent(std::move(ev));
        }
    }
    catch (...)
    {
        /// Best-effort rollback cleanup, like dropRefBestEffort: debris is GC-reclaimed, but swallowing
        /// without a diagnostic could leave a live phantom ref after a backend outage.
        removed = false;
        ProfileEvents::increment(ProfileEvents::CASRefRollbackBestEffortDropFailed);
        tryLogCurrentException(getLogger("CachedPartFolderAccess"),
            fmt::format("CA conditional rollback dropRefIfMatches failed (ns={} ref={} expected={}); "
                        "the ref may remain live", key.ns.string(), key.ref, Cas::manifestRefDebugString(expected)));
    }
    /// The read above is authoritative fresh state regardless of outcome, so any locally retained view
    /// is invalidated unconditionally -- cheap and conservative, matching dropRefBestEffort.
    eraseView(key);
    return removed;
}

void CachedPartFolderAccess::dropNamespace(const Cas::RootNamespace & ns)
{
    store->dropNamespace(ns);
    if (view_cache)
    {
        const String prefix = ns.string() + '\0';
        view_cache->remove([&](const String & k, const auto &) { return k.starts_with(prefix); });
    }
    ProfileEvents::increment(ProfileEvents::CASPartFolderViewInvalidations);
}

void CachedPartFolderAccess::recordDecision(const String & cache_key, LastDecision decision,
                                            const PartFolderView * view, bool retained) const
{
    if (!params.explain_enabled)
        return;   /// Disabled diagnostics keep the read path free of journal locking and allocation.
    std::lock_guard lock(explain_mutex);
    if (explain_map.size() >= EXPLAIN_MAX_ENTRIES)
        explain_map.clear();
    auto & e = explain_map[cache_key];
    e.last_decision = decision;
    e.retained = retained;
    if (view)
    {
        e.manifest_ref = Cas::manifestRefDebugString(view->manifestId().ref);
        e.estimated_bytes = view->estimatedBytes();
    }
}

size_t CachedPartFolderAccess::explainJournalSizeForTest() const
{
    std::lock_guard lock(explain_mutex);
    return explain_map.size();
}

CachedPartFolderAccess::ExplainResult CachedPartFolderAccess::explain(const PartRefKey & key) const
{
    ExplainResult result;
    {
        std::lock_guard lock(explain_mutex);
        const auto it = explain_map.find(key.cacheKey());
        if (it != explain_map.end())
            result = it->second;
    }
    /// `retained` is reported live against the cache, not from the decision snapshot: `dropNamespace`
    /// erases every key of a namespace via one `CacheBase::remove` predicate sweep without a per-key
    /// `recordDecision` call, so a snapshot value would go stale for every key it touches except the
    /// one last read. A live membership check is authoritative for every eraser (write-through or
    /// namespace-wide) and costs one more `CacheBase` lookup on this test/log-only path.
    result.retained = view_cache && view_cache->get(key.cacheKey()) != nullptr;
    return result;
}

}
