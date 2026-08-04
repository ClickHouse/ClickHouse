#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <fmt/format.h>
#include <algorithm>
#include <exception>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace

{

CasRefCatalog::Snapshot readOptionalForBootstrap(Backend & backend, const Layout & layout)
{
    const auto got = backend.get(layout.refCatalogKey());
    if (!got)
    {
        RefCatalog empty;
        return CasRefCatalog::Snapshot{
            .catalog = empty, .token = std::nullopt, .life_index = CatalogLifeIndex(empty)};
    }
    RefCatalog catalog = decodeRefCatalog(got->bytes);
    return CasRefCatalog::Snapshot{
        .catalog = catalog, .token = got->token, .life_index = CatalogLifeIndex(catalog)};
}

}

CasRefCatalog::Snapshot CasRefCatalog::read(Backend & backend, const Layout & layout)
{
    Snapshot snapshot = readOptionalForBootstrap(backend, layout);
    if (!snapshot.token)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Mandatory CAS ref catalog '{}' is absent -- refusing to interpret opaque life "
            "objects as an empty ownership universe",
            layout.refCatalogKey());
    return snapshot;
}

CasRefCatalog::Snapshot CasRefCatalog::initializeEmptyForNewPool(Backend & backend, const Layout & layout)
{
    RefCatalog empty;
    const String canonical_empty = encodeRefCatalog(empty);
    const PutResult put = backend.putIfAbsent(layout.refCatalogKey(), canonical_empty);
    if (put.outcome == PutOutcome::Done)
        return Snapshot{.catalog = empty, .token = put.token, .life_index = CatalogLifeIndex(empty)};

    /// A second opener can win after both proved the prefix empty. Decode its exact object before
    /// accepting the race; conflict is never a license to continue with an assumed empty catalog or
    /// arbitrary decoded body.
    const auto got = backend.get(layout.refCatalogKey());
    if (!got)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS ref catalog '{}' disappeared after bootstrap create conflict",
            layout.refCatalogKey());
    RefCatalog catalog = decodeRefCatalog(got->bytes);
    if (!catalog.entries.empty() || got->bytes != canonical_empty)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS ref catalog '{}' conflicts with bootstrap's required canonical empty catalog",
            layout.refCatalogKey());
    return Snapshot{.catalog = std::move(catalog), .token = got->token, .life_index = CatalogLifeIndex(empty)};
}

std::optional<NamespaceLifeId> CasRefCatalog::lifeIfCataloged(
    Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const Snapshot snap = read(backend, layout);
    for (const CatalogEntry & entry : snap.catalog.entries)
        if (entry.ns.string() == ns.string() && entry.state != NsState::Creating)
            return snap.life_index.resolve(entry.incarnation);
    return std::nullopt;
}

std::vector<NamespaceLifeId> CasRefCatalog::liveUniverse(Backend & backend, const Layout & layout)
{
    const Snapshot snap = read(backend, layout);
    snap.life_index.throwIfAmbiguous("CAS live namespace discovery");
    std::vector<NamespaceLifeId> universe;
    universe.reserve(snap.catalog.entries.size());
    for (const CatalogEntry & entry : snap.catalog.entries)
    {
        if (entry.state == NsState::Creating)
            continue;
        universe.push_back(NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation));
    }
    return universe;
}

namespace
{

/// Live-lock brake, the same shape and for the same reason as `publishCkpt`'s/`allocateWriterEpoch`'s
/// on their own contended token-CAS singletons: the catalog is ONE object mutated by every lifecycle
/// transition of every namespace in the pool, so persistent contention is a real, not theoretical,
/// exit condition to plan for.
constexpr size_t kMaxCatalogCasAttempts = 100;

/// Shared body of `casUpdate`/`casAdmitEntry`. `encode` turns a freshly `mutate`d candidate into the
/// bytes to write: the plain path just grammar-checks (`encodeRefCatalog`), the admitting path also
/// runs both admission predicates (`checkCatalogAdmission`) first. Retries on `Conflict` against a
/// FRESH read, exactly like `PoolMeta::admitOrValidate` -- never re-encoding the stale candidate.
RefCatalog casUpdateImpl(
    Backend & backend, const Layout & layout,
    const std::function<RefCatalog(const RefCatalog &)> & mutate,
    const std::function<String(const RefCatalog &)> & encode)
{
    const String key = layout.refCatalogKey();
    CasRefCatalog::Snapshot snap = CasRefCatalog::read(backend, layout);

    for (size_t attempt = 0; attempt < kMaxCatalogCasAttempts; ++attempt)
    {
        snap.life_index.throwIfAmbiguous("CAS ref catalog mutation");
        RefCatalog candidate = mutate(snap.catalog);
        const String bytes = encode(candidate);
        const CasResult res = backend.casPut(key, bytes, snap.token);
        if (res.outcome == CasOutcome::Committed)
            return candidate;

        snap = CasRefCatalog::read(backend, layout);
        /// `read` treats authoritative absence after a conflict as corruption. Therefore no retry
        /// can turn a vanished mandatory catalog into a one-update replacement authority.
    }

    throwCasWriteRetryLater(fmt::format(
        "CAS ref catalog '{}' did not converge after {} attempts", key, kMaxCatalogCasAttempts));
}

/// Thrown from inside a `casUpdate` `mutate` closure to signal a refusal that must STOP the attempt
/// rather than be treated as a `Conflict` to retry: `casUpdateImpl` propagates whatever `mutate`
/// throws straight out, uncaught, which is exactly the behavior these three need. Retrying any of them
/// against a freshly re-read catalog would just re-decide against an entry that is, by definition, no
/// longer `observed` -- token-exactness means the FIRST mismatch is final, not a reason to loop.
/// Each is caught by its own exact type right where it is thrown; deriving from `std::exception`
/// is only so the throw itself is well-formed, never so a caller catches these by base class.
struct CatalogFenceMovedMarker : std::exception {};
struct CatalogEntryMismatchMarker : std::exception {};
struct CatalogCreatorStillLiveMarker : std::exception {};

/// Two `thread_local_rng` draws composed into a `UInt128`, the same pattern already used throughout
/// this tree to mint build ids and incarnation tags (`CasPartWriteTxn.cpp`'s `mintU128`,
/// `ContentAddressedTransaction.cpp`'s `incarnation_tag`). Retried on the astronomically unlikely `0`
/// draw: unlike those callers, this value must never be zero (`CatalogEntry::incarnation == 0` is
/// always invalid -- "0 never names a life"), and this is the one mint site in that family with a
/// grammar rule to uphold.
UInt128 mintFreshIncarnation()
{
    UInt128 v = 0;
    while (v == 0)
        v = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
    return v;
}

/// Shared by every function below that needs "the current entry for this namespace, if any" --
/// keeping ONE lookup rather than three independently-written `find_if`s that could drift apart on
/// what counts as a match.
std::vector<CatalogEntry>::const_iterator findEntry(const RefCatalog & catalog, const RootNamespace & ns)
{
    return std::find_if(catalog.entries.begin(), catalog.entries.end(),
        [&](const CatalogEntry & e) { return e.ns.string() == ns.string(); });
}

/// Thrown from `createNamespaceStep1`'s own `mutate` (below) when a FRESH read -- the first one, or
/// any `Conflict` retry's re-read -- already carries an entry for the namespace being admitted. Never
/// thrown by the public `casAdmitEntry`: that function keeps its documented "already-present is a
/// caller bug, let `encodeRefCatalog` abort" contract for its many single-namespace-per-catalog test
/// callers -- it has no production caller at all; `createNamespaceStep1` below duplicates its
/// admission shape rather than calling it, precisely so this recheck can be added without weakening
/// `casAdmitEntry` itself. `createNamespace` alone needs the other answer, because ITS
/// "already present" can be a sibling opener's OWN in-flight step 1 landing between createNamespace's
/// pre-check read and this loop's read -- a race the design already names and resumes through
/// `Superseded`, not a caller bug.
struct CatalogEntryAlreadyPresentMarker : std::exception {};

/// Fires once, synchronously, right before `createNamespaceStep1`'s own first catalog read -- i.e.
/// after `createNamespace`'s pre-check read already observed no entry. Lets a test land a sibling
/// opener's full `createNamespace` call in that exact window, driving the interleaving
/// `CatalogEntryAlreadyPresentMarker` exists to catch instead of relying on real thread scheduling.
/// Empty (no-op) in production, mirroring every other `*_hook_for_test` in this tree.
std::function<void()> create_namespace_step1_pre_read_hook_for_test;

/// Step 1 of `createNamespace`, split out so it can recheck presence on EVERY catalog read this loop
/// performs (the first one, and any `Conflict` retry's re-read), not only the snapshot-in-time read
/// `createNamespace` itself already did before calling in. That single upfront read cannot see a
/// sibling opener's OWN step 1 landing between it and this loop's read; without the recheck here, this
/// loop would blindly insert a second row for the same namespace and let `encodeRefCatalog`'s
/// canonical-order/no-duplicate grammar check abort the process with `LOGICAL_ERROR` for what is, at
/// this call site only, an ordinary race outcome.
RefCatalog createNamespaceStep1(
    Backend & backend, const Layout & layout, uint64_t gc_shards, const CatalogEntry & entry)
{
    /// Moved into a local before invoking, not called on the global directly: a hook that reassigns
    /// `create_namespace_step1_pre_read_hook_for_test` from inside its own body (a test driving a
    /// one-shot interleaving) would otherwise reassign the very `std::function` object whose `operator()`
    /// is executing it -- undefined behavior, not merely untidy. The local copy is a distinct object the
    /// hook body cannot reach.
    if (create_namespace_step1_pre_read_hook_for_test)
    {
        std::function<void()> hook_to_run;
        std::swap(hook_to_run, create_namespace_step1_pre_read_hook_for_test);
        hook_to_run();
    }

    const auto mutate = [&entry](const RefCatalog & cur) -> RefCatalog
    {
        if (findEntry(cur, entry.ns) != cur.entries.end())
            throw CatalogEntryAlreadyPresentMarker{};
        RefCatalog next = cur;
        const auto it = std::lower_bound(next.entries.begin(), next.entries.end(), entry,
            [](const CatalogEntry & a, const CatalogEntry & b) { return a.ns.string() < b.ns.string(); });
        next.entries.insert(it, entry);
        return next;
    };
    return casUpdateImpl(backend, layout, mutate,
        [&entry, gc_shards, &layout](const RefCatalog & c)
        {
            return checkCatalogAdmission(c, gc_shards, layout, entry.ns);
        });
}

}

RefCatalog CasRefCatalog::casUpdate(
    Backend & backend, const Layout & layout, const std::function<RefCatalog(const RefCatalog &)> & mutate)
{
    const auto identity_preserving_mutate = [&](const RefCatalog & current) -> RefCatalog
    {
        RefCatalog candidate = mutate(current);
        if (candidate.entries.size() != current.entries.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CasRefCatalog::casUpdate cannot add or delete catalog entries -- use casAdmitEntry, "
                "deleteCompletedRemoving, or cancelStalledCreating");
        for (size_t i = 0; i < current.entries.size(); ++i)
        {
            if (candidate.entries[i].ns != current.entries[i].ns
                || candidate.entries[i].incarnation != current.entries[i].incarnation)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "CasRefCatalog::casUpdate cannot replace catalog identity at row {} -- namespace "
                    "and incarnation are immutable outside the narrow admission/deletion APIs",
                    i);
        }
        return candidate;
    };
    return casUpdateImpl(
        backend, layout, identity_preserving_mutate, [](const RefCatalog & c) { return encodeRefCatalog(c); });
}

RefCatalog CasRefCatalog::casAdmitEntry(
    Backend & backend, const Layout & layout, uint64_t gc_shards, const CatalogEntry & entry)
{
    if (entry.state == NsState::Removing)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::casAdmitEntry cannot admit namespace '{}' directly as Removing -- "
            "removal is an exact transition of an existing Live row",
            entry.ns.string());

    /// The mutation shape is FIXED (insert `entry` at its canonical position) rather than a
    /// caller-supplied lambda -- see the header comment on why that is the point, not an
    /// inconvenience. A namespace that already has an entry is not de-duplicated here: the insert
    /// makes the candidate carry two adjacent equal-ns rows, and `encodeRefCatalog`'s own
    /// canonical-order/no-duplicate check (run inside `checkCatalogAdmission` below) rejects that
    /// shape -- one place owns that rule, not two.
    const auto mutate = [&entry](const RefCatalog & cur) -> RefCatalog
    {
        RefCatalog next = cur;
        const auto it = std::lower_bound(next.entries.begin(), next.entries.end(), entry,
            [](const CatalogEntry & a, const CatalogEntry & b) { return a.ns.string() < b.ns.string(); });
        next.entries.insert(it, entry);
        return next;
    };
    return casUpdateImpl(backend, layout, mutate,
        [&entry, gc_shards, &layout](const RefCatalog & c)
        {
            return checkCatalogAdmission(c, gc_shards, layout, entry.ns);
        });
}

CasRefCatalog::BeginRemovingOutcome CasRefCatalog::beginRemoving(
    Backend & backend, const Layout & layout, const CatalogEntry & observed,
    uint64_t removal_started_round, uint64_t admitted_generation,
    const std::function<void(uint64_t)> & check_fence_or_throw)
{
    if (observed.state != NsState::Live || observed.creator || observed.removal_started_round)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::beginRemoving: namespace '{}' is not an exact Live entry",
            observed.ns.string());

    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        try { check_fence_or_throw(admitted_generation); }
        catch (...) { throw CatalogFenceMovedMarker{}; }

        const auto it = findEntry(cur, observed.ns);
        if (it == cur.entries.end() || *it != observed)
            throw CatalogEntryMismatchMarker{};

        RefCatalog next = cur;
        CatalogEntry & entry = next.entries[it - cur.entries.begin()];
        entry.state = NsState::Removing;
        entry.removal_started_round = removal_started_round;
        return next;
    };

    try
    {
        casUpdateImpl(backend, layout, mutate, [](const RefCatalog & c) { return encodeRefCatalog(c); });
    }
    catch (const CatalogFenceMovedMarker &)
    {
        return BeginRemovingOutcome::FencedOut;
    }
    catch (const CatalogEntryMismatchMarker &)
    {
        const Snapshot current = read(backend, layout);
        const auto it = findEntry(current.catalog, observed.ns);
        if (it != current.catalog.entries.end()
            && it->incarnation == observed.incarnation
            && it->state == NsState::Removing)
            return BeginRemovingOutcome::AlreadyRemoving;
        return BeginRemovingOutcome::EntryChanged;
    }
    return BeginRemovingOutcome::Transitioned;
}

CasRefCatalog::CompletedRemovingDeleteResult CasRefCatalog::deleteCompletedRemoving(
    Backend & backend, const Layout & layout, const CatalogEntry & observed,
    const CasFoldSeal & authoritative_parent, uint64_t admitted_generation,
    const std::function<LeaderFenceStatus(uint64_t)> & check_fence)
{
    if (observed.state != NsState::Removing || !observed.removal_started_round)
        return {
            .outcome = CompletedRemovingDeleteOutcome::ProofRefused,
            .invalidated_life = std::nullopt,
            .catalog_snapshot = std::nullopt};

    const auto parent_it = authoritative_parent.ref_lives.find(observed.incarnation);
    if (parent_it == authoritative_parent.ref_lives.end()
        || !parent_it->second.cleanup_evidence
        || parent_it->second.coverage.hold)
        return {
            .outcome = CompletedRemovingDeleteOutcome::ProofRefused,
            .invalidated_life = std::nullopt,
            .catalog_snapshot = std::nullopt};

    return deleteCompletedRemovingAtSnapshot(
        backend, layout, read(backend, layout), observed, authoritative_parent,
        admitted_generation, check_fence);
}

CasRefCatalog::CompletedRemovingDeleteResult CasRefCatalog::deleteCompletedRemovingAtSnapshot(
    Backend & backend, const Layout & layout, Snapshot catalog_snapshot,
    const CatalogEntry & observed, const CasFoldSeal & authoritative_parent,
    uint64_t admitted_generation,
    const std::function<LeaderFenceStatus(uint64_t)> & check_fence)
{
    if (observed.state != NsState::Removing || !observed.removal_started_round)
        return {
            .outcome = CompletedRemovingDeleteOutcome::ProofRefused,
            .invalidated_life = std::nullopt,
            .catalog_snapshot = std::nullopt};

    const auto parent_it = authoritative_parent.ref_lives.find(observed.incarnation);
    if (parent_it == authoritative_parent.ref_lives.end()
        || !parent_it->second.cleanup_evidence
        || parent_it->second.coverage.hold)
        return {
            .outcome = CompletedRemovingDeleteOutcome::ProofRefused,
            .invalidated_life = std::nullopt,
            .catalog_snapshot = std::nullopt};

    const NamespaceLifeId old_life
        = NamespaceLifeId::fromCatalogEntry(observed.ns, observed.incarnation);
    const auto resolved_result = [&](CompletedRemovingDeleteOutcome outcome)
    {
        const auto current_it = findEntry(catalog_snapshot.catalog, observed.ns);
        const bool old_life_still_cataloged = current_it != catalog_snapshot.catalog.entries.end()
            && current_it->incarnation == observed.incarnation;
        return CompletedRemovingDeleteResult{
            .outcome = outcome,
            .invalidated_life = old_life_still_cataloged
                ? std::nullopt
                : std::optional<NamespaceLifeId>{old_life},
            .catalog_snapshot = std::move(catalog_snapshot)};
    };

    for (size_t attempt = 0; attempt < kMaxCatalogCasAttempts; ++attempt)
    {
        catalog_snapshot.life_index.throwIfAmbiguous("CAS completed-removal deletion");
        const auto observed_it = findEntry(catalog_snapshot.catalog, observed.ns);
        if (observed_it == catalog_snapshot.catalog.entries.end() || *observed_it != observed)
            return resolved_result(CompletedRemovingDeleteOutcome::EntryChanged);

        bool fence_lost = check_fence(admitted_generation) == LeaderFenceStatus::Moved;

        std::optional<CasResult> cas_result;
        std::exception_ptr attempt_failure;
        if (!fence_lost)
        {
            RefCatalog candidate = catalog_snapshot.catalog;
            candidate.entries.erase(candidate.entries.begin() + (observed_it - catalog_snapshot.catalog.entries.begin()));
            try
            {
                cas_result = backend.casPut(
                    layout.refCatalogKey(), encodeRefCatalog(candidate), catalog_snapshot.token);
            }
            catch (...)
            {
                attempt_failure = std::current_exception();
            }
        }

        /// The response to a conditional erase is not authority for what became durable. Resolve
        /// every attempted erase, and a pre-CAS fence refusal, through one complete catalog read.
        /// This snapshot is also the next retry/selection cut, so no second read separates them.
        catalog_snapshot = read(backend, layout);

        if (!fence_lost)
            fence_lost = check_fence(admitted_generation) == LeaderFenceStatus::Moved;
        if (fence_lost)
            return resolved_result(CompletedRemovingDeleteOutcome::FencedOut);

        const auto current_it = findEntry(catalog_snapshot.catalog, observed.ns);
        const bool old_life_still_cataloged = current_it != catalog_snapshot.catalog.entries.end()
            && current_it->incarnation == observed.incarnation;
        if (!old_life_still_cataloged)
            return resolved_result(current_it == catalog_snapshot.catalog.entries.end()
                ? CompletedRemovingDeleteOutcome::Deleted
                : CompletedRemovingDeleteOutcome::EntryChanged);

        if (attempt_failure)
            std::rethrow_exception(attempt_failure);
        if (cas_result && cas_result->outcome == CasOutcome::Committed)
            throwCasWriteRetryLater(fmt::format(
                "CAS ref catalog erase for namespace '{}' reported committed, but a complete resolution read "
                "still observed incarnation {}",
                observed.ns.string(), u128ToHex(observed.incarnation)));
        /// A token conflict that leaves the exact old row present retries from this mandatory
        /// resolution snapshot. The fence is checked again immediately before the next CAS.
    }

    throwCasWriteRetryLater(fmt::format(
        "CAS ref catalog erase for namespace '{}' did not converge after {} attempts",
        observed.ns.string(), kMaxCatalogCasAttempts));
}

CasRefCatalog::StalledCreatingCancelOutcome CasRefCatalog::cancelStalledCreating(
    Backend & backend, const Layout & layout, const CatalogEntry & observed,
    const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal,
    uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::cancelStalledCreating: namespace '{}' is not a Creating entry with a "
            "creator fence",
            observed.ns.string());

    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        try { check_fence_or_throw(admitted_generation); }
        catch (...) { throw CatalogFenceMovedMarker{}; }

        const auto it = findEntry(cur, observed.ns);
        if (it == cur.entries.end() || *it != observed)
            throw CatalogEntryMismatchMarker{};
        if (!is_creator_fence_terminal(*observed.creator))
            throw CatalogCreatorStillLiveMarker{};

        RefCatalog next = cur;
        next.entries.erase(next.entries.begin() + (it - cur.entries.begin()));
        return next;
    };

    try
    {
        casUpdateImpl(backend, layout, mutate, [](const RefCatalog & c) { return encodeRefCatalog(c); });
    }
    catch (const CatalogFenceMovedMarker &) { return StalledCreatingCancelOutcome::FencedOut; }
    catch (const CatalogEntryMismatchMarker &) { return StalledCreatingCancelOutcome::EntryChanged; }
    catch (const CatalogCreatorStillLiveMarker &) { return StalledCreatingCancelOutcome::CreatorFenceStillLive; }
    return StalledCreatingCancelOutcome::Cancelled;
}

CasRefCatalog::NamespaceCreationOutcome CasRefCatalog::completeCreation(
    Backend & backend, const Layout & layout, const CatalogEntry & observed,
    uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw,
    const CkptDeadline & deadline)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::completeCreation: namespace '{}' is not a Creating entry with a creator "
            "fence -- steps 2/3 only ever run over one of those", observed.ns.string());

    /// Step 2 (spec §3): INV-4's first `_ckpt` writer for this incarnation, and the only writer that
    /// will ever know its genesis epoch -- see `Pool/CasRefCkpt.h`'s `publishCkpt` doc for the merge
    /// discipline this rides on unchanged. `FencedOut` here ends the attempt: nothing durable changed.
    const RefCkpt contribution{.life_epoch = std::optional<uint64_t>{observed.creator->writer_epoch},
                                .committed_through = std::nullopt,
                                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    if (publishCkpt(backend, layout, NamespaceLifeId::fromCatalogEntry(observed.ns, observed.incarnation),
                     contribution, admitted_generation, check_fence_or_throw,
                     deadline) == CkptPublishOutcome::FencedOut)
        return NamespaceCreationOutcome::FencedOut;

    /// Step 3. `mutate` is the fence re-check point `casUpdate`'s header doc names -- checked FIRST,
    /// mirroring `publishCkpt`'s own "after the read, before the CAS, on every attempt" placement, so a
    /// caller stale on BOTH axes between step 2 and here is reported `FencedOut`, never `Superseded`
    /// (both are truthful refusals of a CAS that was never sent; this is only which one speaks first).
    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        try { check_fence_or_throw(admitted_generation); }
        catch (...) { throw CatalogFenceMovedMarker{}; }   /// typed, not propagated -- publishCkpt's own precedent

        const auto it = findEntry(cur, observed.ns);
        if (it == cur.entries.end() || *it != observed)
            throw CatalogEntryMismatchMarker{};

        RefCatalog next = cur;
        next.entries[static_cast<size_t>(it - cur.entries.begin())].state = NsState::Live;
        next.entries[static_cast<size_t>(it - cur.entries.begin())].creator = std::nullopt;
        return next;
    };

    try
    {
        casUpdate(backend, layout, mutate);
    }
    catch (const CatalogFenceMovedMarker &) { return NamespaceCreationOutcome::FencedOut; }
    catch (const CatalogEntryMismatchMarker &) { return NamespaceCreationOutcome::Superseded; }
    return NamespaceCreationOutcome::Live;
}

CasRefCatalog::NamespaceCreationOutcome CasRefCatalog::createNamespace(
    Backend & backend, const Layout & layout, uint64_t gc_shards,
    const RootNamespace & ns, const CreatorFence & creator,
    uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw,
    const CkptDeadline & deadline)
{
    /// Read-first, per the Task 2 review's own note on `casAdmitEntry`: a namespace that already
    /// carries an entry is THIS function's job to reject with a clear message, not `casAdmitEntry`'s
    /// duplicate-namespace grammar refusal (which would report a `LOGICAL_ERROR` about canonical order
    /// -- true, but useless to a caller trying to understand why its create failed). A concurrent
    /// insert of the SAME namespace between this read and step 1 is still caught -- `casAdmitEntry`'s
    /// own grammar check is the backstop, not the only check.
    const Snapshot snap = read(backend, layout);
    const auto existing = findEntry(snap.catalog, ns);
    if (existing != snap.catalog.entries.end())
    {
        /// `Creating` is not this function's problem to solve (the class-level doc above says so) --
        /// it is exactly the race `resolveNamespaceLife`'s own loop is built to absorb: sibling openers
        /// of the SAME namespace (e.g. concurrent per-part freeze threads of one query, which share one
        /// mount's fence) can all observe "no entry" before any of them lands step 1, then race into
        /// this call. Reporting `Superseded` sends the loser back through the loop, where it re-reads
        /// and takes the documented resume path (its own fence: `completeCreation`; a foreign one:
        /// `reconcileStaleCreator`) instead of aborting the server for an outcome the design already
        /// names and handles. `Live`/`Removing` stay a `LOGICAL_ERROR`: `namespaceLife`'s caller filters
        /// `Live` before ever reaching here and refuses `Removing` outright, so seeing either here means
        /// a caller bypassed that dispatch, not a race.
        if (existing->state == NsState::Creating)
            return NamespaceCreationOutcome::Superseded;
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::createNamespace: namespace '{}' already carries a catalog entry (state "
            "'{}') -- a stalled Creating entry is resumed through reconcileStaleCreator + "
            "completeCreation, never a fresh createNamespace call; an existing Live or Removing "
            "namespace must complete its current lifecycle before a fresh creation can be admitted",
            ns.string(), nsStateToWord(existing->state));
    }

    const CatalogEntry entry{.ns = ns, .state = NsState::Creating,
                              .incarnation = mintFreshIncarnation(), .creator = creator};
    /// The read above is a snapshot in time, not a lock: a sibling opener of the SAME namespace that
    /// also observed "no entry" can land its own step 1 between that read and this one. `casAdmitEntry`
    /// itself cannot be the backstop for that shape -- it retries its own `Conflict`s by blindly
    /// re-inserting `entry` into whatever it freshly reads, and a duplicate-namespace insert reaches
    /// `encodeRefCatalog`'s grammar check as an unconditional `LOGICAL_ERROR` abort. `createNamespaceStep1`
    /// is the same admission, but rechecks presence on every read this loop performs (not just the one
    /// above) and reports the race as `Superseded` instead.
    try
    {
        createNamespaceStep1(backend, layout, gc_shards, entry);   /// step 1
    }
    catch (const CatalogEntryAlreadyPresentMarker &)
    {
        return NamespaceCreationOutcome::Superseded;
    }
    return completeCreation(backend, layout, entry, admitted_generation, check_fence_or_throw, deadline);
}

void CasRefCatalog::setCreateNamespaceStep1PreReadHookForTest(std::function<void()> hook)
{
    create_namespace_step1_pre_read_hook_for_test = std::move(hook);
}

CasRefCatalog::ReconcileCreatorOutcome CasRefCatalog::reconcileStaleCreator(
    Backend & backend, const Layout & layout, const CatalogEntry & observed, const CreatorFence & new_creator,
    const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal,
    uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::reconcileStaleCreator: namespace '{}' is not a Creating entry with a "
            "creator fence -- nothing to reconcile", observed.ns.string());

    /// Review I6: the fence re-check is checked FIRST, on every fresh read this CAS retries -- the same
    /// placement `completeCreation` uses for exactly the same reason (see that function's own doc).
    /// Token-exactness (the catalog's own entry, by full value) comes next: it is the cheaper, purely
    /// local comparison, and a mismatch here means the question "is the OLD creator's fence terminal" is
    /// moot -- `observed` no longer describes anything live to reconcile.
    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        try { check_fence_or_throw(admitted_generation); }
        catch (...) { throw CatalogFenceMovedMarker{}; }   /// typed, not propagated -- completeCreation's own precedent

        const auto it = findEntry(cur, observed.ns);
        if (it == cur.entries.end() || *it != observed)
            throw CatalogEntryMismatchMarker{};
        if (!is_creator_fence_terminal(*observed.creator))
            throw CatalogCreatorStillLiveMarker{};

        RefCatalog next = cur;
        next.entries[static_cast<size_t>(it - cur.entries.begin())].creator = new_creator;
        return next;
    };

    try
    {
        casUpdate(backend, layout, mutate);
    }
    catch (const CatalogFenceMovedMarker &) { return ReconcileCreatorOutcome::FencedOut; }
    catch (const CatalogEntryMismatchMarker &) { return ReconcileCreatorOutcome::EntryChanged; }
    catch (const CatalogCreatorStillLiveMarker &) { return ReconcileCreatorOutcome::CreatorFenceStillLive; }
    return ReconcileCreatorOutcome::Reconciled;
}

void CasRefCatalog::checkPublicationAdmittedOrThrow(const RefCatalog & catalog, const RootNamespace & ns)
{
    const auto it = findEntry(catalog, ns);
    if (it != catalog.entries.end() && it->state == NsState::Creating)
        throwCasWriteRetryLater(fmt::format(
            "CAS ref catalog: namespace '{}' is still Creating -- no ref writes are admitted until "
            "its creation completes or is reconciled away", ns.string()));
}

}
