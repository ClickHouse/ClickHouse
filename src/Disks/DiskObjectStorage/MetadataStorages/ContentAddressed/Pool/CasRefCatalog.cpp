#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasHotKeys.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <base/defines.h>
#include <fmt/format.h>
#include <algorithm>
#include <exception>
#include <variant>

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

/// The one verdict for an absent mandatory catalog, so the read entry point and the mutation's own
/// `decide` cannot drift apart on what absence means.
[[noreturn]] void throwMandatoryCatalogAbsent(const String & key)
{
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "Mandatory CAS ref catalog '{}' is absent -- refusing to interpret opaque life "
        "objects as an empty ownership universe",
        key);
}

/// Every non-committed alternative of a catalog write, as the exception its meaning already implies.
/// `Declined` cannot reach here: `create` never declines, and no `decide` in this file returns
/// nothing to write.
[[noreturn]] void throwCatalogWriteFailure(WriteResult result, const String & what)
{
    orThrow(std::move(result), what);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: the write was declined, which this call cannot produce", what);
}

CasRefCatalog::Snapshot readOptionalForBootstrap(CasOperation & op, const Layout & layout,
                                                const Retry & policy = Retry::standard())
{
    const std::optional<Object> got = op.read(layout.refCatalogKey(), policy);
    if (!got)
    {
        RefCatalog empty;
        return CasRefCatalog::Snapshot{
            .catalog = empty, .etag = std::nullopt, .life_index = CatalogLifeIndex(empty)};
    }
    RefCatalog catalog = decodeRefCatalog(got->bytes);
    return CasRefCatalog::Snapshot{
        .catalog = catalog, .etag = got->etag, .life_index = CatalogLifeIndex(catalog)};
}

}

CasRefCatalog::Snapshot CasRefCatalog::read(CasOperation & op, const Layout & layout, const Retry & policy)
{
    Snapshot snapshot = readOptionalForBootstrap(op, layout, policy);
    if (!snapshot.etag)
        throwMandatoryCatalogAbsent(layout.refCatalogKey());
    return snapshot;
}

CasRefCatalog::Snapshot CasRefCatalog::initializeEmptyForNewPool(CasOperation & op, const Layout & layout)
{
    const String key = layout.refCatalogKey();
    RefCatalog empty;
    const String canonical_empty = encodeRefCatalog(empty);
    WriteResult result = op.create(key, canonical_empty, Retry::standard());
    if (const auto * committed = std::get_if<Committed>(&result))
        return Snapshot{.catalog = empty, .etag = committed->etag, .life_index = CatalogLifeIndex(empty)};

    /// A second opener can win after both proved the prefix empty. The refused precondition was
    /// settled by an exact read, so the winner's object is decoded from what that read observed;
    /// a conflict is never a license to continue with an assumed empty catalog or an arbitrary body.
    const auto * conflict = std::get_if<Conflict>(&result);
    if (!conflict)
        throwCatalogWriteFailure(std::move(result), fmt::format("CAS ref catalog '{}' bootstrap create", key));

    const auto * occupant = std::get_if<Object>(&conflict->seen);
    if (!occupant)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS ref catalog '{}' disappeared after bootstrap create conflict", key);
    RefCatalog catalog = decodeRefCatalog(occupant->bytes);
    if (!catalog.entries.empty() || occupant->bytes != canonical_empty)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS ref catalog '{}' conflicts with bootstrap's required canonical empty catalog", key);
    return Snapshot{.catalog = std::move(catalog), .etag = occupant->etag,
                    .life_index = CatalogLifeIndex(empty)};
}

std::optional<NamespaceLifeId> CasRefCatalog::lifeIfCataloged(
    CasOperation & op, const Layout & layout, const RootNamespace & ns)
{
    const Snapshot snap = read(op, layout);
    for (const CatalogEntry & entry : snap.catalog.entries)
        if (entry.ns.string() == ns.string() && entry.state != NsState::Creating)
            return snap.life_index.resolve(entry.incarnation);
    return std::nullopt;
}

std::vector<NamespaceLifeId> CasRefCatalog::liveUniverse(CasOperation & op, const Layout & layout)
{
    const Snapshot snap = read(op, layout);
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

/// Thrown from inside a `casUpdate` `mutate` closure to signal a refusal that must STOP the attempt
/// rather than be treated as a refused precondition to retry: `casUpdateImpl` propagates whatever
/// `mutate` throws straight out, uncaught, which is exactly the behavior these three need. Retrying
/// any of them against a freshly re-read catalog would just re-decide against an entry that is, by
/// definition, no longer `observed` -- token-exactness means the FIRST mismatch is final, not a reason
/// to loop. Each is caught by its own exact type right where it is thrown; deriving from
/// `std::exception` is only so the throw itself is well-formed, never so a caller catches these by
/// base class.
struct CatalogFenceMovedMarker : std::exception {};
struct CatalogEntryMismatchMarker : std::exception {};
struct CatalogCreatorStillLiveMarker : std::exception {};

/// Live-lock brake for the ONE loop below that is written by hand rather than driven by the engine:
/// the catalog is a single object mutated by every lifecycle transition of every namespace in the
/// pool, so persistent contention is a real, not theoretical, exit condition to plan for.
///
/// It bounds ATTEMPTS, and is the SECONDARY bound: the loop freezes one `Retry` before it starts and
/// every verb of every iteration shares that absolute deadline, so wall-clock time is already bounded
/// by one standard window. This cap exists so a call that somehow converges on neither still ends.
constexpr size_t kMaxCatalogCasAttempts = 100;

/// Shared body of `casUpdate`/`casAdmitEntry`. `encode` turns a freshly `mutate`d candidate into the
/// bytes to write: the plain path just grammar-checks (`encodeRefCatalog`), the admitting path also
/// runs both admission predicates (`checkCatalogAdmission`) first. A refused precondition re-runs
/// `mutate` against the resolve read's fresh body -- the one the pool's hot-key lane remembers and
/// the next hold starts from -- never re-encoding the stale candidate.
RefCatalog casUpdateImpl(
    CasOperation & op, const Layout & layout,
    const std::function<RefCatalog(const RefCatalog &)> & mutate,
    const std::function<String(const RefCatalog &)> & encode,
    const Retry & policy = Retry::standard())
{
    const String key = layout.refCatalogKey();
    /// The candidate the LAST `decide` produced, which is the one the engine wrote: every earlier one
    /// belongs to an attempt whose precondition was refused.
    std::optional<RefCatalog> written;

    const auto decide = [&](const std::optional<Object> & current) -> std::optional<String>
    {
        /// Absence of the mandatory catalog is corruption, not a fresh bootstrap. Refusing here is
        /// what stops any mutation from replacing every other namespace with a one-update catalog.
        if (!current)
            throwMandatoryCatalogAbsent(key);
        const RefCatalog durable = decodeRefCatalog(current->bytes);
        CatalogLifeIndex(durable).throwIfAmbiguous("CAS ref catalog mutation");
        RefCatalog candidate = mutate(durable);
        String bytes = encode(candidate);
        written = std::move(candidate);
        return bytes;
    };

    /// One hold at a time per pool on this key, from the pool's last known catalog when the lane holds
    /// one. The loop is this function's: a `Conflict` is a lost race against another server (the lane
    /// never conflicts with itself), repaid after the flat jitter, or after the growing schedule when
    /// the conflict settled a transport fault. Under a single-attempt policy the first `Conflict` is
    /// the answer, as the engine's own verb answers it.
    const Retry frozen = op.freeze(policy);
    uint32_t settled_faults = 0;
    for (;;)
    {
        WriteResult result = op.hotKeys().submit(key, op, frozen, decide);
        if (const auto * conflict = std::get_if<Conflict>(&result); conflict && !frozen.single_attempt)
        {
            op.pause(conflict->any_ambiguous ? Retry::backoff(++settled_faults) : Retry::conflictBackoff());
            continue;
        }
        /// The fence can be lost in two places and both mean the same to a lifecycle caller: inside
        /// `decide`, which throws the marker itself, and between two attempts, where the engine
        /// notices it first and no further `decide` runs. Normalising the second onto the first is
        /// what keeps "the fence moved" a returned outcome rather than an exception.
        if (const auto * gave_up = std::get_if<GaveUp>(&result); gave_up && gave_up->why == GaveUp::Why::FenceLost)
            throw CatalogFenceMovedMarker{};
        if (!std::holds_alternative<Committed>(result))
            throwCatalogWriteFailure(std::move(result), fmt::format("CAS ref catalog '{}' update", key));
        return std::move(*written);
    }
}

/// `casUpdate`'s own guard, shared with the two lifecycle callers that need to catch the fence marker
/// the public entry point translates away.
std::function<RefCatalog(const RefCatalog &)> identityPreserving(
    const std::function<RefCatalog(const RefCatalog &)> & mutate)
{
    return [&mutate](const RefCatalog & current) -> RefCatalog
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
}

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

/// Fires once, synchronously, right before `createNamespace`'s own pre-check read -- the window in
/// which a sibling opener of the SAME namespace can complete an entire birth (or begin a removal) that
/// this call's pre-check then observes. Lets a test land that interleaving deterministically instead of
/// relying on real thread scheduling. Empty (no-op) in production, mirroring every other
/// `*_hook_for_test` in this tree.
std::function<void()> create_namespace_pre_check_hook_for_test;

/// Step 1 of `createNamespace`, split out so it can recheck presence on EVERY catalog read this loop
/// performs (the first one, and any `Conflict` retry's re-read), not only the snapshot-in-time read
/// `createNamespace` itself already did before calling in. That single upfront read cannot see a
/// sibling opener's OWN step 1 landing between it and this loop's read; without the recheck here, this
/// loop would blindly insert a second row for the same namespace and let `encodeRefCatalog`'s
/// canonical-order/no-duplicate grammar check abort the process with `LOGICAL_ERROR` for what is, at
/// this call site only, an ordinary race outcome.
RefCatalog createNamespaceStep1(
    CasOperation & op, const Layout & layout, uint64_t gc_shards, const CatalogEntry & entry,
    const Retry & policy)
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
    return casUpdateImpl(op, layout, mutate,
        [&entry, gc_shards, &layout](const RefCatalog & c)
        {
            return checkCatalogAdmission(c, gc_shards, layout, entry.ns);
        },
        policy);
}

}

RefCatalog CasRefCatalog::casUpdate(
    CasOperation & op, const Layout & layout, const std::function<RefCatalog(const RefCatalog &)> & mutate)
{
    try
    {
        return casUpdateImpl(
            op, layout, identityPreserving(mutate), [](const RefCatalog & c) { return encodeRefCatalog(c); });
    }
    catch (const CatalogFenceMovedMarker &)
    {
        /// The marker is this file's private signal; a caller outside it gets the exception class every
        /// other admission refusal raises.
        throwCasTransientUnavailable(
            fmt::format("CAS ref catalog '{}' update", layout.refCatalogKey()),
            "mount fence tripped: the update was admitted under an incarnation this node no longer holds");
    }
}

RefCatalog CasRefCatalog::casAdmitEntry(
    CasOperation & op, const Layout & layout, uint64_t gc_shards, const CatalogEntry & entry)
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
    try
    {
        return casUpdateImpl(op, layout, mutate,
            [&entry, gc_shards, &layout](const RefCatalog & c)
            {
                return checkCatalogAdmission(c, gc_shards, layout, entry.ns);
            });
    }
    catch (const CatalogFenceMovedMarker &)
    {
        /// The marker is this file's private signal; a caller outside it gets the exception class every
        /// other admission refusal raises.
        throwCasTransientUnavailable(
            fmt::format("CAS ref catalog '{}' update", layout.refCatalogKey()),
            "mount fence tripped: the update was admitted under an incarnation this node no longer holds");
    }
}

CasRefCatalog::BeginRemovingOutcome CasRefCatalog::beginRemoving(
    CasOperation & op, const Layout & layout, const CatalogEntry & observed,
    uint64_t removal_started_round)
{
    if (observed.state != NsState::Live || observed.creator || observed.removal_started_round)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::beginRemoving: namespace '{}' is not an exact Live entry",
            observed.ns.string());

    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        if (!op.admitted())
            throw CatalogFenceMovedMarker{};

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
        casUpdateImpl(op, layout, mutate, [](const RefCatalog & c) { return encodeRefCatalog(c); });
    }
    catch (const CatalogFenceMovedMarker &)
    {
        return BeginRemovingOutcome::FencedOut;
    }
    catch (const CatalogEntryMismatchMarker &)
    {
        const Snapshot current = read(op, layout);
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
    CasOperation & op, const Layout & layout, const CatalogEntry & observed,
    const CasFoldSeal & authoritative_parent,
    const std::function<void()> & refresh_authority)
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
        op, layout, read(op, layout), observed, authoritative_parent, refresh_authority);
}

CasRefCatalog::CompletedRemovingDeleteResult CasRefCatalog::deleteCompletedRemovingAtSnapshot(
    CasOperation & op, const Layout & layout, Snapshot catalog_snapshot,
    const CatalogEntry & observed, const CasFoldSeal & authoritative_parent,
    const std::function<void()> & refresh_authority)
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

    /// ONE bound for the whole loop, frozen before the first iteration: every erase and every
    /// resolution read below shares this deadline, so a permanently contended catalog gives up
    /// retry-later within one standard window rather than spending a fresh window per verb per
    /// iteration. The paced retry at the end of the loop is a bare sleep that does not consult the
    /// deadline, so the loop can sleep one backoff (at most 5 s) past it before the next erase refuses
    /// to start.
    const Retry policy = op.freeze(Retry::standard());

    for (size_t attempt = 0; attempt < kMaxCatalogCasAttempts; ++attempt)
    {
        /// So the admission consulted below is not one reading taken before the first attempt.
        if (refresh_authority)
            refresh_authority();

        catalog_snapshot.life_index.throwIfAmbiguous("CAS completed-removal deletion");
        /// A caller-supplied cut without an incarnation cannot state a precondition, and an erase that
        /// fell back to an unconditional write would delete whatever a concurrent writer had put there.
        if (!catalog_snapshot.etag)
            throwMandatoryCatalogAbsent(layout.refCatalogKey());
        const auto observed_it = findEntry(catalog_snapshot.catalog, observed.ns);
        if (observed_it == catalog_snapshot.catalog.entries.end() || *observed_it != observed)
            return resolved_result(CompletedRemovingDeleteOutcome::EntryChanged);

        /// Nothing is attempted without admission, and a refusal here has sent nothing.
        if (!op.admitted())
            return resolved_result(CompletedRemovingDeleteOutcome::FencedOut);

        RefCatalog candidate = catalog_snapshot.catalog;
        candidate.entries.erase(candidate.entries.begin() + (observed_it - catalog_snapshot.catalog.entries.begin()));
        WriteResult erase = op.replace(layout.refCatalogKey(), encodeRefCatalog(candidate),
                                       *catalog_snapshot.etag, policy);

        /// An operation whose admission is gone cannot issue the resolution read either, so the call
        /// ends HERE and reports the cut it was given rather than a fresh one. There is nothing further
        /// this actor may learn, and nothing further it may do.
        if (!op.admitted())
            return resolved_result(CompletedRemovingDeleteOutcome::FencedOut);

        /// The response to a conditional erase is not authority for what became durable. Resolve every
        /// attempted erase through one complete catalog read. This snapshot is also the next
        /// retry/selection cut, so no second read separates them.
        catalog_snapshot = read(op, layout, policy);

        const auto current_it = findEntry(catalog_snapshot.catalog, observed.ns);
        const bool old_life_still_cataloged = current_it != catalog_snapshot.catalog.entries.end()
            && current_it->incarnation == observed.incarnation;
        if (!old_life_still_cataloged)
            return resolved_result(current_it == catalog_snapshot.catalog.entries.end()
                ? CompletedRemovingDeleteOutcome::Deleted
                : CompletedRemovingDeleteOutcome::EntryChanged);

        /// The row survived the attempt. Only a refused precondition may be tried again against the
        /// mandatory resolution snapshot above; every other alternative is terminal for this call, and
        /// a commit the resolution read contradicts is reported rather than believed.
        if (!std::holds_alternative<Conflict>(erase))
        {
            if (std::holds_alternative<Committed>(erase))
                throwCasWriteRetryLater(fmt::format(
                    "CAS ref catalog erase for namespace '{}' reported committed, but a complete resolution read "
                    "still observed incarnation {}",
                    observed.ns.string(), u128ToHex(observed.incarnation)));
            throwCatalogWriteFailure(std::move(erase), fmt::format(
                "CAS ref catalog erase for namespace '{}'", observed.ns.string()));
        }

        /// Only a refused precondition reaches here, so this pause paces one contended key's retries.
        /// The argument is the number of reissues so far, which is one more than the zero-based
        /// iteration: `backoff(0)` is no wait at all, and the first retry is the one most likely to
        /// collide with the writer that just won.
        op.pause(Retry::backoff(static_cast<uint32_t>(attempt) + 1));
    }

    throwCasWriteRetryLater(fmt::format(
        "CAS ref catalog erase for namespace '{}' did not converge after {} attempts",
        observed.ns.string(), kMaxCatalogCasAttempts));
}

CasRefCatalog::StalledCreatingCancelOutcome CasRefCatalog::cancelStalledCreating(
    CasOperation & op, const Layout & layout, const CatalogEntry & observed,
    const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::cancelStalledCreating: namespace '{}' is not a Creating entry with a "
            "creator fence",
            observed.ns.string());

    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        if (!op.admitted())
            throw CatalogFenceMovedMarker{};

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
        casUpdateImpl(op, layout, mutate, [](const RefCatalog & c) { return encodeRefCatalog(c); });
    }
    catch (const CatalogFenceMovedMarker &) { return StalledCreatingCancelOutcome::FencedOut; }
    catch (const CatalogEntryMismatchMarker &) { return StalledCreatingCancelOutcome::EntryChanged; }
    catch (const CatalogCreatorStillLiveMarker &) { return StalledCreatingCancelOutcome::CreatorFenceStillLive; }
    return StalledCreatingCancelOutcome::Cancelled;
}

CasRefCatalog::NamespaceCreationOutcome CasRefCatalog::completeCreation(
    CasOperation & op, const Layout & layout, const CatalogEntry & observed, const Retry & policy)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::completeCreation: namespace '{}' is not a Creating entry with a creator "
            "fence -- steps 2/3 only ever run over one of those", observed.ns.string());

    /// Step 2 (spec §3): INV-4's first `_ckpt` writer for this incarnation, and the only writer that
    /// will ever know its genesis epoch -- see `Pool/CasRefCkpt.h`'s `publishCkpt` doc for the merge
    /// discipline this rides on unchanged. `FencedOut` here ends the attempt without step 3; the
    /// `_ckpt` itself may or may not have become durable, which is why the entry is left `Creating`
    /// for whichever actor next reconciles it rather than cleaned up here.
    const RefCkpt contribution{.life_epoch = std::optional<uint64_t>{observed.creator->writer_epoch},
                                .committed_through = std::nullopt,
                                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    if (publishCkpt(op, layout, NamespaceLifeId::fromCatalogEntry(observed.ns, observed.incarnation),
                     contribution, policy) == CkptPublishOutcome::FencedOut)
        return NamespaceCreationOutcome::FencedOut;

    /// Step 3. `mutate` is the fence re-check point `casUpdate`'s header doc names -- checked FIRST,
    /// mirroring `publishCkpt`'s own "after the read, before the CAS, on every attempt" placement, so a
    /// caller stale on BOTH axes between step 2 and here is reported `FencedOut`, never `Superseded`
    /// (both are truthful refusals of a CAS that was never sent; this is only which one speaks first).
    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        /// Typed, not propagated: the caller asked "did this land", and "the fence moved, so nothing
        /// was sent" is an answer, not a failure of the operation.
        if (!op.admitted())
            throw CatalogFenceMovedMarker{};

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
        casUpdateImpl(op, layout, identityPreserving(mutate),
                      [](const RefCatalog & c) { return encodeRefCatalog(c); }, policy);
    }
    catch (const CatalogFenceMovedMarker &) { return NamespaceCreationOutcome::FencedOut; }
    catch (const CatalogEntryMismatchMarker &) { return NamespaceCreationOutcome::Superseded; }
    return NamespaceCreationOutcome::Live;
}

CasRefCatalog::NamespaceCreationOutcome CasRefCatalog::createNamespace(
    CasOperation & op, const Layout & layout, uint64_t gc_shards,
    const RootNamespace & ns, const CreatorFence & creator, const Retry & policy)
{
    /// Read-first, per the Task 2 review's own note on `casAdmitEntry`: a namespace that already
    /// carries an entry is THIS function's job to notice and report `Superseded` for, not
    /// `casAdmitEntry`'s duplicate-namespace grammar refusal (which would report a `LOGICAL_ERROR` about
    /// canonical order -- true, but useless to a caller whose create merely lost a race). A concurrent
    /// insert of the SAME namespace between this read and step 1 is still caught -- `casAdmitEntry`'s
    /// own grammar check is the backstop, not the only check.
    if (create_namespace_pre_check_hook_for_test)
    {
        std::function<void()> hook_to_run;
        std::swap(hook_to_run, create_namespace_pre_check_hook_for_test);
        hook_to_run();
    }
    const Snapshot snap = read(op, layout, policy);
    const auto existing = findEntry(snap.catalog, ns);
    if (existing != snap.catalog.entries.end())
    {
        /// This read is a snapshot taken AFTER the caller's own "no entry" read (`resolveNamespaceLife`'s
        /// loop, or any other dispatcher that only reaches `createNamespace` once it has seen nothing to
        /// adopt). A sibling opener of the SAME namespace -- concurrent threads of one server: parallel
        /// background movers, inserts, or per-part `FREEZE` -- can land anywhere in its own three-step
        /// sequence in the gap between those two reads, so EVERY state observed here is a race outcome,
        /// never a caller bug: `Creating` (a sibling landed step 1 only), `Live` (a sibling completed all
        /// three steps and already won birth), and `Removing` (a concurrent drop) are all reported
        /// `Superseded`, sending the loser back through its own resume loop rather than aborting the
        /// server for an outcome the design already names and handles. There the loop's fresh re-read
        /// tells the loser what actually happened: `Live` is adopted directly, `Removing` is refused by
        /// the loop's own `Removing` branch, and a still-`Creating` entry resumes through
        /// `reconcileStaleCreator` + `completeCreation` (or, if it is this caller's own fence, straight
        /// through `completeCreation`).
        return NamespaceCreationOutcome::Superseded;
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
        createNamespaceStep1(op, layout, gc_shards, entry, policy);   /// step 1
    }
    catch (const CatalogEntryAlreadyPresentMarker &)
    {
        return NamespaceCreationOutcome::Superseded;
    }
    catch (const CatalogFenceMovedMarker &)
    {
        /// The creator's own admission moved while step 1 waited its turn or wrote: an answer, not a
        /// failure, and the same one the two later steps already give.
        return NamespaceCreationOutcome::FencedOut;
    }
    return completeCreation(op, layout, entry, policy);
}

void CasRefCatalog::setCreateNamespaceStep1PreReadHookForTest(std::function<void()> hook)
{
    create_namespace_step1_pre_read_hook_for_test = std::move(hook);
}

void CasRefCatalog::setCreateNamespacePreCheckHookForTest(std::function<void()> hook)
{
    create_namespace_pre_check_hook_for_test = std::move(hook);
}

CasRefCatalog::ReconcileCreatorOutcome CasRefCatalog::reconcileStaleCreator(
    CasOperation & op, const Layout & layout, const CatalogEntry & observed, const CreatorFence & new_creator,
    const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal, const Retry & policy)
{
    if (observed.state != NsState::Creating || !observed.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CasRefCatalog::reconcileStaleCreator: namespace '{}' is not a Creating entry with a "
            "creator fence -- nothing to reconcile", observed.ns.string());

    /// The admission check comes FIRST, on every fresh read this retries -- the same placement
    /// `completeCreation` uses for exactly the same reason (see that function's own doc).
    /// Entry-exactness (the catalog's own entry, by full value) comes next: it is the cheaper, purely
    /// local comparison, and a mismatch here means the question "is the OLD creator's fence terminal" is
    /// moot -- `observed` no longer describes anything live to reconcile.
    const auto mutate = [&](const RefCatalog & cur) -> RefCatalog
    {
        /// Typed, not propagated: the caller asked "did this land", and "the fence moved, so nothing
        /// was sent" is an answer, not a failure of the operation.
        if (!op.admitted())
            throw CatalogFenceMovedMarker{};

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
        casUpdateImpl(op, layout, identityPreserving(mutate),
                      [](const RefCatalog & c) { return encodeRefCatalog(c); }, policy);
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
