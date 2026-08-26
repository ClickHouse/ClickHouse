#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <functional>
#include <optional>
#include <vector>

namespace DB::Cas
{

/// The `cas/ref_catalog` object (spec INV-3) as seen from the pool side: reading the current
/// catalog, and the generic token-CAS retry primitive every lifecycle transition rides. This class
/// builds ONLY that primitive -- the actual lifecycle steps (the three-conditional-write creation
/// sequence, the removal terminal-record-then-entry-delete sequence) are later tasks' job, built ON
/// TOP of `casUpdate`/`casAdmitEntry`.
class CasRefCatalog
{
public:
    /// The catalog snapshot as read from the backend: the decoded object plus the token an update
    /// must present to `casPut`. Operational reads always return a token because the catalog is a
    /// mandatory control object after pool bootstrap.
    struct Snapshot
    {
        RefCatalog catalog;
        std::optional<Token> token;
        CatalogLifeIndex life_index;
    };

    /// Reads and decodes the mandatory current catalog. Absence is corruption, never an empty
    /// authority set: without the catalog, opaque life keys cannot prove ownership.
    static Snapshot read(Backend & backend, const Layout & layout);

    /// Materializes the explicit empty catalog for a prefix already proven new by
    /// `probePoolBootstrapResidual`. This is the only absence-tolerant catalog operation: no
    /// existing-pool caller can accidentally turn authoritative absence into an empty catalog. A
    /// concurrent bootstrap winner is accepted only after its object is read and decoded.
    static Snapshot initializeEmptyForNewPool(Backend & backend, const Layout & layout);

    /// The catalog's life for `ns` if a `Live`/`Removing` entry names it, else `nullopt` -- ONE catalog
    /// read and, crucially, NO WRITE OF ANY KIND. This is the resolution a READ or a REMOVAL uses: it
    /// answers "this namespace does not exist" instead of making it exist, which is what
    /// `CasRefLedger::resolveNamespaceLife` would do (it mints for an absent entry, correctly, on behalf
    /// of a writer). A caller must not substitute that one here: a read or an unlink against a
    /// never-opened table would then perform a catalog CAS and a `_ckpt` publish, growing the single
    /// pool-wide catalog object -- which is under a capacity-admission predicate -- for a namespace
    /// nobody ever created.
    ///
    /// `Creating` is excluded for the same reason `liveUniverse` excludes it: no publication can exist
    /// under an entry still being created, so there is nothing to resolve to and nothing to read.
    static std::optional<NamespaceLifeId> lifeIfCataloged(
        Backend & backend, const Layout & layout, const RootNamespace & ns);

    /// Every `Live`/`Removing` life the catalog currently names, from this call's own catalog `GET`.
    /// This helper is for independent readers such as `CasFsck`; a GC fold instead keeps the immutable
    /// post-LIST snapshot attached to its scan and reuses that exact cut throughout the round.
    /// `Creating` is excluded: spec §3, no publication can exist yet.
    static std::vector<NamespaceLifeId> liveUniverse(Backend & backend, const Layout & layout);

    /// The generic token-CAS retry loop shared by every catalog mutation, mirroring
    /// `PoolMeta::admitOrValidate`'s loop: read the current snapshot, apply `mutate` to obtain the
    /// CANDIDATE next catalog, `casPut` it against the mandatory object's observed token, and on
    /// `Conflict` re-read and re-apply `mutate` to the
    /// FRESH snapshot -- never re-encoding the stale candidate. `mutate` must return a canonically
    /// ordered, grammar-valid candidate; `encodeRefCatalog` (called internally) enforces that.
    ///
    /// Bounded (the same live-lock brake `publishCkpt`/`allocateWriterEpoch` use on their own
    /// contended token-CAS singletons): after 100 conflicting attempts it gives up and raises the
    /// typed retryable error `throwCasWriteRetryLater`, naming the key and the attempt count, rather
    /// than spinning forever against a pathologically busy catalog.
    ///
    /// A re-read that finds the object genuinely ABSENT after it was previously observed present is
    /// corruption, not a fresh bootstrap. The required `read` throws before another CAS attempt, so
    /// no mutation can replace every other namespace with a one-update catalog.
    ///
    /// This primitive runs NO admission check: Constraint 13 (removal is never refused) means
    /// whether a candidate must clear the additive predicate is the CALLER's decision, not this
    /// loop's. A caller mutating an entry's state without growing the catalog (a removal transition)
    /// uses this directly.
    ///
    /// THE FENCE OBLIGATION (Task 3 carry-over from the Task 2 review): this loop has no fence
    /// parameter and performs no fence check of its own -- `publishCkpt`'s "AFTER the read, BEFORE the
    /// CAS, on every attempt" discipline has no equivalent built in here. The seam a fenced caller
    /// MUST use is `mutate` itself: it runs, fresh, after EVERY read this loop performs (the very
    /// first one and every one after a `Conflict`), immediately before the candidate it returns is
    /// encoded and `casPut`. A caller that needs its own write fenced (any catalog mutation minted
    /// under a mount incarnation -- which is every one Task 3 onward adds) MUST throw from inside
    /// `mutate`, checking on EVERY invocation, not once before calling `casUpdate`: checking once
    /// before the call fences against the read this loop is *about* to perform, not the one it just
    /// did, and a `Conflict` retry performs an entirely new read `mutate` is never told about except
    /// by being called again. `completeCreation`'s own `mutate` (below) is the first production
    /// caller to ride this seam, and does so by wrapping its `check_fence_or_throw` call at the top of
    /// its `mutate`, exactly where `publishCkpt` places the identical check.
    static RefCatalog casUpdate(
        Backend & backend, const Layout & layout,
        const std::function<RefCatalog(const RefCatalog &)> & mutate);

    /// Admits exactly ONE new namespace into the catalog under INV-3's two-predicate gate, inserting
    /// `entry` at its canonical (ns-sorted) position and running the SAME bounded `casUpdate` retry
    /// loop. Takes the entry to insert rather than an arbitrary mutation, by design: an admission
    /// entry point that accepted a free-form candidate could be handed a REMOVAL by a future caller
    /// that reads as correct, silently reopening Constraint 13 (removal is never refused) behind a
    /// name that says "admitting". A namespace `entry.ns` already carries an entry is a bug in the
    /// caller (Task 3's creation lifecycle owns checking that first) and surfaces as
    /// `encodeRefCatalog`'s own canonical-order/no-duplicate grammar check, inside
    /// `checkCatalogAdmission`.
    static RefCatalog casAdmitEntry(
        Backend & backend, const Layout & layout, uint64_t gc_shards, const CatalogEntry & entry);

    enum class BeginRemovingOutcome : uint8_t
    {
        Transitioned,
        AlreadyRemoving,
        EntryChanged,
        FencedOut,
    };

    /// Exact `Live -> Removing` transition. The immutable observed row is compared by full value on
    /// every catalog retry, and the mount fence is checked after every fresh read and before its CAS.
    /// A row already `Removing` under the same namespace/life resolves an ambiguous or concurrent
    /// transition positively; no caller may change its recorded start round afterward.
    static BeginRemovingOutcome beginRemoving(
        Backend & backend, const Layout & layout, const CatalogEntry & observed,
        uint64_t removal_started_round, uint64_t admitted_generation,
        const std::function<void(uint64_t)> & check_fence_or_throw);

    /// Outcome of the only fold-authorized catalog deletion. A refusal never writes the catalog.
    enum class CompletedRemovingDeleteOutcome : uint8_t
    {
        Deleted,
        ProofRefused,
        EntryChanged,
        FencedOut,
    };

    /// Authority result for completed-removal erases. Only an explicit `Moved` is a fence outcome;
    /// exceptions mean authority could not be evaluated and propagate to the caller.
    enum class LeaderFenceStatus : uint8_t
    {
        Held,
        Moved,
    };

    struct CompletedRemovingDeleteResult
    {
        CompletedRemovingDeleteOutcome outcome;
        /// Present only when a mandatory fresh catalog read proves that the exact observed life is no
        /// longer cataloged, whether this actor's erase committed or another actor removed/replaced it.
        std::optional<NamespaceLifeId> invalidated_life;
        /// The complete mandatory resolution snapshot after an attempted erase. The GC drain feeds
        /// this directly into its next deterministic selection; proof refusal performs no read and
        /// leaves it absent.
        std::optional<Snapshot> catalog_snapshot;

        bool operator==(CompletedRemovingDeleteOutcome expected) const { return outcome == expected; }
    };

    /// Exact-CAS-deletes `observed` only when it is a complete `Removing` row and the authoritative
    /// adopted parent carries cleanup evidence, but no hold, in the row keyed by the same opaque life
    /// id. The whole parent seal is consumed so a caller cannot separate the life id from its proof or
    /// reduce the proof to a caller-computed boolean. The leader fence is checked after every fresh
    /// catalog read and before every attempted CAS.
    static CompletedRemovingDeleteResult deleteCompletedRemoving(
        Backend & backend, const Layout & layout, const CatalogEntry & observed,
        const CasFoldSeal & authoritative_parent, uint64_t admitted_generation,
        const std::function<LeaderFenceStatus(uint64_t)> & check_fence);

    /// Same exact deletion, using the caller's complete selected catalog snapshot and token for the
    /// one CAS attempt. Its mandatory resolution snapshot is returned in the result so a catalog-only
    /// drain can select the next row without an intervening read.
    static CompletedRemovingDeleteResult deleteCompletedRemovingAtSnapshot(
        Backend & backend, const Layout & layout, Snapshot catalog_snapshot,
        const CatalogEntry & observed, const CasFoldSeal & authoritative_parent,
        uint64_t admitted_generation,
        const std::function<LeaderFenceStatus(uint64_t)> & check_fence);

    /// Outcome of exact stalled-creation cancellation, the only other exported deletion shape.
    enum class StalledCreatingCancelOutcome : uint8_t
    {
        Cancelled,
        CreatorFenceStillLive,
        EntryChanged,
        FencedOut,
    };

    /// Exact-CAS-deletes one observed `Creating` row only after its complete creator fence is proven
    /// terminal. This performs no `_ckpt` or other physical cleanup; debris belongs to the janitor.
    static StalledCreatingCancelOutcome cancelStalledCreating(
        Backend & backend, const Layout & layout, const CatalogEntry & observed,
        const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal,
        uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw);

    /// === Task 3: the §3 creation lifecycle, built on the two primitives above ===

    /// Outcome of the two-step tail every creation attempt ends in (`_ckpt` publish + `Creating ->
    /// Live` CAS) -- shared by a fresh `createNamespace` and a reconciler that just adopted a stalled
    /// entry via `reconcileStaleCreator`, since both resume from the identical point (an OBSERVED
    /// `Creating` entry with a live creator identity that is now THIS caller's own).
    enum class NamespaceCreationOutcome : uint8_t
    {
        Live,        /// the entry reached `Live`; `_ckpt` is durable with this creator's `writer_epoch`
                      /// as `life_epoch` (spec INV-4: the genesis epoch, recorded nowhere else).
        FencedOut,   /// this caller's OWN admitted generation moved before the `_ckpt` publish or the
                      /// `Creating -> Live` CAS. Nothing more was written; the caller's own mount
                      /// incarnation is gone, so it cannot be the one to retry.
        Superseded,  /// the catalog entry no longer equals what this caller observed -- a concurrent
                      /// reconciler stole it, or a race already carried it to `Live`/`Removing`. Nothing
                      /// was written; a DIFFERENT actor now owns whatever happens to this namespace next.
    };

    /// Outcome of `reconcileStaleCreator` alone (see below) -- kept distinct from
    /// `NamespaceCreationOutcome` because the two refusal reasons here are not interchangeable with
    /// "fenced" / "superseded": one is "not yet permitted" (retry later, unconditionally on the SAME
    /// entry), the other is "someone else already moved this entry" (retrying against the SAME
    /// `observed` value can never succeed; the caller must re-read first).
    enum class ReconcileCreatorOutcome : uint8_t
    {
        Reconciled,          /// `creator` is now `new_creator`; the caller may proceed as if it had
                              /// just run step 1 itself, over the SAME (unchanged) incarnation.
        CreatorFenceStillLive,  /// `is_creator_fence_terminal` said no -- the stalled creator might
                              /// still complete this itself. Not written; retry later against a FRESH
                              /// terminality read, not immediately.
        EntryChanged,         /// the catalog's current entry for `observed.ns` no longer equals
                              /// `observed` -- token-exactness failed. Not written; the caller must
                              /// re-read the catalog before trying again.
        FencedOut,            /// review I6: this caller's OWN admitted generation moved before the CAS
                              /// -- nothing was written, and the caller's own mount incarnation is gone,
                              /// so it cannot be the one to retry. Mirrors `NamespaceCreationOutcome::
                              /// FencedOut`; without this check a deposed mount could still steal a
                              /// `Creating` entry onto its own dead fence before the following
                              /// `completeCreation` refuses it -- the catalog would be mutated by an
                              /// actor this subsystem otherwise never lets touch it.
    };

    /// The full, fresh §3 sequence for a namespace that carries NO catalog entry yet: mints a random
    /// nonzero incarnation (spec: "fresh_random_128"), runs step 1 (`casAdmitEntry` inserting `{ns,
    /// Creating, incarnation, creator}`), then steps 2+3 via `completeCreation` below.
    ///
    /// Per the Task 2 review's own note on `casAdmitEntry` ("a namespace `entry.ns` already carries an
    /// entry is a bug in the caller -- Task 3's creation lifecycle owns checking that first"): this
    /// function reads the catalog FIRST rather than handing `casAdmitEntry` a doomed insert and letting
    /// its own grammar check report a confusing duplicate-namespace message. A namespace already
    /// `Creating` is not this function's problem to solve -- that is exactly what `reconcileStaleCreator`
    /// + `completeCreation` are for, so this reports `Superseded` (never `LOGICAL_ERROR`) and sends the
    /// caller back through its own resume loop: sibling openers of the same namespace that all observed
    /// "no entry" before any of them landed step 1 race in here exactly this way. A namespace already
    /// `Live`/`Removing` IS a caller bug (recreating an existing name is removal's business, not
    /// creation's) and still throws `LOGICAL_ERROR` naming the observed state.
    static NamespaceCreationOutcome createNamespace(
        Backend & backend, const Layout & layout, uint64_t gc_shards,
        const RootNamespace & ns, const CreatorFence & creator,
        uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw,
        const CkptDeadline & deadline);

    /// Fires once, synchronously, right after `createNamespace`'s own pre-check read observed no
    /// entry and right before its step 1 performs its own (first) catalog read -- the exact window a
    /// sibling opener of the same namespace can land its own step 1 in. Lets a test drive that
    /// interleaving deterministically instead of relying on real thread scheduling. Empty (no-op) hook
    /// in production; a stateless class-scope hook (rather than an instance member) because
    /// `CasRefCatalog` itself carries no state.
    static void setCreateNamespaceStep1PreReadHookForTest(std::function<void()> hook);

    /// Steps 2 (`_ckpt` publish) + 3 (`Creating -> Live` CAS) alone, given an entry the caller already
    /// owns as `observed` -- either the entry `createNamespace`'s own step 1 just inserted, or one a
    /// caller just reconciled onto itself via `reconcileStaleCreator`. Exposed separately (rather than
    /// folded invisibly into `createNamespace`) because reconciliation resumes exactly HERE, never
    /// re-running step 1.
    ///
    /// Step 2: `publishCkpt` with a contribution carrying `observed.creator->writer_epoch` as
    /// `life_epoch` -- INV-4's genesis record; `observed.creator` must be present (i.e. `observed.state
    /// == Creating`), enforced with `LOGICAL_ERROR` since a caller reaching here with anything else is
    /// this module's own bug, not a race. A `FencedOut` from `publishCkpt` ends the attempt here.
    ///
    /// Step 3: `CasRefCatalog::casUpdate`'s `mutate` is the fence re-check point (see the class-level
    /// note below) -- `check_fence_or_throw(admitted_generation)` runs FIRST, on every fresh read this
    /// retry loop performs, exactly like `publishCkpt`'s own re-check; a throw from it is caught and
    /// reported as `FencedOut`, nothing else. ONLY THEN is the fresh entry for `observed.ns` compared
    /// against `observed` by FULL VALUE equality (`CatalogEntry::operator==`) -- the value-CAS that
    /// plays the role `publishCkpt`'s object token plays for `_ckpt`, since one catalog object holds
    /// every namespace's entry and there is no separate per-entry token to CAS against. A mismatch
    /// (stolen by a concurrent reconciler, or already carried to `Live`/`Removing`) is `Superseded`,
    /// caught before any CAS is attempted -- not a retry against fresh state, because retrying here
    /// would mean re-deciding against an entry that is no longer `observed`, which is precisely what
    /// token-exactness forbids. Ordering the fence check before the entry check is deliberate (mirrors
    /// `publishCkpt`); a caller that manages to make BOTH stale sees `FencedOut`, not `Superseded` --
    /// both are truthful refusals of a CAS that was never sent.
    static NamespaceCreationOutcome completeCreation(
        Backend & backend, const Layout & layout, const CatalogEntry & observed,
        uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw,
        const CkptDeadline & deadline);

    /// Stale-`Creating` reconciliation (spec INV-3: "stalled creators occupy entries until
    /// fence-terminal reconciliation"; TLA Task 3 obligation 1: "the call-site is where
    /// token-exactness is enforced"). `observed` must be a `Creating` entry this caller read a moment
    /// ago (`LOGICAL_ERROR` otherwise -- a caller mistake, not a race). Refuses, WITHOUT writing
    /// anything, unless BOTH hold against a FRESH catalog read:
    ///   - `is_creator_fence_terminal(*observed.creator)` -- injected rather than reaching into
    ///     `CasServerRoot` directly, so this module (and its tests) stay independent of the mount-lease
    ///     machinery; the real predicate a production caller wires in is `isCreatorFenceTerminal`
    ///     (`Pool/CasServerRoot.h`, called as `isCreatorFenceTerminal(backend, layout,
    ///     fence.server_root_id, fence.writer_epoch)` -- it takes those two scalars, not the whole
    ///     `CreatorFence`, so the mount layer stays independent of the ref-catalog format), built from
    ///     `writer_epoch` plus the mount-terminality certificates
    ///     `probeNonTerminalMountSlots`/`computeHeartbeatFloor` already use -- NEVER from
    ///     `CreatorFence::fence_generation`. That field IS persisted (Task 2 serializes it into the
    ///     catalog entry), so it reaches the object store fine; what it is NOT is comparable across
    ///     actors: it mirrors `CasMountRuntime::fence_generation`, an in-process atomic that each mount
    ///     bumps from its OWN zero on every open, so a different actor's counter (or the SAME actor's
    ///     after a restart) starts over at the same values and answers a different question than "is
    ///     the incarnation that minted this entry still alive";
    ///   - the catalog's CURRENT entry for `observed.ns` still equals `observed` exactly
    ///     (token-exactness: a concurrent reconciler, or the original creator finishing on its own,
    ///     invalidates this immediately).
    /// On success, CASes `creator` to `new_creator` -- `state` and `incarnation` are UNCHANGED, so the
    /// caller resumes with `completeCreation(backend, layout, {..., .creator = new_creator}, ...)` over
    /// the SAME incarnation, never a fresh one (rebirth under a fresh incarnation is Task 5/removal's
    /// business, not a live reconciliation's).
    ///
    /// `admitted_generation`/`check_fence_or_throw` (review I6): re-checked FIRST on every fresh read
    /// this CAS retries, exactly like `completeCreation`'s own placement -- a caller whose OWN mount
    /// fence has already moved must not be the one to steal a `Creating` entry onto its own (now dead)
    /// fence, even though the following `completeCreation` would go on to refuse it as `FencedOut`
    /// anyway: by then the catalog would already have been mutated by a deposed actor, the one posture
    /// this subsystem otherwise refuses everywhere else.
    static ReconcileCreatorOutcome reconcileStaleCreator(
        Backend & backend, const Layout & layout, const CatalogEntry & observed,
        const CreatorFence & new_creator,
        const std::function<bool(const CreatorFence &)> & is_creator_fence_terminal,
        uint64_t admitted_generation, const std::function<void(uint64_t)> & check_fence_or_throw);

    /// Spec §3: "`Creating` forbids publication -- no ref writes admitted while the entry is
    /// Creating." Throws `throwCasWriteRetryLater`'s class (transient: `Creating` resolves once the
    /// creator finishes or is reconciled away) if `catalog`'s entry for `ns` is `Creating`; a no-op for
    /// every other case -- no entry, `Live`, or `Removing` -- since this is ONLY the birth-lifecycle
    /// gate on the catalog's own `Creating` state, never a general existence/removal check (that role
    /// moves onto the catalog in Task 4/Task 6). Takes an already-read `RefCatalog` rather than
    /// `Backend`/`Layout`, so a caller that is about to append anyway (and so already holds a fresh
    /// read for its OWN purposes) pays no second GET here.
    ///
    /// Production publication is catalog-governed by construction: an ordinary `appendRefOps` first
    /// resolves the namespace lifecycle, refuses a foreign live `Creating` row without writing, and
    /// cannot publish the initial stream object until creation has published `_ckpt` and moved the row
    /// to `Live`. This helper states the same admission rule for callers that already own a catalog cut;
    /// it is not the production append path's enforcement seam.
    static void checkPublicationAdmittedOrThrow(const RefCatalog & catalog, const RootNamespace & ns);
};

}
