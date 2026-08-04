#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowMap.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowManifestSet.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <base/types.h>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::Cas
{

/// Reverse catalog index built from one decoded catalog cut. Every lifecycle state participates.
/// A duplicated physical id remains represented as ambiguous so reporting tools can continue over
/// unrelated unique ids; no caller can accidentally obtain a first-row-wins resolution.
class CatalogLifeIndex
{
public:
    explicit CatalogLifeIndex(const RefCatalog & catalog);

    /// Unique logical life for `life_id`, absence for an id not present in the cut, and
    /// `CORRUPTED_DATA` when multiple current rows share the id.
    std::optional<NamespaceLifeId> resolve(NamespaceLifePhysicalId life_id) const;
    bool isAmbiguous(NamespaceLifePhysicalId life_id) const;
    bool hasAmbiguity() const { return !ambiguous_names.empty(); }

    /// Destructive and catalog-mutating consumers require the whole cut to be unambiguous.
    void throwIfAmbiguous(std::string_view consumer) const;

private:
    std::map<NamespaceLifePhysicalId, NamespaceLifeId> unique_lives;
    std::map<NamespaceLifePhysicalId, std::vector<String>> ambiguous_names;
};

// Shared value types for the ref-ledger writer and the pure ref-log protocol helpers below. They live
// in this protocol header so the ledger can depend on the carriers without making the pool and ledger
// headers include one another.
/// Whether a root-shard mutation originates from the writer path (user-visible publish/drop/precommit)
/// or from GC/maintenance. Diagnostic-only (`toString`, event logging): recorded on the mutation item.
enum class RootMutationOrigin : uint8_t
{
    Writer,
    Gc,
};

/// The write-scope of one `appendRefOps` call (ref-append-lane batching): which part of the table the
/// call touches. The flat-combining batch builder admits at most ONE mutation per ref name into a
/// single flush (per-ref durable histories stay bit-identical to the unbatched protocol) and flushes
/// `WholeShard` calls SOLO (dropNamespace and anything touching multiple refs wholesale).
struct MutationScope
{
    enum class Kind : uint8_t { Ref, WholeShard };
    Kind kind = Kind::WholeShard;
    String ref_name;   /// set iff kind == Ref

    /// Creates a scope for a mutation that touches exactly one ref name. The name is moved into the
    /// scope because scopes are normally assembled as part of an append request.
    static MutationScope ref(String name) { return {Kind::Ref, std::move(name)}; }

    /// Creates a scope for a mutation that cannot safely share a batch with per-ref mutations.
    static MutationScope wholeShard() { return {Kind::WholeShard, {}}; }
};

/// Kind of mutation being applied, used in diagnostic logging and metrics. Does not affect behaviour.
enum class RootMutationKind : uint8_t
{
    Publish,
    Drop,
    Precommit,
    Promote,
    Abandon,
    UpdateRefPublishedAt,
    DropNamespace,
    ReclaimPrecommit,
};

/// Human-readable name for `RootMutationOrigin` (diagnostic logging).
inline std::string_view toString(RootMutationOrigin origin)
{
    switch (origin)
    {
        case RootMutationOrigin::Writer: return "Writer";
        case RootMutationOrigin::Gc:     return "Gc";
    }
    return "Unknown";
}

/// Human-readable name for `RootMutationKind` (diagnostic logging).
inline std::string_view toString(RootMutationKind kind)
{
    switch (kind)
    {
        case RootMutationKind::Publish:           return "Publish";
        case RootMutationKind::Drop:              return "Drop";
        case RootMutationKind::Precommit:         return "Precommit";
        case RootMutationKind::Promote:           return "Promote";
        case RootMutationKind::Abandon:           return "Abandon";
        case RootMutationKind::UpdateRefPublishedAt: return "UpdateRefPublishedAt";
        case RootMutationKind::DropNamespace:     return "DropNamespace";
        case RootMutationKind::ReclaimPrecommit:  return "ReclaimPrecommit";
    }
    return "Unknown";
}

/// The result of resolving a ref: its namespace-qualified manifest identity, the manifest size, and
/// the publication timestamp carried by the ref. A `Resolved` value does not own the manifest body.
struct Resolved
{
    /// The namespace-qualified identity of the part manifest this ref names. The owning RootNamespace
    /// + the ref's manifest_ref form the ManifestId (the ref carries no namespace itself — that comes
    /// from the owning root context).
    ManifestId manifest_id;
    uint64_t manifest_size = 0;
    uint64_t published_at_ms = 0;   /// publish wall-clock (epoch ms); 0 = unset
};

/// The non-identity portion of a committed-ref update. The ref's manifest identity is deliberately
/// absent: changing reachability must use an owner transition, while this carrier is only for updating
/// the publication timestamp without changing the manifest edge. In the current all-tree
/// representation, per-part files are ordinary manifest entries rather than a separate mutable-file
/// map, so `published_at_ms` is the remaining metadata that can be restamped in isolation.
struct RefPublishedAtUpdate
{
    uint64_t published_at_ms = 0;   /// publish wall-clock (epoch ms); 0 = unset
};


/// Counts the committed refs and precommit bindings named by one namespace-removal transaction. The
/// transaction contains one exact owner-removal operation for each count before its final
/// `remove_namespace` operation; callers interested only in completion may ignore this summary.
struct DropNamespaceStats
{
    uint64_t committed_refs = 0;
    uint64_t precommits = 0;
};

/// Per-owner configuration passed by value to the ref ledger. It is a projection of the flat pool
/// configuration: `server_root_id` is used in ref-lane diagnostics, while boot time and wait-sleep
/// callbacks remain owned by the pool and are supplied separately because they describe live mount
/// state rather than ref-ledger policy.
struct RefLedgerConfig
{
    String server_root_id;
    uint64_t gc_shards = 1;
    uint64_t snapshot_log_count_threshold = 256;
    uint64_t snapshot_log_bytes_threshold = 1ULL << 20;
    uint64_t snapshot_publish_backoff_initial_ms = 200;
    uint64_t snapshot_publish_backoff_max_ms = 30000;
    uint64_t precommit_sweep_backoff_initial_ms = 200;
    uint64_t precommit_sweep_backoff_max_ms = 30000;
    uint64_t ref_table_cache_bytes = 256ULL << 20;
};

/// The ONE id-derivation rule (INV-1): the id that continues `greatest_applied`'s stream under
/// `live_epoch`. Within an epoch a table's ids are dense -- the successor of the greatest applied one --
/// and an epoch change restarts the sequence at 1, because density is a property of `(namespace, epoch)`
/// and a fresh incarnation's stream is a fresh stream. There is no counter anywhere: the id is a pure
/// function of the state it will be applied to, which is what makes an attempt that sent nothing
/// consume nothing (the next caller derives the same id from the same unchanged state).
///
/// This is the rule itself, applied to an ANCHOR. Callers do not choose the anchor: they go through
/// `RefTableState::nextTxnId`, which derives from `greatest_applied` -- the writer to
/// mint an id, every trial preview to stamp its throwaway transaction, and `applyTxnInPlace` to decide
/// whether a transaction's id is admissible. Sharing one rule is deliberate: an allocator and a checker
/// that each spell it out separately can drift, and a drift here is either a durable hole or a table
/// that refuses its own writes.
///
/// Total by construction (no throw): the one input it cannot serve, an exhausted `ref_sequence` under
/// the live epoch, would need 2^64 transactions in one incarnation of one table. Should a corrupt
/// persisted snapshot ever seed such a state, the wrap produces a `ref_sequence` of 0, which
/// `applyTxnInPlace`'s strict-increase precondition rejects before anything is written.
RefTxnId nextRefTxnId(RefTxnId greatest_applied, uint64_t live_epoch);

/// The in-memory table state: `TableState = Replay(S_X.state, tail(X))`. This class, `applyRefLogTxn`, `snapshotOf`, and
/// `replay` are the ONE shared implementation of that equation -- used verbatim by the writer, its
/// own recovery path, `fsck`, and snapshot construction, so every consumer agrees on what a
/// transaction sequence means. "Namespace" and "table" name the same entity throughout this file
/// (and its callers): one `RefTableState` per `RootNamespace`.
///
/// Representation note (never-born vs removed): there is no separate "first birth" flag. Both
/// "never born" and "Removed" default `lifecycle` to `RefLifecycle::Removed`; they are told apart by
/// `remove_txn_id`: absent means the namespace has never completed a `remove_namespace` transaction
/// (either truly never born, or -- from this class's point of view -- indistinguishable from it,
/// which is fine because a `namespace_birth` op is legal from EITHER case and nothing else is legal
/// from either). Present means a real removal happened and recorded its `RefTxnId`. `committed` and
/// `precommits` are always empty while `lifecycle == Removed` (an invariant `applyRefLogTxn`
/// maintains: `remove_namespace` only fires once both are already empty, and no other operation is
/// legal until the next `namespace_birth`).
class RefTableState
{
public:
    RefTableState() = default;

    RefLifecycle getLifecycle() const { return lifecycle; }
    const std::optional<RefTxnId> & getRemoveTxnId() const { return remove_txn_id; }
    const RefTxnId & getGreatestApplied() const { return greatest_applied; }
    const RefCowMap & getCommitted() const { return committed; }
    const std::set<std::pair<String, ManifestRef>> & getPrecommits() const { return precommits; }
    uint64_t getSnapshotBodyBytes() const { return snapshot_body_bytes; }
    uint64_t getRemovalBodyBytes() const { return removal_body_bytes; }

    /// The id this state's next transaction must carry (INV-1) — the ONE derivation, called by the
    /// writer to mint an id, by every trial preview to stamp its throwaway transaction, and by
    /// `applyTxnInPlace` to decide whether a transaction's id is admissible. Three callers, one rule:
    /// an allocator and a checker that each spell it out separately can drift, and a drift here is
    /// either a durable hole or a table that refuses its own writes.
    ///
    RefTxnId nextTxnId(uint64_t live_epoch) const
    {
        return nextRefTxnId(greatest_applied, live_epoch);
    }

    /// State-install point only (once per ref-log flush, never per batch item): folds the committed
    /// map's and the owned-manifest index's COW overlays into their bases -- in place when the base is
    /// uniquely owned (the production flush case), else into a fresh base (see each container's
    /// `materialize`).
    void materializeCommitted() { committed.materialize(); owned_manifests.materialize(); }

    /// Member-wise swap, guaranteed non-throwing and allocation-free: every member's own swap is both
    /// (`shared_ptr::swap`, `std::map::swap`, `std::set::swap`, `std::optional::swap` over a trivially
    /// swappable payload, plus PODs). This is the ONLY sanctioned way to install a prepared candidate
    /// state after its transaction is already durable -- see `CasRefLedger::commitRefChunk`'s
    /// post-durable install region, which runs under `DENY_ALLOCATIONS_IN_SCOPE`. Move-assignment would
    /// ALSO be `noexcept` today, but it would destroy the displaced state (freeing every `precommits`
    /// node) INSIDE that region; a swap hands the old state back to the caller, which destroys it
    /// outside. That destruction is not merely a tidiness matter: the old state still shares the COW
    /// bases, and `materializeCommitted` folds in place only while they are uniquely owned.
    void swap(RefTableState & other) noexcept;

private:
    RefLifecycle lifecycle = RefLifecycle::Removed;   /// see representation note above
    std::optional<RefTxnId> remove_txn_id;
    RefTxnId greatest_applied{};                       /// {0, 0} = no transaction applied yet

    RefCowMap committed;                                              /// keyed by ref_name
    std::set<std::pair<String, ManifestRef>> precommits;             /// (ref_name, manifest_ref)

    /// COW membership index of every `ManifestRef` with a current owner (a `committed` row or a
    /// `precommits` binding), maintained O(1) per applied op by every arm of `applyOwnerTransition`
    /// and `stateFromSnapshot` that changes ownership. Gives `manifestAlreadyOwned` O(1) instead of
    /// a linear scan over `committed` + `precommits`. See Pool/CasRefCowManifestSet.h.
    RefCowManifestSet owned_manifests;

    /// Running byte totals of the two admission-budget encodings' *bodies* (row/op lines only, no
    /// header/meta/trailer framing), maintained O(1) per applied op by `applyOp` and seeded by
    /// `stateFromSnapshot`. A pure function of `(committed, precommits)`: `admits` reads
    /// `framing + total` instead of re-encoding the whole table. See `admits`'s doc for why this is
    /// byte-exact rather than a drift-prone estimate.
    uint64_t snapshot_body_bytes = 0;   /// Σ committedRowEncodedSize + Σ precommitRowEncodedSize
    uint64_t removal_body_bytes  = 0;   /// Σ removalOpEncodedSize(one per committed + one per precommit)

    /// One operation's local preconditions and effect, shared by `applyRefLogTxn`'s per-op loop and by
    /// `admits`'s single-op preview. `txn_id` is only read by `RemoveNamespace` (it becomes the
    /// resulting `remove_txn_id`). Validation is identical no matter which apply strategy reaches here
    /// (see `applyTxnInPlace`), so this takes no mode. Was free `applyOpInPlace`.
    void applyOp(const RefOp & op, const RefTxnId & txn_id);

    /// The `owner_transition` op kind: dispatches on the `(old_binding, new_binding)` shape to one of
    /// the four legal transitions (add precommit / remove precommit / remove committed / promote). Any
    /// other shape is not a recognized transition. The add-precommit arm's cross-owner uniqueness check
    /// runs unconditionally (it is O(1) via `owned_manifests`). Was free.
    void applyOwnerTransition(const RefOp & op);

    /// The `set_published_at` op kind: the committed ref must still name `expected_manifest_ref`;
    /// replaces `published_at_ms` without touching the manifest edge. Was free.
    void applySetPublishedAt(const RefOp & op);

    /// Applies the COMPLETE transaction to `*this` IN PLACE (the two txn-wide preconditions first, then
    /// every op in array order), or throws `CORRUPTED_DATA` -- leaving `*this` PARTIALLY APPLIED
    /// ("poisoned") on any throw. This is the poisoning apply strategy: it is sound ONLY on a state the
    /// caller discards on any throw. It is deliberately private and reachable from OUTSIDE this
    /// translation unit at exactly ONE place -- `replay`, its `friend`, which builds its `RefTableState`
    /// locally and returns it only after the WHOLE tail succeeds (any throw destroys that local state
    /// during unwinding, so no consumer ever observes a poisoned state). The public
    /// `applyRefLogTxn` reaches it too, but only through a scratch copy that turns it into the strong
    /// guarantee "throw => the caller's `state` is byte-for-byte unchanged". No caller can express the
    /// dangerous combination -- poison a live state that must survive a throw -- because the poisoning
    /// path is structurally unreachable except via `replay`.
    void applyTxnInPlace(const RefLogTxn & txn);

    /// True iff `manifest_ref` already names an existing committed row or precommit binding under ANY
    /// ref_name (the add-precommit rule: "no conflicting owner may name the same manifest"). Was free.
    bool manifestAlreadyOwned(const ManifestRef & manifest_ref) const;

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Debug/sanitizer-only: recompute both body totals from scratch and assert the incrementally
    /// maintained values match. Was free.
    void debugAssertBodyCounters() const;
#endif

    friend void applyRefLogTxn(RefTableState & state, const RefLogTxn & txn);
    friend RefTableState stateFromSnapshot(const RefTableSnapshot & snapshot);
    friend RefTableState replay(const std::optional<RefTableSnapshot> & snapshot, std::span<const RefLogTxn> tail);
    friend bool admits(const RefTableState & state, const RefOp & op,
                       uint64_t snapshot_budget, uint64_t removal_budget);
    /// The streaming generalisation of `replay`: reaches the same private in-place poisoning path
    /// (`applyTxnInPlace`) on its own discard-on-throw candidate, one decoded transaction at a time.
    friend class RefReplayBuilder;
};

/// The inverse of `snapshotOf`: state from a snapshot's rows. `replay` may receive a hand-built
/// `RefTableSnapshot` that never passed through `decodeRefTableSnapshot`, so this round-trips it
/// through the codec's own `encodeRefTableSnapshot`/`decodeRefTableSnapshot` rather than
/// re-implementing a second, independently-maintained copy of its validation (sortedness, no
/// duplicates, canonical names, nonzero ids, and `manifest_ref` field validity) that could silently
/// miss a case. A decoded snapshot always constructs a `Live` runtime state; terminal lifecycle and
/// its removal evidence exist only in replayed log state. Concretely: a hand-built snapshot with two committed
/// rows sharing one `ref_name` would otherwise DROP the second row via `RefCowMap::emplace` below
/// (same no-overwrite-on-existing-key semantics as `std::map::emplace`) -- the same phantom-alive
/// class of bug as a promote's silent displacement (see `applyOwnerTransition` above), just reached
/// through snapshot loading instead of a transaction.
///
/// One check this does that the codec does NOT: cross-owner manifest uniqueness. `CasRefSnapshotCodec`
/// only enforces sortedness and no-duplicate `ref_name` (committed) / `(ref_name, manifest_ref)`
/// (precommits); it never checks that a `ManifestRef` has at most one owner across committed rows and
/// precommits. A snapshot naming one manifest under two owners is semantically corrupt (it would
/// double-count GC's `+1/-1` edges and violate the add-precommit uniqueness invariant `applyRefLogTxn`
/// enforces), so as each row is loaded this throws `CORRUPTED_DATA` if the manifest already has an
/// owner. This is the one place that enforces it; `owned_manifests.insert` would also throw, but the
/// explicit check here reports "corrupt snapshot data" rather than the container's "index drifted =
/// code bug" framing, which is the accurate diagnosis for a malformed persisted snapshot.
///
/// Promoted from `CasRefProtocol.cpp`'s anonymous namespace to the public protocol API: the ONE
/// validated way to construct a state from rows -- tests and benchmarks use it instead of poking
/// fields.
RefTableState stateFromSnapshot(const RefTableSnapshot & snapshot);

/// Applies the COMPLETE transaction to `state`, or throws `CORRUPTED_DATA` (each transition shape
/// below has exactly one precondition enforced here) -- with the STRONG exception guarantee: a throw
/// anywhere leaves `state` byte-for-byte unchanged. The txn-wide preconditions (a `txn_id` that is the
/// contiguous successor, `remove_namespace` ordering) are checked before any mutation, and the
/// whole apply runs two-phase against a scratch copy that replaces `state` only once the WHOLE
/// transaction has succeeded, so no intra-transaction intermediate state (e.g. a manifest with its
/// precommit already gone but its committed binding not yet installed) is ever observable to a caller
/// -- matching the promote rule: "There is no moment at which the manifest has no owner." This is the
/// only public apply entry point, and it is always the strong guarantee: the writer's append-time
/// contract and every trial/shape-check preview use it as-is.
///
/// The poisoning in-place apply strategy (E3 -- no scratch copy, `state` partially applied on throw)
/// is NOT reachable here: it is `RefTableState::applyTxnInPlace`, private, used only by `replay` (which
/// discards its local state on any throw). There is no mode argument and no way for an external caller
/// to select the poisoning path.
///
/// Enforced preconditions:
///  - `txn.txn_id` must be strictly greater than `state.greatest_applied` AND must be exactly the
///    contiguous successor `RefTableState::nextTxnId` derives (INV-1): the next sequence number within
///    the same writer epoch, or 1 under a greater one. A hole in a table's DURABLE stream is corruption,
///    not a tolerated allocation artefact.
///  - `remove_namespace`, if present, must be the transaction's FINAL operation, and every earlier
///    operation must be an exact owner-removal `owner_transition` (`old_binding` set, `new_binding`
///    empty). The codec does not check this shape; this is the one place
///    that does.
///  - `namespace_birth`: legal only while `lifecycle != Live`. Catalog admission guarantees this is
///    either a never-born runtime or a fresh physical life after predecessor deletion.
///  - `owner_transition` add (no `old_binding`, `new_binding.kind == Precommit`): namespace must be
///    `Live`; the exact `(ref_name, manifest_ref)` pair must be absent from `precommits`; AND no
///    existing committed row or precommit binding, under ANY ref_name, may already name the same
///    `manifest_ref` ("no conflicting owner may name the same manifest" -- this
///    is what lets `GC`'s `+1/-1` manifest-edge delta treat one manifest as ever having at most one
///    owner). The build-tuple-is-locally-active-build half of that same sentence is the writer's own
///    concern -- `RefTableState` has no notion of "active builds".
///  - `owner_transition` remove (an `old_binding`, no `new_binding`): the exact binding (Precommit or
///    Committed, matching `ref_name` and `manifest_ref`) must exist.
///  - `owner_transition` promote (`old_binding.kind == Precommit`, `new_binding.kind == Committed`,
///    same `ref_name` and `manifest_ref` on both sides): the exact precommit must exist, AND
///    `ref_name` must not already name a DIFFERENT committed manifest -- that stale row must be
///    evicted by its own explicit `owner_transition(old=Committed, new=None)` first (an earlier op of
///    the same transaction, so the two together read as one atomic replace, or an earlier
///    transaction). Promote never displaces an existing committed row implicitly: `GC`'s
///    manifest-edge delta is read off each transaction's explicit
///    ops, not a before/after state diff, so a silent displacement would never emit the evicted
///    manifest's "-1" edge -- it would leak as phantom-alive forever. On success the precommit is
///    replaced by a committed row whose `published_at_ms` starts UNSET (the initial stamp arrives
///    via a separate `set_published_at` op, in the same transaction or a later one).
///  - Any other `old_binding`/`new_binding` combination is not a recognized transition shape.
///  - `set_published_at`: namespace must be `Live`; the committed ref named by `ref_name` must still name
///    `expected_manifest_ref`.
///  - `remove_namespace`: namespace must be `Live` and both `committed` and `precommits` must already
///    be empty at this point in the (in-array-order) replay -- which is only true if the transaction's
///    earlier removal ops actually named every owner.
///  - Any operation other than `namespace_birth` while `lifecycle == Removed` is rejected:
///    "Any operation other than a valid later namespace_birth while state is
///    Removed is corruption."
///
/// `CORRUPTED_DATA` throughout: every rejection above uses the same "is corruption" framing,
/// extended uniformly to every precondition in this section, matching how
/// `CasRefLogCodec`/`CasRefSnapshotCodec` already use `CORRUPTED_DATA` for "this data does not
/// correspond to a valid state" one layer down (wire shape rather than transition legality). Recovery
/// and `fsck` -- the primary callers replaying persisted logs -- want exactly that fail-closed
/// framing; a writer that wants a friendlier user-facing rejection for an ordinary attempted mutation
/// (e.g. "ref already exists") checks its own business state before ever building the op.
void applyRefLogTxn(RefTableState & state, const RefLogTxn & txn);

/// The canonical snapshot of `state` under `ns`: `committed` sorted by
/// bytewise `ref_name` (guaranteed by `RefCowMap`'s sorted merge-iteration order,
/// `Pool/CasRefCowMap.h` -- the same ordering `std::map<String, ...>` gave before it, by design)
/// and `precommits` sorted by `(ref_name, manifest_ref)` (guaranteed by
/// `std::set<std::pair<String, ManifestRef>>`'s iteration order, since `ManifestRef::operator<`
/// matches the tuple order `CasRefSnapshotCodec` itself sorts by). `snapshot_id` is
/// `state.greatest_applied`. A non-`Live` state is terminal replay evidence rather than snapshot
/// state and is rejected with `CORRUPTED_DATA`. This does not otherwise enforce that the result is
/// encodable (a never-born state's `snapshot_id` is `{0, 0}`, which `encodeRefTableSnapshot`
/// already rejects) -- that check already lives in the codec and need not be duplicated here.
RefTableSnapshot snapshotOf(const RefTableState & state, const String & ns);

/// `TableState = Replay(S_X.state, tail(X))` in one call: starts from `snapshot`
/// (or the empty/never-born state when absent) and applies every transaction in `tail`, in order, via
/// `applyRefLogTxn`. A given `snapshot` is revalidated in full -- sortedness, no duplicates, canonical
/// names, nonzero ids, and `manifest_ref` field validity, i.e. everything
/// `CasRefSnapshotCodec` already enforces -- because `replay` may be handed
/// a hand-built `RefTableSnapshot` that never passed through `decodeRefTableSnapshot` (`fsck`, most
/// notably). Every entry of `tail` must also share one `ns` -- with `snapshot`'s `ns` when a snapshot
/// is given, otherwise with each other. A mismatch (of either kind) throws `CORRUPTED_DATA`: silently
/// accepting a malformed snapshot or replaying transactions from the wrong table would produce a
/// wrong-but-plausible-looking state, exactly the class of bug this equation exists to make
/// impossible.
RefTableState replay(const std::optional<RefTableSnapshot> & snapshot, std::span<const RefLogTxn> tail);

/// Everything a successful recovery of one ref table seeds. Produced by streaming replay
/// (`RefReplayBuilder::finish`) rather than assigned field-by-field into the runtime, so the whole
/// publication is one value installed atomically -- a prose field list would drift, but a struct that
/// the install copies wholesale cannot silently lose a field (Codex review round 4, spec §5).
///
/// `finish` populates the fields that are a pure function of `(base snapshot, replayed tail)`: `state`,
/// `newest_snapshot_id`, `tail_count`, `tail_bytes`, and `base_snapshot_bytes`. The
/// remaining fields are recovery-context the streaming builder cannot know -- the writer's own recovery
/// (`CasRefLedger::ensureRefTableRecovered`) fills the admission budgets,
/// `needs_stale_precommit_sweep` and `last_epoch_seal`, before installing the whole struct under
/// `state_mutex` with `recovered` set last. The read-only consumers
/// (`recoverRefTableDetailedFromAuthority` for the orphan sweep and fsck) read only
/// `state` (plus `newest_snapshot_id` for the sweep) and leave the rest at default.
struct RecoveryResult
{
    RefTableState state;
    /// Identity of the base snapshot this recovery replayed from: `nullopt` for a never-born table.
    std::optional<RefTxnId> newest_snapshot_id;
    /// Applied transactions strictly newer than `newest_snapshot_id`, and the sum of their stored
    /// (sealed) object byte sizes -- the tail-since-snapshot accounting the runtime tracks.
    uint64_t tail_count = 0;
    uint64_t tail_bytes = 0;
    /// Encoded (sealed) body size of the base snapshot; 0 for a never-born base.
    uint64_t base_snapshot_bytes = 0;

    /// Recovery-context fields (filled by `ensureRefTableRecovered`, default for other consumers):
    uint64_t snapshot_budget = 0;
    uint64_t removal_budget = 0;
    bool needs_stale_precommit_sweep = false;
    /// The `EpochSeal` that closed the last dead epoch the recovery CAS-walk crossed -- minted by it,
    /// adopted from a concurrent recoverer, or read out of the durable tail. It is the `prev_epoch_seal`
    /// this table's next sequence-1 append must carry, and `nullopt` means GENESIS exactly (see
    /// `CasRefLedger::RefTableRuntime::last_epoch_seal`). Recovery-context rather than replay-derived
    /// because only the walk knows which epochs were dead.
    std::optional<RefTxnId> last_epoch_seal;
};

/// The streaming generalisation of `replay` (spec §5): owns a PRIVATE candidate `RefTableState` and
/// applies decoded transactions into it ONE AT A TIME, in place, discarding the candidate on any throw.
/// It is the memory fix for a long post-snapshot tail: `replay` takes the whole `tail` materialised in a
/// vector (every decoded transaction resident at once, each up to the 20 MiB normal-class cap), whereas
/// a caller that GETs+decodes+`applyOne`+discards one object at a time holds at most a single decoded
/// transaction. `applyOne` reaches `RefTableState::applyTxnInPlace` directly -- the same private
/// poisoning path `replay` uses -- NOT the public scratch-copying `applyRefLogTxn`, which would deep-copy
/// the growing candidate once per transaction and reintroduce the O(K*N) cost `replay` was written to
/// avoid. The candidate never touches any live runtime state; a throw destroys it during unwinding, so no
/// consumer ever observes a poisoned candidate. All three full-tail materialisers stream through this:
/// the writer's recovery, `recoverRefTableDetailedFromAuthority` (orphan sweep and fsck).
class RefReplayBuilder
{
public:
    /// Seeds the candidate from `base` (or the empty/never-born state when absent), revalidating the
    /// snapshot in full exactly as `replay` does (`stateFromSnapshot`). `base_encoded_bytes` is the
    /// stored (sealed) size of that snapshot object, carried through to `RecoveryResult::base_snapshot_bytes`
    /// (0 when the caller does not track it -- the read-only consumers do not).
    explicit RefReplayBuilder(std::optional<RefTableSnapshot> base, uint64_t base_encoded_bytes = 0);

    /// Applies one decoded transaction to the candidate in place. `encoded_bytes` is the stored (sealed)
    /// object size of `txn`, accumulated into the tail-byte total. A decode/apply corruption throws
    /// `CORRUPTED_DATA` (the non-transient class recovery fails fast on), discarding the candidate.
    void applyOne(RefLogTxn && txn, uint64_t encoded_bytes);

    /// The candidate's lifecycle AS OF the transactions applied so far. Recovery's CAS-walk needs it at
    /// each epoch boundary it reaches, and it must be the LIVE reading rather than one taken from the
    /// base snapshot: a removal transaction in the replayed tail is exactly the case where the two
    /// differ, and it is the case that decides whether the epoch below gets a seal.
    RefLifecycle lifecycle() const { return candidate.getLifecycle(); }

    /// Materialises nothing extra (matches `replay`: the writer's recovery folds the COW overlays via
    /// `materializeCommitted` on the result; the read-only consumers do not) and returns the replay-derived
    /// `RecoveryResult` fields, moving the candidate out. The builder must not be used afterwards.
    RecoveryResult finish() &&;

private:
    RefTableState candidate;
    std::optional<String> expected_ns;
    RecoveryResult result;
};

/// Resident footprint, in bytes, of a decoded ref-log transaction: the heap it keeps alive while it is
/// held in memory -- its op vector's element storage plus every owned string (the transaction `ns` and
/// each op's ref-name strings). A deterministic function of the decoded CONTENT (unlike the compressed
/// stored size, which understates a highly-compressible transaction), so a memory-bound test built on it
/// is stable under ASan. This is what a whole-tail materialiser accumulates N-fold, while the streaming
/// recovery loops hold exactly one decoded transaction resident at a time.
uint64_t decodedRefLogTxnFootprint(const RefLogTxn & txn);

/// Report a decoded-transaction memory delta to the installed streaming-recovery memory probe, if any
/// (a no-op in production -- no probe is installed). Each recovery loop calls `+footprint` when a
/// decoded transaction becomes resident and `-footprint` when it is discarded, so the probe observes the
/// loop's real resident set. Exposed (rather than confined to one translation unit) because the three
/// recovery loops live in three files and a memory-bound test's materialising control drives the
/// identical seam.
void reportReplayMemoryDelta(int64_t delta_footprint_bytes);

/// Test-only observability for the streaming-recovery memory invariant (spec §5): while a probe is
/// installed, each recovery loop reports the resident footprint of every decoded transaction it holds,
/// for exactly the span it holds it (`reportReplayMemoryDelta` + `decodedRefLogTxnFootprint`). A
/// memory-bound test tracks the peak of the summed reported footprint and asserts it stays within a
/// single transaction, where the retired whole-tail materialiser -- and the test-local materialising
/// control that stands in for it -- held the entire tail resident at once. Because the report spans the
/// decoded transaction's whole GET->decode->apply->discard lifetime at the LOOP, not one apply in
/// isolation, a regression that materialises the whole tail before applying it is caught. No probe
/// installed => no accounting. Guarded by an internal mutex; install before driving recovery and clear
/// afterwards.
void setRecoveryReplayMemoryProbeForTest(std::function<void(int64_t delta_footprint_bytes)> probe);

/// The exact encoded size of `state`'s canonical snapshot (`encodeRefTableSnapshot(snapshotOf(state,
/// "")).size()`), computed in O(1) from the running body counter plus O(1) framing instead of a full
/// re-encode. Used by `admits` and directly property-tested against the real encoder.
uint64_t encodedSnapshotBudgetSize(const RefTableState & state);

/// The exact encoded size of `state`'s hypothetical whole-namespace removal transaction, computed in
/// O(1) from the running body counter plus O(1) framing. Used by `admits`.
uint64_t encodedRemovalBudgetSize(const RefTableState & state);

/// Admission budget: true iff applying `op` to a COPY of `state` (via the same
/// per-operation validator `applyRefLogTxn` uses -- an `op` that is not itself a legal transition
/// throws exactly as `applyRefLogTxn` would, since `admits` answers "would this legal op still fit
/// the budget", not "is this op legal") keeps BOTH the resulting table snapshot and the resulting
/// hypothetical complete-removal transaction within their respective byte budgets.
///
/// `RefTableState` carries no `ns` (it is per-table but not one of this class's fields), so both
/// hypothetical encodings are measured with an empty `ns`. `ns` is constant for one table for its
/// entire lifetime, so a caller computes its own table's `ns.size()` overhead once (the wire layout's
/// `u32` length prefix itself is present in BOTH the empty-`ns` measurement here and the real encoding,
/// so it cancels -- only the `ns` bytes themselves are the delta; repeated exactly once in a snapshot
/// body and once in a removal-transaction body, see `CasRefSnapshotCodec` / `CasRefLogCodec`'s wire
/// layout) and pre-subtracts it, together with its own safety margin, from the raw
/// `ref_snapshot_max_bytes` / `ref_removal_max_bytes` hard limits before calling `admits`.
///
/// Implementation: sizes are computed incrementally. `RefTableState` carries running body-byte totals
/// (`snapshot_body_bytes` / `removal_body_bytes`) maintained O(1) per applied op by `RefTableState::applyOp`;
/// `admits` applies `op` to a scratch copy and reads `framing + total` via `encodedSnapshotBudgetSize`
/// / `encodedRemovalBudgetSize`, making the whole check O(touched rows) instead of O(table size). This
/// is byte-exact rather than a drift-prone estimate: both budget encodings are pure per-row sums, the
/// per-row contributions come from the same codec primitives the full encoders use, and a
/// debug/sanitizer-only recompute-and-compare `chassert` (`RefTableState::debugAssertBodyCounters`)
/// reasserts equality on every applied transaction and every `admits` preview.
bool admits(const RefTableState & state, const RefOp & op, uint64_t snapshot_budget, uint64_t removal_budget);

/// Pure ref-log intake primitives for a GC round. None of these read a
/// manifest body, a snapshot body, or `gc/state`: they turn a global `LIST cas/ns/stream/` result and the
/// decoded bodies of new transactions into (a) the per-table log/snapshot/marker listing, (b) the
/// deterministic manifest-edge delta, and (c) the exact ref-object cleanup plan. The GC round
/// (`CasGc.cpp`) drives the manifest-body reads (`foldManifestEdges`), the fold barrier, the durable
/// cursor, and the batch deletions around these functions. Keeping the delta and cleanup logic pure
/// makes it directly unit-testable (`gtest_cas_ref_intake.cpp`) without a full round.

/// One `+1`/`-1` manifest edge emitted by one ref-log operation.
/// `manifest_id` is namespace-qualified (equal `ManifestRef` tuples under two tables stay distinct).
/// The ordinals locate the exact operation and edge inside the transaction, giving the spec's
/// `event_id = {namespace, RefTxnId, operation_ordinal, edge_ordinal}` its determinism: replaying the
/// same logs yields byte-identical edges, so retry and competing GC attempts produce the same delta.
struct RefManifestEdge
{
    ManifestId manifest_id;
    int change = 0;              /// +1 activation | -1 removal
    RefOwnerKind owner_kind = RefOwnerKind::Committed;   /// kind of the binding that produced this edge:
                                 /// the `new_binding` kind for a `+1`, the `old_binding` kind for a `-1`.
                                 /// The GC fold needs it to classify a missing manifest body -- a removed
                                 /// precommit that never activated is skipped, every other missing body clamps.
    uint32_t op_ordinal = 0;     /// index of the op within its transaction
    uint32_t edge_ordinal = 0;   /// 0 = the removal edge, 1 = the activation edge of one op

    bool operator==(const RefManifestEdge &) const = default;
};

/// The manifest edges of ONE decoded transaction, in operation order, reading NO manifest body.
/// `owner_transition` recognizes EXACTLY the four shapes `classifyOwnerTransitionShape`
/// (Pool/CasRefProtocol.cpp) also uses to drive `RefTableState::applyOwnerTransition` -- the SAME
/// classification, not a second copy of the shape knowledge:
///   - add precommit (no old_binding, new_binding.kind == Precommit)     => `+1` for `new.manifest_ref`
///   - remove precommit / remove committed (old_binding set, no new_binding)
///                                                                       => `-1` for `old.manifest_ref`
///   - promote (old_binding.kind == Precommit, new_binding.kind == Committed, SAME ref_name and
///     manifest_ref)                                                    => no edge (net zero: the
///     manifest keeps an owner the whole time)
///   - `namespace_birth` / `set_published_at` / `remove_namespace`      => no edge
/// Any other `owner_transition` shape -- neither binding, old+new naming DIFFERENT manifests, a
/// promote whose old/new ref_name disagree, or any other kind combination -- throws `CORRUPTED_DATA`.
/// These are exactly the shapes `applyRefLogTxn`/`replay` already reject at the state-machine layer, so
/// a hand-corrupted or adversarial log body is the only way this branch is reached; a legitimately
/// written log never produces one. The GC fold (`CasGc.cpp`) extracts edges inside the same try-block
/// as `decodeRefLogTxn`, so the throw gets the identical "ref log body invalid: ref folding aborted
/// this round" treatment as an undecodable body -- no cursor advance, no deletions, an anomaly
/// recorded. The orphan sweep (`CasOrphanManifestSweep.cpp`) catches it around the whole
/// `activeManifestKeys` construction and skips (or marks errored) that namespace's deletions rather
/// than trusting an incomplete protection view.
/// The `remove_namespace` operation changes lifecycle only; the exact owner removals that must precede
/// it in the same transaction already emit their own `-1` edges.
std::vector<RefManifestEdge> manifestEdgesOfTxn(const RefLogTxn & txn);

/// The GC fold consumes a table's new transactions ONE log at a time, in ascending id order, emitting
/// each log's `manifestEdgesOfTxn` into `foldManifestEdges` and advancing the durable cursor per fully
/// folded log (mirroring the legacy per-event journal fold, including its clamp-on-missing-body barrier).
/// In-batch add+remove cancellation is therefore NOT done as a pre-fold net pass: the idempotent
/// `(blob, source_id)` in-degree set-merge already cancels a `+edge` and matching `-edge` folded into one
/// generation, and a pre-fold net pass would be unsafe -- a mid-batch clamp could split a cancelled pair
/// across the advanced cursor, folding a spurious `-1` in a later round. So there is deliberately no
/// `netManifestDelta` here.

/// The `remove_txn_id` of a transaction that ends its namespace's life (contains a `remove_namespace`
/// operation), or `nullopt`. The value equals `txn.txn_id`. The round routes it into the durable
/// cleanup evidence of that life's fold-state row.
std::optional<RefTxnId> removalTxnId(const RefLogTxn & txn);

/// One table's surviving ref-object keys from this round's global `LIST`, split by kind and sorted
/// ascending.
struct RefTableListing
{
    std::vector<RefTxnId> logs;
    std::vector<RefTxnId> snapshots;
    bool operator==(const RefTableListing &) const = default;
};

/// Parse and group a global `LIST` of keys under `layout.casRefsPrefix` by physical life id. Every
/// key is expected to be one of the three immutable stream-object kinds; checkpoints and namespace
/// files live in the separate state tree and are never offered by this hot enumeration. An
/// unrecognized stream key throws `CORRUPTED_DATA`, so the round cannot derive a partial delta or
/// authorize destructive work from an incomplete classification.
/// A key outside `casRefsPrefix` is ignored: the caller lists only the stream prefix, and a foreign key
/// is not this format's concern.
std::map<NamespaceLifePhysicalId, RefTableListing> groupRefKeys(
    const Layout & layout, const std::vector<String> & listed_keys);

/// The exact ref objects one round may delete for one namespace life.
/// Pure; acts only on keys THIS round's scan returned, but the scan is never cleanup authority. The
/// caller may supply `checkpoint` only after exact validation of the `_ckpt`-named recovery triple:
/// `_ckpt` plus its same-id non-seal `_log` and `_snap`. A later-epoch base also returns the exact
/// predecessor seal that proved its transition; `retained_log_proof` keeps that log outside the delete
/// plan while the checkpoint remains authoritative. Without a validated base the plan is empty.
/// With it, a log `L` is deletable only when `L < checkpoint` and `L <= durable_cursor`; a listed
/// snapshot is deletable only when its id is `< checkpoint`. The base's same-id `_log` and `_snap`,
/// and every newer stream object, are retained.
struct RefCleanupPlan
{
    std::vector<RefTxnId> deletable_logs;
    std::vector<RefTxnId> deletable_snapshots;

    bool operator==(const RefCleanupPlan &) const = default;
};
RefCleanupPlan planRefCleanup(const RefTableListing & listing, const RefTxnId & durable_cursor,
                              std::optional<RefTxnId> checkpoint = std::nullopt,
                              std::optional<RefTxnId> retained_log_proof = std::nullopt);

/// Why an epoch crossing failed, or that it was PROVED. See `crossEpochFromSeal`.
enum class EpochCrossOutcome : uint8_t
{
    Proved,             /// `start` is sequence 1 of the epoch that chains from `from_seal`
    NothingConsumed,    /// `from_seal` is `{0, 0}`: nothing has been consumed, so there is no seal to cross from
    NotASeal,           /// the record at `from_seal` is KNOWN not to be an `EpochSeal`
    StartAbsent,        /// an epoch-start record the back-chain needs is not there
    StartInvalid,       /// an epoch-start record is undecodable (`detail` carries the codec's message)
    ChainDoesNotReach,  /// the chain names no seal at `from_seal` -- a genesis record, or one that skips it
};

/// One epoch crossing's outcome plus what it cost, so a caller can attribute the reads it performed.
struct EpochCrossResult
{
    EpochCrossOutcome outcome = EpochCrossOutcome::ChainDoesNotReach;
    RefTxnId start{};            /// meaningful only on `Proved`
    RefTxnId probed{};           /// the epoch-start id the walk last read -- names the object in a diagnostic
    String detail;               /// the decode error, on `StartInvalid`
    uint64_t body_gets = 0;      /// epoch-start bodies actually fetched
    uint64_t absent_probes = 0;  /// epoch-start reads that came back absent

    bool proved() const { return outcome == EpochCrossOutcome::Proved; }
};

/// Cross into the epoch that follows the one `from_seal` closed, and PROVE it rather than guess it.
///
/// A listing only NOMINATES a candidate epoch (`witness`). The proof is the back-chain: the target
/// epoch's sequence-1 record names the seal that must have been consumed before it (INV-2). If it names
/// a seal ABOVE `from_seal`, an epoch sits in between that the nomination omitted -- the chain is
/// followed back one epoch and retried, which is what makes a crossing independent of any enumeration.
/// Anything else -- no sequence 1, an undecodable one, a genesis record (no `prev_epoch_seal`) above a
/// consumed seal, or a chain that skips the position -- is an unproven crossing and is reported as such.
///
/// WHAT THE CHAIN PROVES, EXACTLY: the IDENTITY of the position the next epoch chains from, not its
/// KIND. Those come apart when a writer names an ordinary record as `prev_epoch_seal`, and the damage is
/// the one the seal exists to prevent -- epoch `E` declared closed while its writer may still append, so
/// a later `{E, k}` lands permanently below the cursor. So the kind is checked wherever it is knowable:
/// `seal_proven` carries `refLogTxnIsEpochSeal` of the record the CALLER applied at `from_seal` (free --
/// it decoded the body to apply it). Pass `nullopt` only when the caller applied nothing and `from_seal`
/// is an inherited cursor whose record may since have been cleaned, so its kind is unknowable by any
/// amount of reading here and the crossing rests on chain-trust.
///
/// Terminates: `validateEpochSealGrammarStructural` guarantees `prev_epoch_seal->writer_epoch + 1 ==
/// txn_id.writer_epoch`, so the target epoch decreases one numeric epoch per link and is bounded below
/// by `from_seal`'s own epoch. The record it proves is read once more by the caller's own walk: one
/// redundant `GET` per epoch crossed, and crossings happen once per writer-epoch change.
///
/// READ-ONLY, and shared deliberately: the GC fold's intake and fsck's audit must not be able to
/// disagree about when an epoch boundary has been proved -- a rule that says which records a cut
/// contains cannot have two implementations.
///
/// `life`: the namespace's life, REQUIRED (review NEW-3 -- a `nullopt`-resolves-internally default was
/// tried once and reintroduced the exact divergence review C3 removed from `Gc::fold`, just relocated
/// into `CasFsck.cpp`'s independent walk, which had its OWN already-resolved `life` in scope one call
/// site above and simply did not pass it). Every caller must resolve `life` itself, ONCE, and pass the
/// SAME value here that it uses for every other read in its own walk -- this function no longer
/// resolves anything on its own, so there is no second resolution left to disagree with the first.
EpochCrossResult crossEpochFromSeal(Backend & backend, const Layout & layout, const RootNamespace & ns,
                                    const RefTxnId & from_seal, std::optional<bool> seal_proven,
                                    const RefTxnId & witness, const NamespaceLifeId & life);

/// Return the one exact successor an immutable `_ckpt.committed_through` range permits after the
/// decoded record at `current`, or `nullopt` when `current` is the inclusive frontier itself. An
/// `EpochSeal` ends its numeric epoch, so a strictly-later frontier in that SAME epoch is corrupt:
/// advancing to `{E+1,1}` and letting ordinary ordering terminate would silently discard the invalid
/// part of the claimed range. Shared by every checkpoint-bounded reader so recovery, fsck, and the
/// orphan protection walk cannot disagree about that malformed authority.
std::optional<RefTxnId> nextRefLogIdWithinCommittedFrontier(
    const RefTxnId & current, bool is_epoch_seal, const RefTxnId & committed_through);

/// The result of `recoverRefTableDetailedFromAuthority`: the replayed table state plus the identity of
/// the snapshot recovery actually selected as its base (`nullopt` when it found no snapshot at all).
struct RecoveredRefTable
{
    RefTableState state;
    std::optional<RefTxnId> newest_snapshot_id;
    /// The exact lifecycle authority's last sealed epoch. The authoritative read-only entry point
    /// copies this from its immutable `_ckpt` input.
    std::optional<RefTxnId> last_epoch_seal;
};

/// Exact-read and decode the checkpoint-named recovery base. The anchor is the bounded triple
/// `_ckpt` + same-id non-seal `_log` + same-id `_snap`: read and decode the log first, reject an
/// `EpochSeal`, validate its contextual epoch backlink against `_ckpt.life_epoch` and the exact
/// `_ckpt.last_epoch_seal` when that field describes the base's preceding epoch, exact-read and require
/// the named predecessor to be an `EpochSeal`, then read the snapshot. This order prevents a forged
/// snapshot at any historical seal or contextually invalid epoch start from becoming state. Cleanup
/// retains both the matching log and returned predecessor proof while the checkpoint names this base.
struct CheckpointSnapshotBase
{
    RefTableSnapshot snapshot;
    uint64_t bytes = 0;
    /// The exact `EpochSeal` named by a non-genesis sequence-1 base. Recovery needs that object as
    /// durable transition proof, so cleanup must retain it together with the checkpoint base.
    std::optional<RefTxnId> predecessor_seal_id;
};

CheckpointSnapshotBase readCheckpointSnapshotBase(
    Backend & backend, const Layout & layout, const NamespaceLifeId & life, const RefCkpt & checkpoint);

/// Recover a ref table from ONE immutable lifecycle authority cut supplied by the caller. `catalog_entry`
/// is either the exact row from that caller's frozen catalog cut or absence from that same cut; `ckpt` is
/// the exact decoded `_ckpt` that caller read for the row's `NamespaceLifeId`. This function does NOT
/// GET the catalog or `_ckpt` itself: accepting a later, competing cut would make its result disagree
/// with the caller's other decisions. `chooseRecoveryGrounding` makes absent/Creating names non-recoverable
/// and requires a readable `_ckpt` with `life_epoch` for Live/Removing.
///
/// Recovery performs no stream `LIST`: its replay is exact point GETs from the checkpoint-named base
/// through inclusive `committed_through`; a missing checkpoint-named base or committed log is corruption
/// under this immutable authority. In particular, this read-only API never probes or adopts `F+1`. There
/// is deliberately no self-resolving compatibility overload: every consumer must pass the row from its
/// frozen catalog cut explicitly.
RecoveredRefTable recoverRefTableDetailedFromAuthority(
    Backend & backend, const Layout & layout, const std::optional<CatalogEntry> & catalog_entry,
    const std::optional<RefCkpt> & ckpt);

}
