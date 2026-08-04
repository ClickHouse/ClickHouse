#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>
#include <algorithm>
#include <limits>
#include <mutex>
#include <type_traits>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

CatalogLifeIndex::CatalogLifeIndex(const RefCatalog & catalog)
{
    for (const CatalogEntry & entry : catalog.entries)
    {
        const NamespaceLifePhysicalId life_id = entry.incarnation;
        if (auto ambiguous_it = ambiguous_names.find(life_id); ambiguous_it != ambiguous_names.end())
        {
            ambiguous_it->second.push_back(entry.ns.string());
            continue;
        }

        const auto [it, inserted] = unique_lives.emplace(
            life_id, NamespaceLifeId::fromCatalogEntry(entry.ns, life_id));
        if (inserted)
            continue;

        std::vector<String> names;
        names.push_back(it->second.ns.string());
        names.push_back(entry.ns.string());
        unique_lives.erase(it);
        ambiguous_names.emplace(life_id, std::move(names));
    }
}

bool CatalogLifeIndex::isAmbiguous(NamespaceLifePhysicalId life_id) const
{
    return ambiguous_names.contains(life_id);
}

std::optional<NamespaceLifeId> CatalogLifeIndex::resolve(NamespaceLifePhysicalId life_id) const
{
    if (const auto ambiguous_it = ambiguous_names.find(life_id); ambiguous_it != ambiguous_names.end())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS ref catalog: life_id {} is shared by current namespaces '{}' and '{}' -- both rows are unresolvable",
            renderIncarnation(life_id), ambiguous_it->second[0], ambiguous_it->second[1]);
    if (const auto it = unique_lives.find(life_id); it != unique_lives.end())
        return it->second;
    return std::nullopt;
}

void CatalogLifeIndex::throwIfAmbiguous(std::string_view consumer) const
{
    if (ambiguous_names.empty())
        return;
    const auto & [life_id, names] = *ambiguous_names.begin();
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "{}: catalog life_id {} is shared by current namespaces '{}' and '{}' -- refusing a decision from an ambiguous cut",
        consumer, renderIncarnation(life_id), names[0], names[1]);
}

namespace
{

/// Txn-wide structural check, NOT a per-op precondition: if `ops` contains a
/// `RemoveNamespace`, it must be the last element, and every earlier op must be an exact
/// owner-removal `owner_transition` (`old_binding` set, `new_binding` empty). The sole other legal
/// form is `[NamespaceBirth, RemoveNamespace]`: a cataloged life may own `_ckpt`/`_files` without ever
/// having emitted a ref transaction, and its empty birth+terminal must be one durable removal record.
/// `CasRefLogCodec` deliberately does not check this shape -- this is the one place that does.
void checkRemoveNamespaceOrdering(const std::vector<RefOp> & ops)
{
    const bool has_remove = std::any_of(ops.begin(), ops.end(),
        [](const RefOp & op) { return op.kind == RefOpKind::RemoveNamespace; });
    if (!has_remove)
        return;

    if (ops.empty() || ops.back().kind != RefOpKind::RemoveNamespace)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableState: remove_namespace must be the final operation of its transaction");

    if (ops.size() == 2 && ops.front().kind == RefOpKind::NamespaceBirth)
        return;

    for (size_t i = 0; i + 1 < ops.size(); ++i)
    {
        const RefOp & op = ops[i];
        const bool pure_removal = op.kind == RefOpKind::OwnerTransition
            && op.old_binding.has_value() && !op.new_binding.has_value();
        if (!pure_removal)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefTableState: every operation before remove_namespace must be an exact owner removal");
    }
}

/// Installed test probe for the streaming-recovery memory invariant (see the header). Guarded by its
/// own mutex so an install/clear from a test thread cannot tear against a concurrent recovery.
std::mutex g_recovery_replay_memory_probe_mutex;
std::function<void(int64_t)> g_recovery_replay_memory_probe;

/// The four legal `owner_transition` shapes, decided purely from the (old_binding, new_binding)
/// optionals and their `RefOwnerKind`s -- no state read. `RefTableState::applyOwnerTransition` (the
/// writer/replay state machine) and `manifestEdgesOfTxn` (the GC fold's edge extractor) both switch
/// over this single classification instead of each carrying their own shape predicates, so a shape
/// neither consumer recognizes cannot silently acquire divergent meaning in one of them.
enum class OwnerTransitionShape : uint8_t
{
    AddPrecommit,      /// no old_binding, new_binding.kind == Precommit
    RemovePrecommit,   /// old_binding.kind == Precommit, no new_binding
    RemoveCommitted,   /// old_binding.kind == Committed, no new_binding
    Promote,           /// old_binding.kind == Precommit, new_binding.kind == Committed, SAME ref_name
                       /// and manifest_ref
};

/// Classify `op`'s (old_binding, new_binding) shape into one of the four legal transitions. Anything
/// else -- neither binding, old+new naming DIFFERENT manifests, a promote whose old/new ref_name
/// disagree, or any other kind combination -- throws `CORRUPTED_DATA` naming the offending combination
/// instead of falling through to a caller that would otherwise assign it accidental meaning.
[[nodiscard]] OwnerTransitionShape classifyOwnerTransitionShape(const RefOp & op)
{
    const bool has_old = op.old_binding.has_value();
    const bool has_new = op.new_binding.has_value();

    if (!has_old && has_new && op.new_binding->kind == RefOwnerKind::Precommit)
        return OwnerTransitionShape::AddPrecommit;

    if (has_old && !has_new && op.old_binding->kind == RefOwnerKind::Precommit)
        return OwnerTransitionShape::RemovePrecommit;

    if (has_old && !has_new && op.old_binding->kind == RefOwnerKind::Committed)
        return OwnerTransitionShape::RemoveCommitted;

    if (has_old && has_new && op.old_binding->kind == RefOwnerKind::Precommit
        && op.new_binding->kind == RefOwnerKind::Committed
        && op.old_binding->ref_name == op.new_binding->ref_name
        && op.old_binding->manifest_ref == op.new_binding->manifest_ref)
        return OwnerTransitionShape::Promote;

    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "owner_transition does not match any legal transition shape (has_old={}, old_kind={}, "
        "has_new={}, new_kind={})",
        has_old, has_old ? std::to_string(static_cast<uint8_t>(op.old_binding->kind)) : "n/a",
        has_new, has_new ? std::to_string(static_cast<uint8_t>(op.new_binding->kind)) : "n/a");
}

}

void setRecoveryReplayMemoryProbeForTest(std::function<void(int64_t delta_footprint_bytes)> probe)
{
    std::lock_guard lock(g_recovery_replay_memory_probe_mutex);
    g_recovery_replay_memory_probe = std::move(probe);
}

void reportReplayMemoryDelta(int64_t delta_footprint_bytes)
{
    /// Reads the installed probe under the mutex and calls it OUTSIDE the lock so the probe body may
    /// itself do arbitrary work. A no-op in production (no probe installed).
    std::function<void(int64_t)> probe;
    {
        std::lock_guard lock(g_recovery_replay_memory_probe_mutex);
        probe = g_recovery_replay_memory_probe;
    }
    if (probe)
        probe(delta_footprint_bytes);
}

uint64_t decodedRefLogTxnFootprint(const RefLogTxn & txn)
{
    /// A deterministic proxy for the heap a decoded transaction keeps alive: the ns string, the op
    /// vector's element storage (ops count x per-op record size), and every owned ref-name string. Uses
    /// `size()` (not `capacity()`) so the value depends only on the decoded content, hence is identical
    /// across a streaming decode and a materialising control's decode of the same object.
    uint64_t bytes = txn.ns.size() + txn.ops.size() * sizeof(RefOp);
    for (const RefOp & op : txn.ops)
    {
        bytes += op.ref_name.size();
        if (op.old_binding)
            bytes += op.old_binding->ref_name.size();
        if (op.new_binding)
            bytes += op.new_binding->ref_name.size();
    }
    return bytes;
}

/// Member-wise swap; see the header for the install-region contract it exists for. Every member is
/// enumerated here by hand rather than swapped through a generated move, so a member added to the
/// class without a line here is a silent state-corruption bug -- the `static_assert`s below are the
/// type-level half of the guarantee (the macro at the call site proves the code path, these prove the
/// contract of the types), and `debugAssertBodyCounters` cross-checks the counters this swap carries.
void RefTableState::swap(RefTableState & other) noexcept
{
    static_assert(std::is_nothrow_swappable_v<RefLifecycle>);
    static_assert(std::is_nothrow_swappable_v<std::optional<RefTxnId>>);
    static_assert(std::is_nothrow_swappable_v<RefTxnId>);
    static_assert(std::is_nothrow_swappable_v<std::set<std::pair<String, ManifestRef>>>);
    static_assert(std::is_nothrow_swappable_v<uint64_t>);
    static_assert(noexcept(std::declval<RefCowMap &>().swap(std::declval<RefCowMap &>())));
    static_assert(noexcept(std::declval<RefCowManifestSet &>().swap(std::declval<RefCowManifestSet &>())));

    using std::swap;
    swap(lifecycle, other.lifecycle);
    swap(remove_txn_id, other.remove_txn_id);
    swap(greatest_applied, other.greatest_applied);
    committed.swap(other.committed);
    precommits.swap(other.precommits);
    owned_manifests.swap(other.owned_manifests);
    swap(snapshot_body_bytes, other.snapshot_body_bytes);
    swap(removal_body_bytes, other.removal_body_bytes);
}

/// True iff `manifest_ref` already names an existing committed row or precommit binding under ANY
/// ref_name (the add-precommit rule: "no conflicting owner may name the same manifest"). O(1) via
/// `owned_manifests`, a COW membership index (Pool/CasRefCowManifestSet.h) that every ownership-
/// changing arm below (and `stateFromSnapshot`) maintains in lock-step with `committed` and
/// `precommits`. The old linear scan lives on, in debug/sanitizer builds only, as
/// `debugAssertBodyCounters`'s cross-check that the index has not drifted from those two containers.
bool RefTableState::manifestAlreadyOwned(const ManifestRef & manifest_ref) const
{
    return owned_manifests.contains(manifest_ref);
}

/// The `owner_transition` op kind: dispatches on the `(old_binding,
/// new_binding)` shape to one of the four legal transitions (add precommit / remove precommit /
/// remove committed / promote). Any other shape is not a recognized transition.
void RefTableState::applyOwnerTransition(const RefOp & op)
{
    if (lifecycle != RefLifecycle::Live)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: owner_transition while namespace is not Live");

    /// Shape legality is decided once, by the shared classifier; everything below is the per-shape
    /// PRECONDITION check and effect, unchanged.
    switch (classifyOwnerTransitionShape(op))
    {
        /// Add precommit: no old_binding, a fresh Precommit new_binding.
        case OwnerTransitionShape::AddPrecommit:
        {
            const RefOwnerBinding & b = *op.new_binding;
            if (precommits.contains({b.ref_name, b.manifest_ref}))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: add precommit '{}' already exists for this exact manifest", b.ref_name);
            /// Cross-owner uniqueness runs UNCONDITIONALLY, in every apply strategy (writer append AND
            /// trusted replay). Since E2 this is an O(1) `owned_manifests` lookup, so the E1-era elision of
            /// it under trusted replay (which downgraded it to a debug-only `chassert`) bought nothing
            /// measurable while making a corrupted log/snapshot FAIL OPEN -- a double-owner input would drift
            /// the index and let ordinary later writes append invariant-violating durable history. Keeping it
            /// here is what makes replay fail CLOSED on a corrupted `manifest_ref` collision.
            if (manifestAlreadyOwned(b.manifest_ref))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: manifest already has a conflicting owner under another ref_name");
            precommits.emplace(b.ref_name, b.manifest_ref);
            snapshot_body_bytes += precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, b.ref_name, b.manifest_ref});
            removal_body_bytes  += removalOpEncodedSize(RefOwnerKind::Precommit, b.ref_name, b.manifest_ref);
            owned_manifests.insert(b.manifest_ref);
            return;
        }

        /// Remove precommit: an exact Precommit old_binding, no new_binding.
        case OwnerTransitionShape::RemovePrecommit:
        {
            const RefOwnerBinding & b = *op.old_binding;
            if (precommits.erase({b.ref_name, b.manifest_ref}) == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: exact precommit binding '{}' to remove is absent", b.ref_name);
            snapshot_body_bytes -= precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, b.ref_name, b.manifest_ref});
            removal_body_bytes  -= removalOpEncodedSize(RefOwnerKind::Precommit, b.ref_name, b.manifest_ref);
            owned_manifests.erase(b.manifest_ref);
            return;
        }

        /// Remove committed ref: an exact Committed old_binding, no new_binding.
        case OwnerTransitionShape::RemoveCommitted:
        {
            const RefOwnerBinding & b = *op.old_binding;
            const auto it = committed.find(b.ref_name);
            if (it == committed.end() || !(it->second.manifest_ref == b.manifest_ref))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: exact committed binding '{}' to remove is absent", b.ref_name);
            const RefCommittedRow removed = it->second;
            committed.erase(it);
            snapshot_body_bytes -= committedRowEncodedSize(removed);
            removal_body_bytes  -= removalOpEncodedSize(RefOwnerKind::Committed, removed.ref_name, removed.manifest_ref);
            owned_manifests.erase(removed.manifest_ref);
            return;
        }

        /// Promote: the SAME ref_name and manifest_ref move from Precommit to Committed
        /// in one atomic step; the resulting row's `published_at_ms` starts unset (installed by the
        /// companion set_published_at op in the same transaction, or a later one).
        case OwnerTransitionShape::Promote:
        {
            const RefOwnerBinding & b = *op.old_binding;
            if (precommits.erase({b.ref_name, b.manifest_ref}) == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: exact precommit binding '{}' to promote is absent", b.ref_name);
            snapshot_body_bytes -= precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, b.ref_name, b.manifest_ref});
            removal_body_bytes  -= removalOpEncodedSize(RefOwnerKind::Precommit, b.ref_name, b.manifest_ref);
            /// `owned_manifests` is deliberately left untouched by a promote: `b.manifest_ref` moves from
            /// precommit ownership to committed ownership without ever giving it up in between -- the
            /// same "there is no moment at which the manifest has no owner" invariant this function's
            /// header doc states for promote generally. The index tracks "does ANY owner currently name
            /// this manifest", not which kind, so an erase-then-insert pair here would be pure overhead.
            /// A DIFFERENT manifest already committed under this exact ref_name must be evicted by its
            /// own explicit owner_transition(old=Committed, new=None) first (an earlier op of this same
            /// transaction, or an earlier transaction) -- never silently here. `GC`'s manifest-edge delta
            /// is read off the transaction's explicit ops, not a
            /// before/after state diff; a promote that silently evicted a stale committed row would never
            /// emit that manifest's "-1" edge, leaking it as phantom-alive forever.
            if (committed.contains(b.ref_name))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: promote '{}' would silently displace a different already-committed "
                    "manifest -- remove it with an explicit owner_transition first", b.ref_name);
            RefCommittedRow row;
            row.ref_name = b.ref_name;
            row.manifest_ref = b.manifest_ref;
            snapshot_body_bytes += committedRowEncodedSize(row);
            removal_body_bytes  += removalOpEncodedSize(RefOwnerKind::Committed, row.ref_name, row.manifest_ref);
            committed.emplace(b.ref_name, std::move(row));
            return;
        }
    }
    /// Reachable only if a future `OwnerTransitionShape` enumerator is added without a matching `case`
    /// (mirrors `applyOp`'s exhaustive-switch-then-throw shape below) -- `-Wswitch`/`-Werror` catches that
    /// at compile time; this throw is the runtime backstop for builds without it.
    throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: unhandled owner_transition shape");
}

/// The `set_published_at` op kind: the committed ref must still name `expected_manifest_ref`; replaces
/// `published_at_ms` without touching the manifest edge.
void RefTableState::applySetPublishedAt(const RefOp & op)
{
    if (lifecycle != RefLifecycle::Live)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: set_published_at while namespace is not Live");

    const auto it = committed.find(op.ref_name);
    if (it == committed.end() || !(it->second.manifest_ref == op.expected_manifest_ref))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableState: set_published_at '{}' no longer names its expected_manifest_ref", op.ref_name);

    /// `RefCowMap`'s iterator is read-only (Pool/CasRefCowMap.h): a write always goes through
    /// `insert_or_assign`, never through the found iterator in place. Copy the row, apply the same
    /// field mutation the old in-place code did, and write the whole row back -- this IS the COW
    /// map's single-row copy-out, not a whole-table one.
    RefCommittedRow updated = it->second;
    const uint64_t old_row_bytes = committedRowEncodedSize(it->second);
    updated.published_at_ms = op.published_at_ms;
    snapshot_body_bytes -= old_row_bytes;
    snapshot_body_bytes += committedRowEncodedSize(updated);
    /// removal_body_bytes unchanged: set_published_at touches neither ref_name nor manifest_ref.
    committed.insert_or_assign(op.ref_name, std::move(updated));
}

/// One operation's local preconditions and effect, shared by
/// `applyRefLogTxn`'s per-op loop and by `admits`'s single-op preview. `txn_id` is only read by
/// `RemoveNamespace` (it becomes the resulting `remove_txn_id`). Validation is identical regardless of
/// which apply strategy reached here, so this takes no mode.
void RefTableState::applyOp(const RefOp & op, const RefTxnId & txn_id)
{
    switch (op.kind)
    {
        case RefOpKind::NamespaceBirth:
        {
            /// Namespace birth is legal from an empty runtime admitted under a fresh catalog life,
            /// never from `Live`. A predecessor still `Removing` is refused before recovery.
            if (lifecycle == RefLifecycle::Live)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: namespace_birth while already Live");
            lifecycle = RefLifecycle::Live;
            remove_txn_id.reset();
            return;
        }
        case RefOpKind::OwnerTransition:
            applyOwnerTransition(op);
            return;
        case RefOpKind::SetPublishedAt:
            applySetPublishedAt(op);
            return;
        case RefOpKind::RemoveNamespace:
        {
            /// Remove namespace: requires Live and both owner sets already empty -- true only
            /// if this transaction's earlier ops (checked by `checkRemoveNamespaceOrdering`) actually
            /// named every owner that existed when the transaction started.
            if (lifecycle != RefLifecycle::Live)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: remove_namespace while not Live");
            if (!committed.empty() || !precommits.empty())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: remove_namespace with nonempty owner sets");
            /// `committed`/`precommits` empty implies `owned_manifests` empty too -- every entry in
            /// the index is put there by an ownership change to one of those two containers. A
            /// mismatch here means the index has drifted, not that this transaction is invalid.
            chassert(owned_manifests.empty());
            lifecycle = RefLifecycle::Removed;
            remove_txn_id = txn_id;
            return;
        }
        case RefOpKind::EpochSeal:
            /// INV-2's in-band epoch closure. A seal carries NO table content, and that is its whole
            /// design: its effect is that it OCCUPIES `{E, T+1}` -- the exact key a dying predecessor's
            /// in-flight PUT would have taken, so the store's own write-once create becomes the fence --
            /// and that it advances `greatest_applied`, which `applyTxnInPlace` does after this switch.
            /// There is deliberately nothing to do here: a seal that changed a row would be a seal that
            /// can lose one.
            ///
            /// `Live` is required because a seal closes the epoch of a LIVE stream. A never-born
            /// namespace has no stream to close, and a `Removed` one already closed its own with the
            /// terminal record -- in both cases a seal is a statement about a stream that does not
            /// exist, i.e. an object built against a different table's history. Recovery's CAS-walk
            /// enforces the identical gate on the minting side, so this is the read half of ONE rule
            /// rather than a second one that can drift from it.
            if (lifecycle != RefLifecycle::Live)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableState: epoch_seal at {}-{} while the namespace is not Live -- a seal closes the "
                    "epoch of a live stream, and this table has none",
                    txn_id.writer_epoch, txn_id.ref_sequence);
            return;
    }
    /// Reachable only through a hand-corrupted RefOpKind (mirrors CasRefLogCodec.cpp's
    /// exhaustive-switch-then-throw shape); every named enumerator returns above.
    throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableState: unknown op kind {}", static_cast<uint8_t>(op.kind));
}

RefTableState stateFromSnapshot(const RefTableSnapshot & snapshot)
{
    const String bytes = encodeRefTableSnapshot(snapshot);
    const RefTableSnapshot validated = decodeRefTableSnapshot(bytes, snapshot.ns, snapshot.snapshot_id);

    RefTableState state;
    /// A persisted snapshot is a materialization of a live stream only. `RefTableState` defaults to
    /// `Removed`, so this assignment is deliberately explicit rather than relying on construction
    /// defaults that have the opposite meaning.
    state.lifecycle = RefLifecycle::Live;
    state.greatest_applied = validated.snapshot_id;
    /// The codec validated sortedness and no-duplicate ref_name/(ref_name, manifest_ref), but NEVER
    /// cross-owner `manifest_ref` uniqueness -- a snapshot naming one manifest under two owners
    /// (committed/committed, committed/precommit, or precommit/precommit) is semantically corrupt and
    /// this is the one place that rejects it. The check runs before each `owned_manifests.insert` so it
    /// reports "corrupt snapshot data" rather than the container's "index drifted = code bug" framing,
    /// which would be the wrong diagnosis for malformed persisted data.
    for (const RefCommittedRow & row : validated.committed)
    {
        if (state.owned_manifests.contains(row.manifest_ref))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "stateFromSnapshot: snapshot names one manifest under two owners (committed ref '{}')", row.ref_name);
        state.committed.emplace(row.ref_name, row);
        state.snapshot_body_bytes += committedRowEncodedSize(row);
        state.removal_body_bytes  += removalOpEncodedSize(RefOwnerKind::Committed, row.ref_name, row.manifest_ref);
        state.owned_manifests.insert(row.manifest_ref);
    }
    for (const RefOwnerBinding & b : validated.precommits)
    {
        if (state.owned_manifests.contains(b.manifest_ref))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "stateFromSnapshot: snapshot names one manifest under two owners (precommit ref '{}')", b.ref_name);
        state.precommits.emplace(b.ref_name, b.manifest_ref);
        state.snapshot_body_bytes += precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, b.ref_name, b.manifest_ref});
        state.removal_body_bytes  += removalOpEncodedSize(RefOwnerKind::Precommit, b.ref_name, b.manifest_ref);
        state.owned_manifests.insert(b.manifest_ref);
    }
    return state;
}

#ifdef DEBUG_OR_SANITIZER_BUILD
/// Debug/sanitizer-only: recompute both body totals from scratch and assert the incrementally
/// maintained values match. This is what makes the incremental counters *provably* byte-exact rather
/// than a drift-prone estimate -- the concern the old non-incremental admits() cited. O(N); compiled
/// only in debug and sanitizer builds (`DEBUG_OR_SANITIZER_BUILD`, the same condition `chassert` fires
/// under), so an ASan/TSan run exercises it too, not just a debug build.
///
/// Also rebuilds the expected `owned_manifests` membership by scanning `committed` + `precommits`
/// (the same linear walk `manifestAlreadyOwned` used to do directly) and cross-checks it against the
/// COW index: every scanned manifest must be present in the index, and the index's total size must
/// equal the number of rows scanned -- together those two checks catch both a missing entry and a
/// stale/extra one, which a size-only or membership-only check could each miss on their own.
void RefTableState::debugAssertBodyCounters() const
{
    uint64_t snap = 0;
    uint64_t rem = 0;
    size_t owned_scanned = 0;
    for (const auto [name, row] : committed)
    {
        snap += committedRowEncodedSize(row);
        rem  += removalOpEncodedSize(RefOwnerKind::Committed, name, row.manifest_ref);
        chassert(owned_manifests.contains(row.manifest_ref));
        ++owned_scanned;
    }
    for (const auto & [name, mref] : precommits)
    {
        snap += precommitRowEncodedSize(RefOwnerBinding{RefOwnerKind::Precommit, name, mref});
        rem  += removalOpEncodedSize(RefOwnerKind::Precommit, name, mref);
        chassert(owned_manifests.contains(mref));
        ++owned_scanned;
    }
    chassert(snapshot_body_bytes == snap);
    chassert(removal_body_bytes == rem);
    chassert(owned_manifests.size() == owned_scanned);
}
#endif

void RefTableState::applyTxnInPlace(const RefLogTxn & txn)
{
    /// Txn-wide preconditions first, before any mutation.
    if (!(greatest_applied < txn.txn_id))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableState: txn_id {}-{} is not strictly greater than the greatest applied {}-{}",
            txn.txn_id.writer_epoch, txn.txn_id.ref_sequence,
            greatest_applied.writer_epoch, greatest_applied.ref_sequence);

    /// INV-1: within `(namespace, epoch)` the DURABLE ids are DENSE, so the only admissible id is the
    /// one `nextTxnId` derives -- the same rule, on the same state, that the writer mints with. This is
    /// what turns "these are the log ids I can see" into "this is the whole stream": a reader that finds
    /// `1..T` knows nothing was lost, which no amount of strict-increase checking could tell it.
    /// Enforcing it HERE, on the read side, is deliberate -- a hole cannot become durable even if some
    /// future writer path forgets the rule, because every apply (writer candidate, recovery replay,
    /// `fsck`'s oracle) runs it.
    ///
    /// The strict-increase check above is not subsumed by this one: it rejects an id under an OLDER
    /// epoch, whose sequence would still look like a legitimate fresh-epoch `1`.
    if (const RefTxnId expected = nextTxnId(txn.txn_id.writer_epoch); txn.txn_id != expected)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableState: txn_id {}-{} does not continue the ref-log stream — the greatest applied id "
            "is {}-{}, so the only contiguous successor is {}-{}",
            txn.txn_id.writer_epoch, txn.txn_id.ref_sequence,
            greatest_applied.writer_epoch, greatest_applied.ref_sequence,
            expected.writer_epoch, expected.ref_sequence);

    /// INV-2's CONTEXTUAL seal grammar, on the read side. `prev_epoch_seal` is required on exactly
    /// sequence 1 of a NON-genesis epoch and forbidden everywhere else; the structural half (shape,
    /// well-formedness, strictly-earlier epoch) is the codec's, and this is the half that needs to know
    /// whether this transaction OPENS a life or CONTINUES one across a transition.
    ///
    /// THE EQUIVALENCE ARGUMENT, because this is the load-bearing part. The answer is DERIVED from the
    /// state, exactly and totally -- no `life_epoch` is plumbed in, and there is no optional to
    /// substitute a zero for (which would demand a chain link on every sequence-1 transaction and reject
    /// every genesis birth -- the trap task 5's interface note names):
    ///
    ///   - state NOT `Live` (never-born, or `Removed`) <=> this transaction can only be a BIRTH <=> the
    ///     epoch it lands in IS this life's genesis epoch, so a chain link is FORBIDDEN. A link here
    ///     would name a seal of a previous life this state has no trace of;
    ///   - state `Live` <=> a prior life is PROVEN to exist below this epoch -- the namespace was applied
    ///     at `greatest_applied.writer_epoch`, and the density check immediately above just proved this
    ///     transaction's epoch is above it -- so the transition is non-genesis and a chain link is
    ///     REQUIRED. Passing `greatest_applied.writer_epoch` yields the same verdict the true
    ///     `life_epoch` would for EVERY value the true one can hold, since the true one is at or below it
    ///     and the rule only compares "strictly above / at or below".
    ///
    /// The derivation is exact for every reachable state and no default exists anywhere in it.
    ///
    /// It is also STRICTLY BETTER than a plumbed-in `life_epoch` would be, and not only because it needs
    /// no plumbing: `life_epoch` is a property of a LIFE, and a namespace can be removed and recreated.
    /// A single global value carried alongside the table would answer for the wrong life after a
    /// rebirth -- it would demand a chain link from a recreated namespace's very first transaction,
    /// which by definition has none. Reading the lifecycle instead makes per-life semantics fall out for
    /// free: a `Removed` state receiving a birth makes THAT epoch the NEW life's genesis, with no
    /// catalog and no extra field to keep in step.
    ///
    /// Both arms go through the ONE shared validator rather than re-deriving its rule, so the writer's
    /// encode-side check and this one cannot drift.
    if (txn.txn_id.ref_sequence == 1)
        validateEpochSealGrammarContextual(
            txn, lifecycle == RefLifecycle::Live ? greatest_applied.writer_epoch : txn.txn_id.writer_epoch);

    checkRemoveNamespaceOrdering(txn.ops);

    /// Apply every op, in array order, IN PLACE. A throw leaves `*this` PARTIALLY APPLIED ("poisoned").
    /// This is the poisoning strategy (E3, no scratch copy): sound ONLY on a state the caller discards
    /// on any throw. The public `applyRefLogTxn` reaches it only through a scratch copy (turning it into
    /// the strong guarantee); `replay` reaches it directly on its own local, discard-on-throw state,
    /// which is what eliminates the per-transaction deep-copy of the replay tail's unbounded COW
    /// overlays -- a K-transaction replay over an N-row base drops from O(K*N) to O(K + N).
    for (const RefOp & op : txn.ops)
        applyOp(op, txn.txn_id);
    greatest_applied = txn.txn_id;
#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Reached only on success, where `*this` is fully applied and its incremental body counters and
    /// owned-manifest index are consistent -- the invariant this cross-check defends.
    debugAssertBodyCounters();
#endif
}

RefTxnId nextRefTxnId(RefTxnId greatest_applied, uint64_t live_epoch)
{
    return greatest_applied.writer_epoch == live_epoch
        ? RefTxnId{live_epoch, greatest_applied.ref_sequence + 1}
        : RefTxnId{live_epoch, 1};
}

void applyRefLogTxn(RefTableState & state, const RefLogTxn & txn)
{
    /// The one public apply entry point, ALWAYS the strong exception guarantee: validate and apply the
    /// whole transaction against a scratch copy; replace `state` only once the whole transaction
    /// succeeds, so a throw anywhere leaves `state` byte-for-byte unchanged and no intra-transaction
    /// intermediate state is ever observable. This copy is cheap on every caller: each applies against a
    /// materialized (empty-overlay) live state or a small bounded-overlay batch scratch, never the
    /// unbounded replay tail (that is `replay`'s job, and it uses the private in-place strategy directly
    /// on its own discard-on-throw local state).
    RefTableState scratch = state;
    scratch.applyTxnInPlace(txn);
    state = std::move(scratch);
}

RefTableSnapshot snapshotOf(const RefTableState & state, const String & ns)
{
    if (state.getLifecycle() != RefLifecycle::Live)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "snapshotOf: terminal namespace state is not snapshot-serializable");

    RefTableSnapshot snapshot;
    snapshot.ns = ns;
    snapshot.snapshot_id = state.getGreatestApplied();

    snapshot.committed.reserve(state.getCommitted().size());
    for (const auto [name, row] : state.getCommitted())
        snapshot.committed.push_back(row);   /// RefCowMap iterates sorted by ref_name (Pool/CasRefCowMap.h)

    snapshot.precommits.reserve(state.getPrecommits().size());
    for (const auto & [name, manifest_ref] : state.getPrecommits())
        snapshot.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, name, manifest_ref});
        /// std::set<std::pair<String, ManifestRef>> iterates sorted by (ref_name, manifest_ref),
        /// matching CasRefSnapshotCodec's required precommit sort order exactly.

    return snapshot;
}

RefTableState replay(const std::optional<RefTableSnapshot> & snapshot, std::span<const RefLogTxn> tail)
{
    RefTableState state = snapshot ? stateFromSnapshot(*snapshot) : RefTableState{};

    const String * expected_ns = snapshot ? &snapshot->ns : nullptr;
    for (const RefLogTxn & txn : tail)
    {
        if (expected_ns && txn.ns != *expected_ns)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefTableState::replay: transaction ns '{}' does not match the table's ns '{}'",
                txn.ns, *expected_ns);
        expected_ns = &txn.ns;
        /// The one place the poisoning in-place apply strategy is reached (E3): `state` is `replay`'s own
        /// local, returned only after the WHOLE tail succeeds, so a mid-tail throw destroys it during
        /// unwinding and no consumer ever observes a poisoned state. `applyTxnInPlace` still runs the
        /// FULL validation `applyRefLogTxn` does -- including the cross-owner uniqueness check, which is
        /// O(1) and no longer elided (post-consult) -- so a corrupted/collision-bearing tail fails closed
        /// here rather than silently drifting the index.
        state.applyTxnInPlace(txn);
    }
    return state;
}

RefReplayBuilder::RefReplayBuilder(std::optional<RefTableSnapshot> base, uint64_t base_encoded_bytes)
{
    if (base)
    {
        result.newest_snapshot_id = base->snapshot_id;
        result.base_snapshot_bytes = base_encoded_bytes;
        expected_ns = base->ns;
        candidate = stateFromSnapshot(*base);   /// full snapshot revalidation, exactly as `replay`
    }
}

void RefReplayBuilder::applyOne(RefLogTxn && txn, uint64_t encoded_bytes)
{
    /// The streaming-recovery memory probe is driven by the CALLER's loop (around GET->decode->this
    /// call->discard), not here: the alive decoded-transaction set is a property of how the loop holds
    /// its transactions, which `applyOne` -- seeing one at a time regardless -- cannot observe. See
    /// `reportReplayMemoryDelta` / `decodedRefLogTxnFootprint`.
    if (expected_ns && txn.ns != *expected_ns)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefReplayBuilder: transaction ns '{}' does not match the table's ns '{}'",
            txn.ns, *expected_ns);
    expected_ns = txn.ns;
    /// The same private in-place poisoning path `replay` uses (E3): `candidate` is this builder's own
    /// state, discarded on any throw (the builder is destroyed during unwinding), so a mid-tail
    /// corruption fails closed here and no consumer ever observes a poisoned candidate. NOT the public
    /// scratch-copying `applyRefLogTxn`, which would deep-copy the growing candidate once per
    /// transaction and reintroduce the O(K*N) cost `replay` was written to avoid.
    candidate.applyTxnInPlace(txn);
    ++result.tail_count;
    result.tail_bytes += encoded_bytes;
}

RecoveryResult RefReplayBuilder::finish() &&
{
    /// Matches `replay`: the candidate is returned WITHOUT `materializeCommitted` -- the writer's
    /// recovery folds the COW overlays once on the result before installing it; the read-only consumers
    /// (orphan sweep and fsck) do not need the fold at all.
    result.state = std::move(candidate);
    return std::move(result);
}

uint64_t encodedSnapshotBudgetSize(const RefTableState & state)
{
    /// `snapshotOf` uses `snapshot_id = state.greatest_applied` and an empty namespace here. Snapshot
    /// lifecycle is fixed to live on the wire, so the framing depends only on those fields and the row
    /// count.
    const uint64_t rows = state.getCommitted().size() + state.getPrecommits().size();
    return snapshotFramingSize("", state.getGreatestApplied(), rows)
        + state.getSnapshotBodyBytes();
}

uint64_t encodedRemovalBudgetSize(const RefTableState & state)
{
    /// The hypothetical whole-namespace removal transaction uses a fixed {1,1} preview id, empty ns,
    /// and one removal op per owner (committed + precommit) plus a terminal remove_namespace op -- so
    /// op_count = committed + precommits + 1.
    static constexpr RefTxnId kPreviewTxnId{1, 1};
    const uint64_t rows = state.getCommitted().size() + state.getPrecommits().size();
    return removalFramingSize("", kPreviewTxnId, rows + 1) + state.getRemovalBodyBytes();
}

bool admits(const RefTableState & state, const RefOp & op, uint64_t snapshot_budget, uint64_t removal_budget)
{
    /// A fixed nonzero placeholder id: this previews `op` in isolation and the scratch state is
    /// discarded immediately after reading its (incrementally maintained) budget sizes.
    static constexpr RefTxnId kPreviewTxnId{1, 1};

    /// Previews an op that has not yet been validated or durably appended anywhere, so it gets the full
    /// append-time check (the same `applyOp` the writer's apply path runs -- validation is strategy-
    /// independent).
    RefTableState scratch = state;
    scratch.applyOp(op, kPreviewTxnId);   // throws exactly as before if `op` is not a legal transition
#ifdef DEBUG_OR_SANITIZER_BUILD
    scratch.debugAssertBodyCounters();
#endif

    if (encodedSnapshotBudgetSize(scratch) > snapshot_budget)
        return false;
    return encodedRemovalBudgetSize(scratch) <= removal_budget;
}

std::vector<RefManifestEdge> manifestEdgesOfTxn(const RefLogTxn & txn)
{
    std::vector<RefManifestEdge> edges;
    edges.reserve(txn.ops.size());   /// every recognized op contributes at most one edge
    const RootNamespace ns{txn.ns};

    for (uint32_t op_ordinal = 0; op_ordinal < txn.ops.size(); ++op_ordinal)
    {
        const RefOp & op = txn.ops[op_ordinal];
        if (op.kind != RefOpKind::OwnerTransition)
            continue;

        /// Shape legality is decided once, by the shared classifier -- the same one
        /// `RefTableState::applyOwnerTransition` dispatches on -- so an unrecognized shape throws here
        /// instead of silently acquiring accidental edge meaning (e.g. an old+new pair naming different
        /// manifests, which the state machine never admits, used to read as a tolerated "replace").
        switch (classifyOwnerTransitionShape(op))
        {
            case OwnerTransitionShape::AddPrecommit:
                edges.push_back(RefManifestEdge{
                    ManifestId{ns, op.new_binding->manifest_ref}, +1, op.new_binding->kind, op_ordinal, 1});
                continue;
            case OwnerTransitionShape::RemovePrecommit:
            case OwnerTransitionShape::RemoveCommitted:
                edges.push_back(RefManifestEdge{
                    ManifestId{ns, op.old_binding->manifest_ref}, -1, op.old_binding->kind, op_ordinal, 0});
                continue;
            case OwnerTransitionShape::Promote:
                /// Same-manifest owner move: the manifest keeps an owner the whole time, so there is no
                /// net edge.
                continue;
        }
        /// Reachable only if a future `OwnerTransitionShape` enumerator is added without a matching
        /// `case` -- `-Wswitch`/`-Werror` catches that at compile time; this throw is the runtime
        /// backstop for builds without it.
        throw Exception(ErrorCodes::CORRUPTED_DATA, "manifestEdgesOfTxn: unhandled owner_transition shape");
    }

    return edges;
}

std::optional<RefTxnId> removalTxnId(const RefLogTxn & txn)
{
    for (const RefOp & op : txn.ops)
        if (op.kind == RefOpKind::RemoveNamespace)
            return txn.txn_id;
    return std::nullopt;
}

std::map<NamespaceLifePhysicalId, RefTableListing> groupRefKeys(
    const Layout & layout, const std::vector<String> & listed_keys)
{
    const String base = layout.casRefsPrefix();
    std::map<NamespaceLifePhysicalId, RefTableListing> out;

    for (const String & key : listed_keys)
    {
        if (!key.starts_with(base))
            continue;

        const auto parsed = layout.parseRefObjectKey(key);
        if (!parsed)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "groupRefKeys: key '{}' under the ref prefix is not a valid ref object -- aborting ref folding", key);

        RefTableListing & table = out[parsed->life_id];
        switch (parsed->kind)
        {
            case RefObjectKind::Log:
                table.logs.push_back(parsed->txn_id);
                break;
            case RefObjectKind::Snap:
                table.snapshots.push_back(parsed->txn_id);
                break;
        }
    }

    for (auto & [life_id, table] : out)
    {
        std::sort(table.logs.begin(), table.logs.end());
        std::sort(table.snapshots.begin(), table.snapshots.end());
    }

    return out;
}

RefCleanupPlan planRefCleanup(const RefTableListing & listing, const RefTxnId & durable_cursor,
                              std::optional<RefTxnId> checkpoint,
                              std::optional<RefTxnId> retained_log_proof)
{
    RefCleanupPlan plan;

    /// A physical `_snap` listed after its PUT but before the `_ckpt` CAS is not a recovery base.
    /// The caller supplies `checkpoint` only after `readCheckpointSnapshotBase` has exact-read the
    /// same-id non-seal `_log` and `_snap`. With no such validated triple cleanup has no coverage
    /// authority and deliberately leaks every listed object.
    if (!checkpoint)
        return plan;

    for (const RefTxnId & log_id : listing.logs)
    {
        if (durable_cursor < log_id)       /// L > cursor: its edge delta is not yet durable
            continue;
        if (*checkpoint <= log_id)         /// the exact witness and its successors remain
            continue;
        if (retained_log_proof == log_id)  /// a later-epoch base still needs its predecessor seal
            continue;
        plan.deletable_logs.push_back(log_id);
    }

    /// The checkpoint's same-id snapshot is the recovery base, so only strictly older listed snapshots
    /// are deletion candidates.
    for (const RefTxnId & snapshot_id : listing.snapshots)
        if (snapshot_id < *checkpoint)
            plan.deletable_snapshots.push_back(snapshot_id);

    return plan;
}

EpochCrossResult crossEpochFromSeal(Backend & backend, const Layout & layout, const RootNamespace & ns,
                                    const RefTxnId & from_seal, std::optional<bool> seal_proven,
                                    const RefTxnId & witness, const NamespaceLifeId & life)
{
    EpochCrossResult result;
    if (from_seal == RefTxnId{})
    {
        result.outcome = EpochCrossOutcome::NothingConsumed;
        return result;
    }
    if (seal_proven && !*seal_proven)
    {
        result.outcome = EpochCrossOutcome::NotASeal;
        return result;
    }

    /// `life`: REQUIRED, not resolved here (review NEW-3) -- an internal fallback resolve was tried
    /// once already (review C3, `Gc::fold`) and once more here (fsck's own independent walk defaulted
    /// to `nullopt` and re-resolved), and both times a caller that had already committed to one `life`
    /// for the rest of its walk could silently diverge from this function's OWN resolution if the
    /// namespace is dropped and recreated between the two reads. `CasFsck.cpp`'s stream walk resolves
    /// `life` once, at the top of its own function, and must pass that SAME value here rather than let
    /// this function re-derive it a second time.
    uint64_t target_epoch = witness.writer_epoch;
    while (target_epoch > from_seal.writer_epoch)
    {
        const RefTxnId start{target_epoch, 1};
        result.probed = start;
        const auto body = backend.get(layout.refLogKey(life, start));
        if (!body)
        {
            ++result.absent_probes;
            result.outcome = EpochCrossOutcome::StartAbsent;
            return result;
        }
        ++result.body_gets;
        RefLogTxn head;
        try
        {
            head = decodeRefLogTxn(openObject(FormatId::RefLog, body->bytes), ns.string(), start);
        }
        catch (const Exception & e)
        {
            result.outcome = EpochCrossOutcome::StartInvalid;
            result.detail = e.message();
            return result;
        }
        if (!head.prev_epoch_seal || *head.prev_epoch_seal < from_seal)
        {
            result.outcome = EpochCrossOutcome::ChainDoesNotReach;
            return result;
        }
        if (*head.prev_epoch_seal == from_seal)
        {
            result.outcome = EpochCrossOutcome::Proved;
            result.start = start;
            return result;
        }
        target_epoch = head.prev_epoch_seal->writer_epoch;
    }
    result.outcome = EpochCrossOutcome::ChainDoesNotReach;
    return result;
}

std::optional<RefTxnId> nextRefLogIdWithinCommittedFrontier(
    const RefTxnId & current, bool is_epoch_seal, const RefTxnId & committed_through)
{
    if (committed_through < current)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS checkpoint-bounded ref walk: current id {}-{} lies above committed_through {}-{}",
            current.writer_epoch, current.ref_sequence,
            committed_through.writer_epoch, committed_through.ref_sequence);
    if (current == committed_through)
        return std::nullopt;

    if (is_epoch_seal)
    {
        if (committed_through.writer_epoch == current.writer_epoch)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS checkpoint-bounded ref walk: committed_through {}-{} lies after EpochSeal {}-{} in "
                "the same numeric epoch",
                committed_through.writer_epoch, committed_through.ref_sequence,
                current.writer_epoch, current.ref_sequence);
        if (current.writer_epoch == std::numeric_limits<uint64_t>::max())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS checkpoint-bounded ref walk: EpochSeal {}-{} has no representable successor",
                current.writer_epoch, current.ref_sequence);
        return RefTxnId{current.writer_epoch + 1, 1};
    }

    if (current.ref_sequence == std::numeric_limits<uint64_t>::max())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS checkpoint-bounded ref walk: log id {}-{} has no representable successor",
            current.writer_epoch, current.ref_sequence);
    return RefTxnId{current.writer_epoch, current.ref_sequence + 1};
}

CheckpointSnapshotBase readCheckpointSnapshotBase(
    Backend & backend, const Layout & layout, const NamespaceLifeId & life, const RefCkpt & checkpoint)
{
    const RootNamespace & ns = life.ns;
    if (!checkpoint.checkpoint_snapshot_id)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint has no snapshot base",
            ns.string());
    }
    if (!checkpoint.life_epoch)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint-named snapshot base has no life_epoch context",
            ns.string());
    }
    const RefTxnId snapshot_id = *checkpoint.checkpoint_snapshot_id;
    const auto log = backend.get(layout.refLogKey(life, snapshot_id));
    if (!log)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} has no matching log under "
            "the supplied immutable lifecycle authority",
            ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence);
    }

    const RefLogTxn base_txn = decodeRefLogTxn(
        openObject(FormatId::RefLog, log->bytes), ns.string(), snapshot_id);
    if (refLogTxnIsEpochSeal(base_txn))
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} names an EpochSeal, "
            "not a snapshot base",
            ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence);
    }
    validateEpochSealGrammarContextual(base_txn, *checkpoint.life_epoch);
    if (base_txn.prev_epoch_seal && checkpoint.last_epoch_seal && checkpoint.committed_through
        && checkpoint.committed_through->writer_epoch == snapshot_id.writer_epoch
        && checkpoint.last_epoch_seal->writer_epoch + 1 == snapshot_id.writer_epoch
        && *base_txn.prev_epoch_seal != *checkpoint.last_epoch_seal)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} refers to previous "
            "epoch seal {}-{}, but checkpoint authority names {}-{}",
            ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence,
            base_txn.prev_epoch_seal->writer_epoch, base_txn.prev_epoch_seal->ref_sequence,
            checkpoint.last_epoch_seal->writer_epoch, checkpoint.last_epoch_seal->ref_sequence);
    }

    std::optional<RefTxnId> predecessor_seal_id;
    if (base_txn.prev_epoch_seal)
    {
        predecessor_seal_id = *base_txn.prev_epoch_seal;
        const auto predecessor = backend.get(layout.refLogKey(life, *predecessor_seal_id));
        if (!predecessor)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} refers to absent "
                "previous epoch seal {}-{}",
                ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence,
                predecessor_seal_id->writer_epoch, predecessor_seal_id->ref_sequence);
        }
        const RefLogTxn predecessor_txn = decodeRefLogTxn(
            openObject(FormatId::RefLog, predecessor->bytes), ns.string(), *predecessor_seal_id);
        if (!refLogTxnIsEpochSeal(predecessor_txn))
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} refers to non-seal "
                "transaction {}-{}",
                ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence,
                predecessor_seal_id->writer_epoch, predecessor_seal_id->ref_sequence);
        }
    }

    const auto snapshot = backend.get(layout.refSnapshotKey(life, snapshot_id));
    if (!snapshot)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery for namespace '{}': checkpoint-named base snapshot {}-{} is absent under the supplied "
            "immutable lifecycle authority",
            ns.string(), snapshot_id.writer_epoch, snapshot_id.ref_sequence);
    }
    return CheckpointSnapshotBase{
        .snapshot = decodeRefTableSnapshot(openObject(FormatId::RefSnapshot, snapshot->bytes), ns.string(), snapshot_id),
        .bytes = snapshot->bytes.size(),
        .predecessor_seal_id = predecessor_seal_id};
}

RecoveredRefTable recoverRefTableDetailedFromAuthority(
    Backend & backend, const Layout & layout, const std::optional<CatalogEntry> & catalog_entry,
    const std::optional<RefCkpt> & ckpt)
{
    /// The frozen catalog row and `_ckpt` supplied by the caller determine every recovery boundary;
    /// this function must not re-read either mutable object, or enumerate the stream, because that
    /// would splice unrelated physical observations into the caller's one authority cut.
    RecoveryGrounding grounding = chooseRecoveryGrounding(catalog_entry, ckpt);
    /// `chooseRecoveryGrounding` has just established that this is a Live/Removing row. Constructing
    /// the life from that SAME value, rather than resolving the name again, preserves the caller's
    /// catalog-cut join.
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(
        catalog_entry->ns, catalog_entry->incarnation);
    const RootNamespace & ns = life.ns;

    std::optional<RefTxnId> base_id = grounding.base;
    std::optional<RefTableSnapshot> base_snapshot;
    uint64_t base_snapshot_bytes = 0;
    if (base_id)
    {
        CheckpointSnapshotBase base = readCheckpointSnapshotBase(backend, layout, life, *ckpt);
        base_snapshot = std::move(base.snapshot);
        base_snapshot_bytes = base.bytes;
    }

    RefReplayBuilder builder(std::move(base_snapshot), base_snapshot_bytes);
    if (grounding.walk_from && grounding.committed_through)
    {
        RefTxnId id = *grounding.walk_from;
        while (id <= *grounding.committed_through)
        {
            const auto got = backend.get(layout.refLogKey(life, id));
            if (!got)
            {
                /// `NamespaceLifeId` is opaque and unique to one logical life. A later birth has a
                /// different stream prefix, so no absent slot at or below this life's exact frontier
                /// can be explained as a rebirth; it is always durable-data loss.
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS read-only recovery for namespace '{}': committed log id {}-{} is absent under "
                    "the supplied immutable checkpoint frontier {}-{}",
                    ns.string(), id.writer_epoch, id.ref_sequence,
                    grounding.committed_through->writer_epoch, grounding.committed_through->ref_sequence);
            }

            RefLogTxn txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), id);
            const bool is_seal = refLogTxnIsEpochSeal(txn);
            const int64_t footprint = static_cast<int64_t>(decodedRefLogTxnFootprint(txn));
            reportReplayMemoryDelta(footprint);
            SCOPE_EXIT({ reportReplayMemoryDelta(-footprint); });
            builder.applyOne(std::move(txn), got->bytes.size());

            if (const std::optional<RefTxnId> next = nextRefLogIdWithinCommittedFrontier(
                    id, is_seal, *grounding.committed_through))
                id = *next;
            else
                break;
        }
    }

    RecoveryResult result = std::move(builder).finish();
    return RecoveredRefTable{
        .state = std::move(result.state),
        .newest_snapshot_id = result.newest_snapshot_id,
        .last_epoch_seal = ckpt->last_epoch_seal};
}

}
