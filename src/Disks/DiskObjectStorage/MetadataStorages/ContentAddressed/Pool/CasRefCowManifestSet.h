#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/defines.h>
#include <cstdint>
#include <map>
#include <memory>
#include <unordered_set>
#include <utility>

namespace DB::Cas
{

/// A copy-cheap membership set of every `ManifestRef` that currently has an owner (a committed row
/// or a precommit binding). `RefTableState` (Pool/CasRefProtocol.h) uses it to hold the
/// add-precommit uniqueness invariant ("no conflicting owner may name the same manifest") as a
/// structure instead of `manifestAlreadyOwned`'s old linear scan over `committed` + `precommits`.
///
/// Same copy-on-write shape as `RefCowMap` (Pool/CasRefCowMap.h): copies share an immutable `base`
/// set (a `shared_ptr` refcount bump, no per-element copy) and differ only in a per-copy `overlay`,
/// so the copy-then-mutate-then-swap pattern `applyRefLogTxn`'s scratch copy uses stays O(overlay
/// size), never O(table size) -- exactly the regression `RefCowMap` was already built to avoid, and
/// a plain `std::set<ManifestRef>` member would have reintroduced here. Membership-only: unlike
/// `RefCowMap` there is no ordered (or any) iteration, because nothing in `RefTableState` ever needs
/// to enumerate owned manifests, only ask "does any owner already name this one". Not thread-safe;
/// same ownership rules as `RefCowMap` (callers retain the state lock or detached-copy ownership
/// rules of the state that contains it).
///
/// `base` is `std::unordered_set` (O(1) lookup, the point of this class at large table size), but
/// `overlay` is deliberately `std::map`, not `std::unordered_map`, even though it holds the exact
/// same key type: `overlay` is copied on EVERY `RefTableState` scratch copy (unlike `base`, which is
/// shared), and libstdc++'s `unordered_map` copy constructor allocates a real bucket array even for
/// an empty source (measured ~30ns/copy via `.claude/tools/cppexpr.sh`, versus effectively free for
/// an empty `std::map` -- the same reason `RefCowMap`'s own `overlay` is a `std::map`, not an
/// `unordered_map`). The common case is an empty-or-few-entries overlay between flushes, so this
/// keeps the added cost of `owned_manifests` on `BM_ScratchCopy` to roughly one more `shared_ptr`
/// copy, not one more `shared_ptr` copy plus a hidden allocation.
///
/// `insert`/`erase` throw `CORRUPTED_DATA` on a violated precondition (absence / presence,
/// respectively) in EVERY build rather than merely `chassert`-ing it: the ref table's own uniqueness
/// invariant already guarantees both, so a violation here means the index itself has drifted from
/// `committed`/`precommits`. A `chassert` would let that drift through silently in a release build,
/// after which a single `erase` could report a manifest absent while another owner still names it --
/// corrupting the add-precommit uniqueness invariant and GC's `+1/-1` manifest-edge accounting
/// downstream. Failing closed keeps `net_delta` from ever drifting. This is the same class of bug
/// `RefTableState::debugAssertBodyCounters` cross-checks in debug/sanitizer builds; the throw extends
/// the guarantee to release builds too.
class RefCowManifestSet
{
public:
    /// Bucket hashing comes from the existing `std::hash<ManifestRef>` specialization
    /// (Primitives/CasTypes.h), picked up by default. Membership hashing only -- this set is never
    /// exposed to attacker-chosen keys, only to manifest refs this process itself allocated, so
    /// adversarial collision resistance is not a concern here.
    using Base = std::unordered_set<ManifestRef>;

    RefCowManifestSet() = default;

    /// True iff `m` currently has an owner: present in the merged base+overlay view. An overlay
    /// tombstone reports absent even when `base` still has `m`.
    bool contains(const ManifestRef & m) const;

    /// Records `m` as owned. `m` must be absent from the merged view or this throws `CORRUPTED_DATA`
    /// (in every build) -- the caller's own uniqueness check is what actually enforces the invariant;
    /// this only guards against the index drifting away from it, failing closed rather than silently.
    void insert(const ManifestRef & m);

    /// Records `m` as no longer owned. `m` must be present in the merged view or this throws
    /// `CORRUPTED_DATA` (in every build), same rationale as `insert`.
    void erase(const ManifestRef & m);

    /// `base->size() + net_delta`, O(1).
    size_t size() const { return static_cast<size_t>(static_cast<int64_t>(base->size()) + net_delta); }
    bool empty() const { return size() == 0; }

    /// Folds `overlay` into `base` and clears the overlay. Call this at the same state-install point
    /// `RefCowMap::materialize()` is called from (once per ref-log flush, never once per batch item).
    /// If the overlay is already empty, this is a no-op.
    ///
    /// When `base` is uniquely owned (`use_count() == 1`, the production flush case), the overlay is
    /// folded into `*base` IN PLACE -- O(overlay), no O(N) `unordered_set` copy. When a copy still
    /// shares `base`, a fresh merged base is built and swapped in, so the shared holder's view stays
    /// byte-unchanged. The full ownership-and-coherence safety argument is `RefCowMap::materialize`'s
    /// (the `use_count()` of 1 is stable against both a concurrent increment and any cross-thread
    /// release, every such release being lock-ordered under `state_mutex`; the in-place fold is coherent
    /// at every throw point; `base` is a non-const `shared_ptr` so no `const_cast` is needed). Both paths
    /// leave an empty overlay and `net_delta == 0`.
    void materialize();

    /// Member-wise swap, guaranteed non-throwing AND allocation-free, with the same contract and the
    /// same install-time purpose as `RefCowMap::swap` (Pool/CasRefCowMap.h): `shared_ptr::swap` and
    /// `std::map::swap` exchange pointers only, `net_delta` is a POD. The swapped-out set keeps its
    /// former base reference until it is destroyed, so a caller that folds the installed set must
    /// destroy the swapped-out one first.
    void swap(RefCowManifestSet & other) noexcept
    {
        base.swap(other.base);
        overlay.swap(other.overlay);
        std::swap(net_delta, other.net_delta);
    }

    /// Test-only: current overlay entry count (0 right after `materialize()`).
    size_t overlayEntriesForTest() const { return overlay.size(); }
    /// Test-only: `base`'s `shared_ptr::use_count()` -- a copy that shares `base` (no per-element
    /// allocation) bumps this by exactly one.
    int64_t baseUseCountForTest() const { return base.use_count(); }
    /// Test-only: identity of the current `base` allocation. `materialize()` on a uniquely-owned base
    /// folds the overlay in place and leaves this unchanged; on a base still shared with a copy it
    /// swaps in a fresh base, changing it. Lets a test tell the fast (in-place) path from the copy path.
    const void * baseIdentityForTest() const { return base.get(); }

private:
    /// Non-const so `materialize()` can fold the overlay into `*base` in place when it is the sole
    /// owner (see `materialize`'s doc for the safety argument). It is never mutated while shared.
    std::shared_ptr<Base> base = std::make_shared<Base>();
    /// `true` = an overlay addition (present, whether or not `base` also has it); `false` = a
    /// tombstone shadowing a `base` member. An overlay-only member that is erased is removed from
    /// this map outright rather than tombstoned (nothing left to shadow), mirroring `RefCowMap`.
    /// `std::map`, not `std::unordered_map`: see the class doc comment above -- this is what keeps an
    /// empty overlay's copy cost negligible.
    std::map<ManifestRef, bool> overlay;
    /// `size() = base->size() + net_delta`, maintained in lock-step by `insert`/`erase` so
    /// `size()`/`empty()` stay O(1). Counts live overlay changes relative to `base`.
    int64_t net_delta = 0;
};

}
