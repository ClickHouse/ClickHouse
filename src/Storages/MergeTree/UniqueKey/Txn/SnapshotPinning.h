#pragma once

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <functional>
#include <memory>
#include <utility>

namespace DB::UniqueKeyTxn
{

/// Pin lifecycle + cluster floor for GC safety. A pin tracks "some reader
/// still needs bitmaps at csn ≥ X" — GC's `clusterFloor()` surfaces the
/// oldest pin so the bitmap-GC rule (`V_next ≤ floor`) is safe.
///
/// Local: in-memory map keyed by csn under `pins_mutex`.
///
/// Required ProfileEvents:
///   UniqueKeyPinAcquireMicros — `acquire()` wall-time.
class IPinRegistry
{
public:
    virtual ~IPinRegistry() = default;

    /// Per-acquired-pin handle. RAII: destruction releases the pin.
    class PinHandle
    {
    public:
        PinHandle(CSN csn, std::function<void()> on_release_)
            : pinned_csn(csn), on_release(std::move(on_release_)) {}

        PinHandle(const PinHandle &) = delete;
        PinHandle & operator=(const PinHandle &) = delete;

        ~PinHandle()
        {
            if (on_release)
                on_release();
        }

        CSN pinnedCsn() const noexcept { return pinned_csn; }

    private:
        CSN pinned_csn;
        std::function<void()> on_release;
    };

    /// Acquire a pin at `csn`. MUST be called before the corresponding
    /// `PartitionView` is consumed — the pin must be visible to GC
    /// before the first scan I/O.
    virtual std::shared_ptr<PinHandle> acquire(CSN csn) = 0;

    /// Cluster oldest_snapshot_csn for GC. On Local, returns
    /// `min(pins.keys())` under `pins_mutex`. Empty pin set → `MAX_CSN`
    /// (no live readers; GC may delete down to `committed_csn`).
    virtual CSN clusterFloor() = 0;

protected:
    /// Helper to manufacture a PinHandle from a friend strategy without
    /// duplicating the ctor wiring. `on_release` runs on the PinHandle's
    /// destruction thread; the strategy is responsible for any locking it
    /// needs there.
    static std::shared_ptr<PinHandle> makeHandle(CSN csn, std::function<void()> on_release)
    {
        return std::make_shared<PinHandle>(csn, std::move(on_release));
    }
};

/// Immutable read-side view: the snapshot's `csn` plus a `csn`-bound bitmap
/// lookup. The view and its pin are captured as ONE observation inside
/// `PartitionTxnController::takeQuerySnapshot`. Read-side consumers anchor part
/// visibility on each part's intrinsic `creation_csn ≤ csn` (see
/// `UniqueKeyReadFilter::isPartVisibleAtSnapshotCsn`), so the view carries no
/// part list of its own.
struct PartitionView
{
    CSN csn = INVALID_CSN;
    /// Bound per-snapshot bitmap lookup. Closes over `bitmap_store`
    /// (raw pointer owned by the partition's `PartitionTxnController`) +
    /// `csn`, so a reader holding the snapshot can fetch the bitmap
    /// visible at `csn` (via `IBitmapStore::readBitmap(part, csn)`)
    /// without re-deriving the csn or routing back through
    /// `PartitionTxnController` (which has no read-side bitmap-fetch
    /// method by design — HARD CONSTRAINT). Populated by
    /// `PartitionTxnController::takeQuerySnapshot`; the pin (held by
    /// `QuerySnapshot`) outlives the closure. Returns a NON-NULL bitmap
    /// always — empty (not null) on miss. The returned `readBitmap` bitmap
    /// is shared const; read-side consumers treat it read-only.
    std::function<ConstRoaringBitmapPtr(const PartName & part)> bitmap_at;
};

/// Cross-thread / cross-pipeline-fragment safe handle to an immutable
/// snapshot of type `T`. All holders share an `IPinRegistry::PinHandle`
/// via `shared_ptr`; the pin releases when the last `Pinned` instance is
/// destroyed.
///
/// Move-only construction; `.share()` returns an extra refcount-bearing
/// copy.
template <typename T>
class Pinned
{
public:
    Pinned() = default;
    Pinned(std::shared_ptr<const T> snapshot, std::shared_ptr<IPinRegistry::PinHandle> pin)
        : snapshot_(std::move(snapshot)), pin_(std::move(pin)) {}

    Pinned(Pinned &&) = default;
    Pinned & operator=(Pinned &&) = default;

    Pinned(const Pinned &) = delete;
    Pinned & operator=(const Pinned &) = delete;

    /// Manufacture an extra-refcount copy. Multiple `.share()`s of the same
    /// `Pinned` all keep the underlying pin alive until the last drops.
    Pinned share() const { return Pinned(snapshot_, pin_); }

    const T & operator*() const noexcept { return *snapshot_; }
    const T * operator->() const noexcept { return snapshot_.get(); }
    explicit operator bool() const noexcept { return static_cast<bool>(snapshot_); }

    CSN pinnedCsn() const noexcept
    {
        return pin_ ? pin_->pinnedCsn() : INVALID_CSN;
    }

private:
    std::shared_ptr<const T> snapshot_;
    std::shared_ptr<IPinRegistry::PinHandle> pin_;
};

/// PartitionView pin held by a read scan for the query's lifetime.
using QuerySnapshot = Pinned<PartitionView>;

}
