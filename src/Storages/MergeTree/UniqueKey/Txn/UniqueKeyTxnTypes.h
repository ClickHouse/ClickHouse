#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{
    class DeleteBitmap;
}

namespace DB::UniqueKeyTxn
{

/// Owned, mutable per-part delete bitmap. WRITER-side shape: the delta a
/// commit contributes (`TouchedPartKills::new_kills`) and the bitmaps the
/// DELETE row finder produces.
using RoaringBitmapPtr = std::shared_ptr<DeleteBitmap>;

/// READER-side shape — `IBitmapStore::readBitmap`'s chosen bitmap and the
/// cumulative payload `commit` publishes (read-only once built; shared across
/// readers, copy-mutated by writers). See `IBitmapStore.h`.
using ConstRoaringBitmapPtr = std::shared_ptr<const DeleteBitmap>;

/// Commit Sequence Number. Per-partition monotone identifier; 0 means "no
/// commit yet on this partition" (recovery starts from this floor).
using CSN = UInt64;

constexpr CSN INVALID_CSN = 0;
constexpr CSN MAX_CSN = static_cast<CSN>(-1);

/// Part name (e.g. `all_1_1_0`). Stable identifier of a MergeTree part within
/// its partition; the manifest's bitmap targets are part names.
using PartName = String;

}
