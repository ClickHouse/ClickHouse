#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{
    class DeleteBitmap;
}

namespace DB::UniqueKeyTxn
{

/// Owned, mutable per-part delete bitmap (writer side).
using DeleteBitmapPtr = std::shared_ptr<DeleteBitmap>;
/// Shared read-only bitmap (reader side; copy-mutated by writers).
using ConstDeleteBitmapPtr = std::shared_ptr<const DeleteBitmap>;

/// Commit Sequence Number. Per-partition monotone identifier; 0 means "no
/// commit yet on this partition" (recovery starts from this floor).
using CSN = UInt64;

constexpr CSN INVALID_CSN = 0;
constexpr CSN MAX_CSN = static_cast<CSN>(-1);

/// Part name (e.g. `all_1_1_0`). Stable identifier of a MergeTree part within
/// its partition; the manifest's bitmap targets are part names.
using PartName = String;

}
