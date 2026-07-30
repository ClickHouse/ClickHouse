#pragma once

#include <IO/CoverageMap.h>
#include <IO/ICacheProvider.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

/// One resident range + its held read buffer.
struct HitEntry { ByteRange range; CacheReaderPtr reader; };
/// One miss CELL. `resolve` attaches the writer when the provider populates
/// (null on a read-only/bypass tier), ready for the plan to fill.
struct MissEntry { ByteRange range; CacheWriterPtr writer; };

/// One tier's held plan buffers, assembled by `observeSpan` from a tier's
/// ranged `lookAt`: hit readers and miss cells with their write handles.
/// Held by the plan across windows; destruction releases the pins and
/// finalizes the writers.
class CacheView
{
public:
    virtual ~CacheView() = default;

    const VectorWithMemoryTracking<HitEntry> & hits() const { return hit_entries; }
    const VectorWithMemoryTracking<MissEntry> & misses() const { return miss_entries; }

    bool allHit() const { return miss_entries.empty(); }
    bool allMiss() const { return hit_entries.empty(); }

    /// PRUNE step: drop the miss at `index` (a cell the plan will not fill in
    /// this tier - e.g. fully covered by a faster tier). Runs before the
    /// writer upgrade, so no write handle is ever opened for it.
    void dropMiss(size_t index) { miss_entries.erase(miss_entries.begin() + index); }

    /// Sorted, disjoint; hits + misses tile the walked range (clamped to EOF /
    /// object end). EACH MISS RANGE IS ONE CELL.
    VectorWithMemoryTracking<HitEntry> hit_entries;
    VectorWithMemoryTracking<MissEntry> miss_entries;
};
using CacheViewPtr = std::unique_ptr<CacheView>;

/// Assemble a span-probe-shaped read-only view (sorted hit entries with
/// readers + one-cell miss entries) over `span` - for callers that need one
/// whole small range at once: the encryption-header read and the buffer-API
/// tests.
CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span);


}
