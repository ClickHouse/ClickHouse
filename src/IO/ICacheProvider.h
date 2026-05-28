#pragma once

#include <IO/Rope.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>
#include <vector>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct CacheLookupResult
{
    VectorWithMemoryTracking<ByteRange> hit_ranges;
    VectorWithMemoryTracking<ByteRange> miss_ranges;

    bool allHit() const { return miss_ranges.empty(); }
    bool allMiss() const { return hit_ranges.empty(); }
};

/// Handle returned by cache lookup. Pins results, provides get/put.
///
/// All `ByteRange` parameters and results are in FILE-LEVEL coordinates
/// (logical offsets inside the file the `ReadPipeline` is reading), so
/// the executor never has to translate. Caches whose internal key space
/// is per-object (`DiskCacheHandle` / FileCache) do their own
/// object_file_offset ↔ file-level translation internally.
///
/// RAII — destructor releases pins.
class ICacheHandle
{
public:
    virtual ~ICacheHandle() = default;

    /// Reports `hit_ranges` / `miss_ranges` in file-level coordinates at this
    /// cache's granularity.
    virtual CacheLookupResult status() const = 0;

    /// Read cached data as a rope slice. `range` must be within
    /// `hit_ranges`. The returned rope's nodes carry file-level
    /// `logical_offset`s.
    virtual Rope get(ByteRange range) = 0;

    /// Provide data for a miss range. `range` must be within
    /// `miss_ranges`; `data` must hold nodes in file-level coordinates.
    /// Returns the number of bytes that actually landed in the cache;
    /// can be less than `range.size` when we lost the downloader race
    /// on some segments, reservation failed (cache full), or the
    /// handle is in bypass mode (returns 0).
    ///
    /// LRU update ordering: implementations MUST defer the LRU-bump for
    /// every hit `get`-ed via this handle until destruction. The executor
    /// keeps the handle alive until after every `put` it intends to issue,
    /// so the bumps run AFTER the inserts — a hit that sits next to fresh
    /// inserts does not become "older" than them under LRU eviction. See
    /// `02944_dynamically_change_filesystem_cache_size` for the regression
    /// this ordering prevents.
    virtual size_t put(ByteRange range, Rope data) = 0;
};

/// Cache provider interface. `ReadPipeline` configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    /// Lookup a range of `object` inside the file.
    ///   - `object` is the per-object identity (`remote_path` / `local_path`).
    ///     Per-object caches (`DiskCacheProvider`) derive their cache key
    ///     and origin from it; file-level caches (`PageCacheProvider`)
    ///     ignore it.
    ///   - `object_file_offset` is the offset inside the logical file
    ///     where `object` starts. Used by per-object caches to translate
    ///     the file-level `range_in_file` to / from their object-local
    ///     internal key space.
    ///   - `range_in_file` is the request range in file-level
    ///     coordinates — `[object_file_offset,
    ///     object_file_offset + object.bytes_size)` is the slice that
    ///     belongs to this `object`.
    virtual std::unique_ptr<ICacheHandle> lookup(
        const StoredObject & object,
        size_t object_file_offset,
        ByteRange range_in_file) = 0;

    virtual String name() const = 0;
};

}
