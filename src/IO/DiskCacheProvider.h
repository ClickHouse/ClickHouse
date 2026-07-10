#pragma once

#include <IO/ICacheProvider.h>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <Common/Logger.h>

namespace DB
{

/// FileCache-backed `ICacheProvider`: a foreground, per-window read-through over the filesystem
/// cache. `tryRead` serves a window only when the whole range is already downloaded (a cache-only
/// `getDownloadedContiguousOrEmpty` probe + pread); `write` populates the covering segments
/// synchronously (`getOrSet` -> become downloader -> reserve -> write -> complete), best-effort.
///
/// No prefetch, no held cross-window buffers, no downloader election: the big PR's concurrent
/// cache-write machinery (and the use-after-free it exposed on the prefetch path) is excluded by
/// construction. Safe to share across the concurrent read fan-out — the `FileCache` is internally
/// synchronized and every call opens its own short-lived holder / reader.
class DiskCacheProvider : public ICacheProvider
{
public:
    DiskCacheProvider(FileCachePtr cache_, FileCacheOriginInfo origin_, size_t boundary_alignment_);

    String name() const override { return "DiskCache"; }

    /// Align a miss fetch to the cache's boundary granularity, so the populated range maps onto
    /// whole cache segments (`getOrSet` aligns segment boundaries to the same value).
    size_t missAlignment() const override { return boundary_alignment; }

    size_t tryRead(const StoredObject & object, size_t offset, char * dst, size_t size) override;
    void write(const StoredObject & object, size_t offset, const char * data, size_t size) override;

private:
    FileCachePtr cache;
    FileCacheOriginInfo origin;
    size_t boundary_alignment;
    LoggerPtr log = getLogger("DiskCacheProvider");
};

}
