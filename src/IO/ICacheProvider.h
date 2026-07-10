#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>
#include <vector>

namespace DB
{

/// One cache tier the `ReaderExecutor` reads through, per window. The executor holds an
/// ordered chain (index 0 = fastest/top); at each window it probes the chain top-down,
/// serves from the first tier that holds the bytes, and on a miss reads from the source
/// and pushes the bytes up into every tier that missed.
///
/// Coordinates are OBJECT-LOCAL (the same coordinate `FileCache` keys on): a request is
/// `(object, offset within the object, size)`. The bytes cached are the RAW source bytes
/// (before any decryption); the executor decrypts what a tier serves, exactly as it
/// decrypts a fresh source read.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    virtual String name() const = 0;

    /// Granularity a miss fetch is aligned to: the executor rounds a miss range DOWN at its
    /// head and UP at its tail to this boundary (clamped to the object) before reading it
    /// from the source, so the region populated into this tier matches its block/segment
    /// boundaries and later reads of neighbouring offsets hit. `1` disables alignment.
    virtual size_t missAlignment() const { return 1; }

    /// Serve up to `size` bytes at object-local `offset` of `object` into `dst`. Returns the
    /// number of bytes served from the head of the range (a contiguous prefix); `0` is a
    /// miss. Never throws on a miss/short read - a lookup failure degrades to `0`.
    virtual size_t tryRead(const StoredObject & object, size_t offset, char * dst, size_t size) = 0;

    /// Populate this tier with `size` raw bytes at object-local `offset`. Best-effort: a
    /// reservation failure, a lost downloader race or a bypassed tier leaves the tier
    /// unpopulated. Never throws.
    virtual void write(const StoredObject & object, size_t offset, const char * data, size_t size) = 0;
};

using ICacheProviderPtr = std::shared_ptr<ICacheProvider>;

/// Ordered cache chain; front = fastest tier probed first, populated first on a miss. A tiny
/// setup-time vector (one entry per cache tier), so std::vector is fine here.
using CacheChain = std::vector<ICacheProviderPtr>; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

}
