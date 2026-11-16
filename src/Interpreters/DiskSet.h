#pragma once

#include <base/types.h>

#include <span>

namespace DB
{

class IColumn;

/// An immutable set of keys on disk, built by `DiskSetBuilder`. It answers membership lookups for batches of
/// keys, and concurrent lookups need no synchronization.
class DiskSet
{
public:
    virtual ~DiskSet() = default;

    /// Sets `found[i]` for each key of `keys`, a column of the key type the set was built with.
    virtual void containsBatch(const IColumn & keys, std::span<UInt8> found) const = 0;

    [[nodiscard]] virtual size_t getTotalRowCount() const = 0;

    /// Returns the bytes of the keys of the set at their full width, which its size limits apply to. The
    /// memory that lookups use, such as Bloom filters, does not count.
    [[nodiscard]] virtual size_t getTotalByteCount() const = 0;
};

/// Returns about the memory that a `DiskSet` of `keys` keys of type `Key`, an unsigned integer of 8 to 256
/// bits, keeps besides its cache: its directory and its Bloom filters, which take at most
/// `max_bytes_for_bloom_filter` bytes (see `DiskSetImpl::estimateMemory`).
template <typename Key>
size_t estimateDiskSetMemory(size_t keys, size_t max_bytes_for_bloom_filter);

}
