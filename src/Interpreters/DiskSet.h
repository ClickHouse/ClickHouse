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

    virtual size_t getTotalRowCount() const = 0;

    /// Returns the memory that the set keeps in memory. Its keys are on disk.
    virtual size_t getTotalByteCount() const = 0;
};

}
