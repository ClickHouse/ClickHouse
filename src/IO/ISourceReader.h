#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

namespace DB
{

/// Stateless range-read from a storage object.
class ISourceReader
{
public:
    virtual ~ISourceReader() = default;

    /// Read [offset, offset+size) from object into buffer.
    /// Returns actual bytes read (may be less at EOF).
    virtual size_t read(
        const StoredObject & object,
        size_t offset, size_t size,
        char * buffer) = 0;

    virtual String name() const = 0;
};

}
