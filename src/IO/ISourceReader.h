#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class ReadBufferFromFileBase;

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

    /// Open a seekable buffer for streaming reads from the object.
    /// Used by ReaderExecutor for live buffer optimization (keep connection open).
    /// Returns nullptr if not supported.
    virtual std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & /* object */)
    {
        return nullptr;
    }

    virtual String name() const = 0;
};

}
