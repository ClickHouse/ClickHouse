#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class ReadBufferFromFileBase;

/// Opens a seekable buffer for reading from a storage object.
class ISourceReader
{
public:
    virtual ~ISourceReader() = default;

    /// Open a seekable buffer for streaming reads from the object.
    /// When use_external_buffer=true, the caller provides buffer memory via set()
    /// before each next() call — enables zero-copy reads into Rope buffers.
    virtual std::unique_ptr<ReadBufferFromFileBase> open(
        const StoredObject & object,
        bool use_external_buffer) = 0;

    virtual String name() const = 0;
};

}
