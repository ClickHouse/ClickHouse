#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class ReadBufferFromFileBase;

/// Opens a seekable buffer for reading from a storage object.
class IFileBasedSourceReader
{
public:
    virtual ~IFileBasedSourceReader() = default;

    /// Open a seekable buffer over the object, positioned at its start.
    virtual std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) = 0;

    virtual String name() const = 0;
};

}
