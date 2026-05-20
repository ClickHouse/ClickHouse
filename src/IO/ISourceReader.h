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

    /// Open a seekable buffer for reads from the object.
    /// The buffer is opened in external-buffer mode where supported: ReaderExecutor
    /// always provides the read destination via set() before each next() call,
    /// so the source reader should never allocate its own buffer memory when it
    /// can avoid it. For source readers whose underlying API doesn't expose the
    /// external-buffer flag (e.g. IBackup::readFile), the executor's set() still
    /// overrides the buffer pointer at read time — the cost is a wasted internal
    /// allocation, not a correctness issue.
    virtual std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) = 0;

    virtual String name() const = 0;
};

}
