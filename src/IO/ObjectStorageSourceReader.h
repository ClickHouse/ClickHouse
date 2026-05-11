#pragma once

#include <IO/ISourceReader.h>
#include <Disks/IDisk.h>
#include <Common/Logger.h>

namespace DB
{

class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

/// Reads from any IObjectStorage (S3, Azure, HDFS, etc.) using readBigAt
/// for stateless range reads, with seek+read fallback.
class ObjectStorageSourceReader : public ISourceReader
{
public:
    ObjectStorageSourceReader(ObjectStoragePtr storage, const ReadSettings & read_settings);

    size_t read(
        const StoredObject & object,
        size_t offset, size_t size,
        char * buffer) override;

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object, bool use_external_buffer = false) override;

    String name() const override { return "ObjectStorageSourceReader"; }

private:
    ObjectStoragePtr storage;
    ReadSettings read_settings;
    LoggerPtr log = getLogger("ObjectStorageSourceReader");
};

}
