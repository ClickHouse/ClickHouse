#pragma once

#include <IO/ISourceReader.h>
#include <Disks/IDisk.h>
#include <Common/Logger.h>

namespace DB
{

class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

/// Reads from any IObjectStorage (S3, Azure, HDFS, etc.).
class ObjectStorageSourceReader : public ISourceReader
{
public:
    ObjectStorageSourceReader(ObjectStoragePtr storage, const ReadSettings & read_settings);

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object, bool use_external_buffer) override;

    String name() const override { return "ObjectStorageSourceReader"; }

private:
    ObjectStoragePtr storage;
    ReadSettings read_settings;
    LoggerPtr log = getLogger("ObjectStorageSourceReader");
};

}
