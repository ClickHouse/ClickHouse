#pragma once

#include <string>
#include <vector>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

class IMetadata
{
public:
    virtual void addObject(const std::string & path, size_t size) = 0;
    virtual void deserialize(ReadBuffer & buf) = 0;

    virtual void serialize(WriteBuffer & buf, bool sync) const = 0;

    virtual std::string getBlobsCommonPrefix() const = 0;
    virtual std::vector<BlobPathWithSize> getBlobs() const = 0;

    virtual bool isReadOnly() const = 0;
    virtual void setReadOnly() = 0;
    virtual void resetRefCount() = 0;
    virtual void incrementRefCount() = 0;
    virtual void decrementRefCount() = 0;

    virtual uint32_t getRefCount() const = 0;
    virtual uint64_t getTotalSizeBytes() const = 0;

    virtual ~IMetadata() = default;
};

using MetadataPtr = std::unique_ptr<IMetadata>;

}
