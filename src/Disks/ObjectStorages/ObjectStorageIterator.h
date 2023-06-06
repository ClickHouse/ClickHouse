#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <memory>

namespace DB
{

class IObjectStorageIterator
{
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;
    virtual RelativePathWithMetadata current() const = 0;
    virtual size_t getAccumulatedSize() const = 0;

    virtual ~IObjectStorageIterator() = default;
};

using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

class ObjectStorageIteratorFromList : public IObjectStorageIterator
{
public:
    explicit ObjectStorageIteratorFromList(RelativePathsWithMetadata && batch_)
        : batch(std::move(batch_))
        , batch_iterator(batch.begin())
    {
    }

    void next() override
    {
        if (isValid())
            ++batch_iterator;
    }

    bool isValid() const override
    {
        return batch_iterator != batch.end();
    }

    RelativePathWithMetadata current() const override;

    size_t getAccumulatedSize() const override
    {
        return batch.size();
    }
private:
    RelativePathsWithMetadata batch;
    RelativePathsWithMetadata::iterator batch_iterator;
};

}
