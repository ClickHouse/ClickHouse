#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <memory>

namespace DB
{

class IObjectStorageIterator
{
public:
    virtual void next() = 0;
    virtual void nextBatch() = 0;
    virtual bool isValid() = 0;
    virtual RelativePathWithMetadata current() = 0;
    virtual RelativePathsWithMetadata currentBatch() = 0;
    virtual std::optional<RelativePathsWithMetadata> getCurrrentBatchAndScheduleNext() = 0;
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

    void nextBatch() override
    {
        batch_iterator = batch.end();
    }

    bool isValid() override
    {
        return batch_iterator != batch.end();
    }

    RelativePathWithMetadata current() override;

    RelativePathsWithMetadata currentBatch() override
    {
        return batch;
    }

    std::optional<RelativePathsWithMetadata> getCurrrentBatchAndScheduleNext() override
    {
        return std::nullopt;
    }

    size_t getAccumulatedSize() const override
    {
        return batch.size();
    }
private:
    RelativePathsWithMetadata batch;
    RelativePathsWithMetadata::iterator batch_iterator;
};

}
