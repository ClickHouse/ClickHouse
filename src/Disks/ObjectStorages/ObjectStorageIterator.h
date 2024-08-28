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
    virtual RelativePathWithMetadataPtr current() = 0;
    virtual RelativePathsWithMetadata currentBatch() = 0;
    virtual std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() = 0;
    virtual size_t getAccumulatedSize() const = 0;

    virtual ~IObjectStorageIterator() = default;
};

using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

class ObjectStorageIteratorFromList : public IObjectStorageIterator
{
public:
    explicit ObjectStorageIteratorFromList(RelativePathsWithMetadata && batch_)
        : batch(std::move(batch_))
        , batch_iterator(batch.begin()) {}

    void next() override
    {
        if (isValid())
            ++batch_iterator;
    }

    void nextBatch() override { batch_iterator = batch.end(); }

    bool isValid() override { return batch_iterator != batch.end(); }

    RelativePathWithMetadataPtr current() override;

    RelativePathsWithMetadata currentBatch() override { return batch; }

    std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() override
    {
        if (batch.empty())
            return {};

        auto current_batch = std::move(batch);
        batch = {};
        return current_batch;
    }

    size_t getAccumulatedSize() const override { return batch.size(); }

private:
    RelativePathsWithMetadata batch;
    RelativePathsWithMetadata::iterator batch_iterator;
};

}
