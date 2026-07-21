#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <memory>

namespace DB
{

class IObjectStorageIterator
{
public:
    /// Moves iterator to the next element. If the iterator not isValid, the behavior is undefined.
    virtual void next() = 0;

    /// Check if the iterator is valid, which means the `current` method can be called.
    virtual bool isValid() = 0;

    /// Return the current element.
    virtual RelativePathWithMetadataPtr current() = 0;

    /// This will initiate prefetching the next batch in background, so it can be obtained faster when needed.
    virtual std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() = 0;

    /// Returns the number of elements in the batches that were fetched so far.
    virtual size_t getAccumulatedSize() const = 0;

    virtual ~IObjectStorageIterator() = default;

private:
    /// Skips all the remaining elements in the current batch (if any),
    /// and moves the iterator to the first element of the next batch,
    /// or, if there is no more batches, the iterator becomes invalid.
    /// If the iterator not isValid, the behavior is undefined.
    virtual void nextBatch() = 0;

    /// Return the current batch of elements.
    /// It is unspecified how batches are formed.
    /// But this method can be used for more efficient processing.
    virtual RelativePathsWithMetadata currentBatch() = 0;
};

using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

class ObjectStorageIteratorFromList : public IObjectStorageIterator
{
public:
    /// Everything is represented by just a single batch.
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
