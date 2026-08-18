#pragma once

#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <mutex>
#include <Common/CurrentMetrics.h>


namespace DB
{

class IObjectStorageIteratorAsync : public IObjectStorageIterator
{
public:
    IObjectStorageIteratorAsync(
        CurrentMetrics::Metric threads_metric,
        CurrentMetrics::Metric threads_active_metric,
        CurrentMetrics::Metric threads_scheduled_metric,
        const std::string & thread_name);

    ~IObjectStorageIteratorAsync() override;

    bool isValid() override;

    RelativePathWithMetadataPtr current() override;
    RelativePathsWithMetadata currentBatch() override;

    void next() override;
    void nextBatch() override;

    size_t getAccumulatedSize() const override;
    std::optional<RelativePathsWithMetadata> getCurrentBatchAndScheduleNext() override;

    void deactivate();

protected:
    /// This method fetches the next batch, and returns true if there are more batches after it.
    virtual bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) = 0;

    struct BatchAndHasNext
    {
        RelativePathsWithMetadata batch;
        bool has_next;
    };

    std::future<BatchAndHasNext> scheduleBatch();

    bool is_initialized{false};
    bool is_finished{false};
    bool has_next_batch{true};
    bool deactivated{false};

    mutable std::recursive_mutex mutex;
    ThreadPool list_objects_pool;
    ThreadPoolCallbackRunnerUnsafe<BatchAndHasNext> list_objects_scheduler;
    std::future<BatchAndHasNext> outcome_future;
    RelativePathsWithMetadata current_batch;
    RelativePathsWithMetadata::iterator current_batch_iterator;
    std::atomic<size_t> accumulated_size = 0;
};


}
