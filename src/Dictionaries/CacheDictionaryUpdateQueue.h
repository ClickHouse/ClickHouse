#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>
#include <functional>

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Dictionaries/ICacheDictionaryStorage.h>

namespace CurrentMetrics
{
    extern const Metric CacheDictionaryUpdateQueueBatches;
    extern const Metric CacheDictionaryUpdateQueueKeys;
}

namespace DB
{

template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryUpdateUnit
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionaryUpdateUnit");

    explicit CacheDictionaryUpdateUnit(
        PaddedPODArray<UInt64> && requested_simple_keys_,
        const DictionaryStorageFetchRequest & request_)
        : requested_simple_keys(std::move(requested_simple_keys_))
        , request(request_)
        , alive_keys(CurrentMetrics::CacheDictionaryUpdateQueueKeys, requested_simple_keys_.size())
    {
        fetched_columns_during_update = request.makeAttributesResultColumns();
    }

    explicit CacheDictionaryUpdateUnit(
        const Columns & requested_complex_key_columns_,
        const std::vector<size_t> && requested_complex_key_rows_,
        const DictionaryStorageFetchRequest & request_)
        : requested_complex_key_columns(requested_complex_key_columns_)
        , requested_complex_key_rows(std::move(requested_complex_key_rows_))
        , request(request_)
        , complex_key_arena(std::make_shared<Arena>())
        , alive_keys(CurrentMetrics::CacheDictionaryUpdateQueueKeys, requested_complex_key_rows.size())
    {
        fetched_columns_during_update = request.makeAttributesResultColumns();
    }

    explicit CacheDictionaryUpdateUnit()
        : alive_keys(CurrentMetrics::CacheDictionaryUpdateQueueKeys, 0)
    {}

    const PaddedPODArray<UInt64> requested_simple_keys;

    const Columns requested_complex_key_columns;
    const std::vector<size_t> requested_complex_key_rows;

    const DictionaryStorageFetchRequest request;

    /// This should be filled by the client during update
    HashMap<KeyType, size_t> requested_keys_to_fetched_columns_during_update_index;
    MutableColumns fetched_columns_during_update;
    const std::shared_ptr<Arena> complex_key_arena;

private:
    template <DictionaryKeyType>
    friend class CacheDictionaryUpdateQueue;

    std::atomic<bool> is_done{false};
    std::exception_ptr current_exception{nullptr};

    /// While UpdateUnit is alive, it is accounted in update_queue size.
    CurrentMetrics::Increment alive_batch{CurrentMetrics::CacheDictionaryUpdateQueueBatches};
    CurrentMetrics::Increment alive_keys;
};

template <DictionaryKeyType dictionary_key_type>
using CacheDictionaryUpdateUnitPtr = std::shared_ptr<CacheDictionaryUpdateUnit<dictionary_key_type>>;

extern template class CacheDictionaryUpdateUnit<DictionaryKeyType::simple>;
extern template class CacheDictionaryUpdateUnit<DictionaryKeyType::complex>;

struct CacheDictionaryUpdateQueueConfiguration
{
    const size_t max_update_queue_size;
    const size_t update_queue_push_timeout_milliseconds;
    const size_t query_wait_timeout_milliseconds;
    const size_t max_threads_for_updates;
};

template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryUpdateQueue
{
public:

    using UpdateFunction = std::function<void (CacheDictionaryUpdateUnitPtr<dictionary_key_type> &)>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by CacheDictionaryUpdateQueue");

    CacheDictionaryUpdateQueue(
        String dictionary_name_for_logs_,
        CacheDictionaryUpdateQueueConfiguration configuration_,
        UpdateFunction && update_func_);

    ~CacheDictionaryUpdateQueue();

    const CacheDictionaryUpdateQueueConfiguration & getConfiguration() const
    {
        return configuration;
    }

    bool isFinished() const
    {
        return finished;
    }

    void stopAndWait();

    void tryPushToUpdateQueueOrThrow(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr);

    void waitForCurrentUpdateFinish(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr) const;

private:
    void updateThreadFunction();

    using UpdateQueue = ConcurrentBoundedQueue<CacheDictionaryUpdateUnitPtr<dictionary_key_type>>;

    String dictionary_name_for_logs;

    CacheDictionaryUpdateQueueConfiguration configuration;
    UpdateFunction update_func;

    UpdateQueue update_queue;
    ThreadPool update_pool;

    mutable std::mutex update_mutex;
    mutable std::condition_variable is_update_finished;

    std::atomic<bool> finished{false};
};

extern template class CacheDictionaryUpdateQueue<DictionaryKeyType::simple>;
extern template class CacheDictionaryUpdateQueue<DictionaryKeyType::complex>;

}
