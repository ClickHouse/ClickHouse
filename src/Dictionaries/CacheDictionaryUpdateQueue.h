#pragma once

#include <atomic>
#include <mutex>
#include <functional>

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn_fwd.h>
#include <Dictionaries/ICacheDictionaryStorage.h>

namespace CurrentMetrics
{
    extern const Metric CacheDictionaryUpdateQueueBatches;
    extern const Metric CacheDictionaryUpdateQueueKeys;
}

namespace DB
{

/** This class is passed between update queue and update queue client during update.

    For simple keys we pass simple keys.

    For complex keys we pass complex keys columns and requested rows to update.

    During update cache dictionary should fill requested_keys_to_fetched_columns_during_update_index and
    fetched_columns_during_update.

    For complex key to extend lifetime of key complex key arena should be used.
*/
template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryUpdateUnit
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, std::string_view>;

    /// Constructor for complex keys update request
    explicit CacheDictionaryUpdateUnit(
        const Columns & key_columns_,
        const PaddedPODArray<KeyState> & key_index_to_state_from_storage_,
        const DictionaryStorageFetchRequest & request_,
        size_t keys_to_update_size_,
        size_t distinct_keys_to_update_size_)
        : key_columns(key_columns_)
        , key_index_to_state(key_index_to_state_from_storage_.begin(), key_index_to_state_from_storage_.end())
        , request(request_)
        , keys_to_update_size(keys_to_update_size_)
        , distinct_keys_to_update_size(distinct_keys_to_update_size_)
        , alive_keys(CurrentMetrics::CacheDictionaryUpdateQueueKeys, keys_to_update_size)
    {}

    CacheDictionaryUpdateUnit()
        : keys_to_update_size(0)
        , distinct_keys_to_update_size(0)
        , alive_keys(CurrentMetrics::CacheDictionaryUpdateQueueKeys, 0)
    {}

    const Columns key_columns;
    const PaddedPODArray<KeyState> key_index_to_state;
    const DictionaryStorageFetchRequest request;
    /// Rows of the block whose key is expired or not found.
    const size_t keys_to_update_size;
    const size_t distinct_keys_to_update_size;

    /// Shared by all units of one update batch, so they are read-only after the update.
    std::shared_ptr<const HashMap<KeyType, size_t>> requested_keys_to_fetched_columns_during_update_index;
    Columns fetched_columns_during_update;

    /// Complex keys are serialized in this arena
    DictionaryKeysArenaHolder<dictionary_key_type> complex_keys_arena_holder;

private:
    template <DictionaryKeyType>
    friend class CacheDictionaryUpdateQueue;

    mutable std::mutex update_mutex;
    mutable std::condition_variable is_update_finished;

    bool is_done{false};
    std::exception_ptr current_exception{nullptr}; /// NOLINT

    /// While UpdateUnit is alive, it is accounted in update_queue size.
    CurrentMetrics::Increment alive_batch{CurrentMetrics::CacheDictionaryUpdateQueueBatches};
    CurrentMetrics::Increment alive_keys;
};

template <DictionaryKeyType dictionary_key_type>
using CacheDictionaryUpdateUnitPtr = std::shared_ptr<CacheDictionaryUpdateUnit<dictionary_key_type>>;

extern template class CacheDictionaryUpdateUnit<DictionaryKeyType::Simple>;
extern template class CacheDictionaryUpdateUnit<DictionaryKeyType::Complex>;

struct CacheDictionaryUpdateQueueConfiguration
{
    /// Size of update queue
    const size_t max_update_queue_size;
    /// Size in thead pool of update queue
    const size_t max_threads_for_updates;
    /// Timeout for trying to push update unit into queue
    const size_t update_queue_push_timeout_milliseconds;
    /// Timeout during sync waititing of update unit
    const size_t query_wait_timeout_milliseconds;
};

/** Responsibility of this class is to provide asynchronous and synchronous update support for CacheDictionary

    It is responsibility of CacheDictionary to perform update with UpdateUnit using UpdateFunction.
*/
/// Keys that one update requested from the source.
struct CacheDictionaryUpdateKeys
{
    /// Sum over the units of the update of the distinct keys of each unit.
    size_t unit_keys = 0;
    /// Distinct keys of all units. Less than unit_keys when different units request the same keys.
    size_t batch_keys = 0;
};

template <DictionaryKeyType dictionary_key_type>
class CacheDictionaryUpdateQueue
{
public:
    /// Client of update queue must provide this function in constructor and perform update using update unit.
    /// Gets one unit for complex keys, and one or more units for simple keys.
    using UpdateFunction
        = std::function<CacheDictionaryUpdateKeys(VectorWithMemoryTracking<CacheDictionaryUpdateUnitPtr<dictionary_key_type>> &)>;

    CacheDictionaryUpdateQueue(
        String dictionary_name_for_logs_,
        CacheDictionaryUpdateQueueConfiguration configuration_,
        UpdateFunction && update_func_);

    ~CacheDictionaryUpdateQueue();

    /// Get configuration that was passed to constructor
    const CacheDictionaryUpdateQueueConfiguration & getConfiguration() const { return configuration; }

    /// Is queue finished
    bool isFinished() const { return update_queue.isFinished(); }

    /// Synchronous wait for update queue to stop
    void stopAndWait();

    /** Try to add update unit into queue.

        If queue is full and oush cannot be performed in update_queue_push_timeout_milliseconds from configuration
        an exception will be thrown.

        If queue already finished an exception will be thrown.
    */
    void tryPushToUpdateQueueOrThrow(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr);

    /** Try to synchronously wait for update completion.

        If exception was passed from update function during update it will be rethrowed.

        If update will not be finished in query_wait_timeout_milliseconds from configuration
        an exception will be thrown.

        If queue already finished an exception will be thrown.
    */
    void waitForCurrentUpdateFinish(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr) const;

private:
    void updateThreadFunction();

    using UpdateQueue = ConcurrentBoundedQueue<CacheDictionaryUpdateUnitPtr<dictionary_key_type>>;

    String dictionary_name_for_logs;

    CacheDictionaryUpdateQueueConfiguration configuration;
    UpdateFunction update_func;

    UpdateQueue update_queue;
    ThreadPool update_pool;

    /// Number of update threads that are updating a batch (the others wait for a unit in the queue).
    std::atomic<size_t> busy_update_threads{0};

    /// Moving average of the share of keys of recent batches that more than one unit requested.
    /// Keys that repeat inside one unit are not counted, because one unit requests them once.
    /// Approximate: concurrent updates of the average can overwrite each other.
    /// Without repeated keys a batch still saves source requests, but it serializes fetches that the threads can do
    /// in parallel, and this was slower in the cold-key benchmark of the PR that added batching.
    /// It starts at the threshold, so the first batch is tried and one batch without overlap disables batching.
    static constexpr double min_repeated_keys_share = 0.1;
    std::atomic<double> repeated_keys_share{min_repeated_keys_share};
    std::atomic<size_t> updates_count{0};
};

extern template class CacheDictionaryUpdateQueue<DictionaryKeyType::Simple>;
extern template class CacheDictionaryUpdateQueue<DictionaryKeyType::Complex>;

}
