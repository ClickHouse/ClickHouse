#include <Dictionaries/CacheDictionaryUpdateQueue.h>

#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

namespace CurrentMetrics
{
    extern const Metric CacheDictionaryThreads;
    extern const Metric CacheDictionaryThreadsActive;
    extern const Metric CacheDictionaryThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

template class CacheDictionaryUpdateUnit<DictionaryKeyType::Simple>;
template class CacheDictionaryUpdateUnit<DictionaryKeyType::Complex>;

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::CacheDictionaryUpdateQueue(
    String dictionary_name_for_logs_,
    CacheDictionaryUpdateQueueConfiguration configuration_,
    UpdateFunction && update_func_)
    : dictionary_name_for_logs(std::move(dictionary_name_for_logs_))
    , configuration(configuration_)
    , update_func(std::move(update_func_))
    , update_queue(configuration.max_update_queue_size)
    , update_pool(CurrentMetrics::CacheDictionaryThreads, CurrentMetrics::CacheDictionaryThreadsActive, CurrentMetrics::CacheDictionaryThreadsScheduled, configuration.max_threads_for_updates)
{
    try
    {
        for (size_t i = 0; i < configuration.max_threads_for_updates; ++i)
            update_pool.scheduleOrThrowOnError([this] { updateThreadFunction(); });
    }
    catch (...)
    {
        stopAndWait();
        throw;
    }
}

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::~CacheDictionaryUpdateQueue()
{
    if (update_queue.isFinished())
        return;

    try {
        stopAndWait();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// TODO: Write log
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::tryPushToUpdateQueueOrThrow(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr)
{
    if (update_queue.isFinished())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CacheDictionaryUpdateQueue finished");

    if (!update_queue.tryPush(update_unit_ptr, configuration.update_queue_push_timeout_milliseconds))
        throw DB::Exception(ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
            "Cannot push to internal update queue in dictionary {}. "
            "Timelimit of {} ms. exceeded. Current queue size is {}",
            dictionary_name_for_logs,
            std::to_string(configuration.update_queue_push_timeout_milliseconds),
            std::to_string(update_queue.size()));
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::waitForCurrentUpdateFinish(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr) const
{
    if (update_queue.isFinished())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CacheDictionaryUpdateQueue finished");

    std::unique_lock<std::mutex> update_lock(update_unit_ptr->update_mutex);

    bool result = update_unit_ptr->is_update_finished.wait_for(
        update_lock,
        std::chrono::milliseconds(configuration.query_wait_timeout_milliseconds),
        [&]
        {
            return update_unit_ptr->is_done || update_unit_ptr->current_exception;
        });

    if (!result)
    {
        throw DB::Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Dictionary {} source seems unavailable, because {} ms timeout exceeded.",
            dictionary_name_for_logs,
            toString(configuration.query_wait_timeout_milliseconds));
    }

    if (update_unit_ptr->current_exception)
    {
        // Don't just rethrow it, because sharing the same exception object
        // between multiple threads can lead to weird effects if they decide to
        // modify it, for example, by adding some error context.
        try
        {
            std::rethrow_exception(update_unit_ptr->current_exception);
        }
        catch (...)
        {
            throw DB::Exception(
                ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
                "Update failed for dictionary '{}': {}",
                dictionary_name_for_logs,
                getCurrentExceptionMessage(true /*with stack trace*/, true /*check embedded stack trace*/));
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::stopAndWait()
{
    if (update_queue.isFinished())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CacheDictionaryUpdateQueue finished");

    update_queue.clearAndFinish();
    update_pool.wait();
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::updateThreadFunction()
{
    setThreadName(ThreadName::CACHE_DICTIONARY_UPDATE_QUEUE);

    /// Each update costs one source query and one exclusive lock of the storage. When all other update threads are
    /// busy and recent batches had repeated keys, a thread takes all pending units and updates them together, so
    /// each repeated key is requested and inserted once. Otherwise units are updated one by one, in parallel.
    /// While batching is disabled because of low overlap, every probe_period updates a batch is tried again.
    /// The key limit sums the distinct keys of each unit, so a key requested by several units is counted several times.
    /// It is an upper bound of the keys of one source request, but the last unit taken and a single large unit can go above it.
    static constexpr size_t probe_period = 32;
    static constexpr size_t max_units_in_batch = 1024;
    static constexpr size_t max_keys_in_batch = 8192;

    /// Reserved once, so collecting a batch does not allocate: an exception there would lose the units taken from the queue.
    VectorWithMemoryTracking<CacheDictionaryUpdateUnitPtr<dictionary_key_type>> batch;
    batch.reserve(max_units_in_batch);

    while (!update_queue.isFinished())
    {
        batch.clear();

        CacheDictionaryUpdateUnitPtr<dictionary_key_type> unit_to_update;
        if (!update_queue.pop(unit_to_update))
            break;

        const size_t busy_threads = busy_update_threads.fetch_add(1) + 1;

        size_t keys_in_batch = unit_to_update->distinct_keys_to_update_size;
        batch.push_back(std::move(unit_to_update));

        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
        {
            const bool no_idle_threads = busy_threads >= configuration.max_threads_for_updates;
            const bool keys_repeat = repeated_keys_share.load(std::memory_order_relaxed) >= min_repeated_keys_share
                || updates_count.fetch_add(1, std::memory_order_relaxed) % probe_period == 0;

            while (no_idle_threads && keys_repeat && batch.size() < max_units_in_batch && keys_in_batch < max_keys_in_batch)
            {
                CacheDictionaryUpdateUnitPtr<dictionary_key_type> next_unit;
                if (!update_queue.tryPop(next_unit))
                    break;

                keys_in_batch += next_unit->distinct_keys_to_update_size;
                batch.push_back(std::move(next_unit));
            }
        }

        std::exception_ptr exception;
        try
        {
            auto update_keys = update_func(batch);

            if (batch.size() > 1 && update_keys.unit_keys > 0)
            {
                double share = 1.0 - static_cast<double>(update_keys.batch_keys) / static_cast<double>(update_keys.unit_keys);
                double previous = repeated_keys_share.load(std::memory_order_relaxed);
                repeated_keys_share.store(0.8 * previous + 0.2 * share, std::memory_order_relaxed);
            }
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        for (auto & unit : batch)
        {
            {
                std::lock_guard lock(unit->update_mutex);
                if (exception)
                    unit->current_exception = exception;
                else
                    unit->is_done = true;
            }

            unit->is_update_finished.notify_all();
        }

        busy_update_threads.fetch_sub(1);
    }
}

template class CacheDictionaryUpdateQueue<DictionaryKeyType::Simple>;
template class CacheDictionaryUpdateQueue<DictionaryKeyType::Complex>;

}
