#include <Dictionaries/CacheDictionaryUpdateQueue.h>

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
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

    auto is_update_finished = [&]
    {
        return update_unit_ptr->is_done || update_unit_ptr->current_exception;
    };

    /// Wait in slices rather than for the whole query_wait_timeout_milliseconds at once, so that
    /// cancellation of this query is observed instead of being deferred until the timeout expires.
    static constexpr auto wait_slice = std::chrono::milliseconds(100);
    const auto deadline
        = std::chrono::steady_clock::now() + std::chrono::milliseconds(configuration.query_wait_timeout_milliseconds);

    bool result = is_update_finished();

    while (!result)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            break;

        CurrentThread::checkIfNotCancelled();

        result = update_unit_ptr->is_update_finished.wait_for(
            update_lock,
            std::min(wait_slice, std::chrono::ceil<std::chrono::milliseconds>(deadline - now)),
            is_update_finished);
    }

    if (!result)
    {
        /// The wait may have expired in the same slice in which the query was cancelled; report the
        /// cancellation rather than a source timeout.
        CurrentThread::checkIfNotCancelled();

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

    while (!update_queue.isFinished())
    {
        CacheDictionaryUpdateUnitPtr<dictionary_key_type> unit_to_update;
        if (!update_queue.pop(unit_to_update))
            break;

        try
        {
            /// Update
            update_func(unit_to_update);

            {
                /// Notify thread about finished updating the bunch of ids
                /// where their own ids were included.
                std::lock_guard lock(unit_to_update->update_mutex);
                unit_to_update->is_done = true;
            }

            unit_to_update->is_update_finished.notify_all();
        }
        catch (...)
        {
            {
                std::lock_guard lock(unit_to_update->update_mutex);
                unit_to_update->current_exception = std::current_exception(); // NOLINT(bugprone-throw-keyword-missing)
            }

            unit_to_update->is_update_finished.notify_all();
        }
    }
}

template class CacheDictionaryUpdateQueue<DictionaryKeyType::Simple>;
template class CacheDictionaryUpdateQueue<DictionaryKeyType::Complex>;

}
