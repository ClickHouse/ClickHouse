#include "CacheDictionaryUpdateQueue.h"

#include <Dictionaries/CacheDictionaryUpdateQueue.h>

#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

template class CacheDictionaryUpdateUnit<DictionaryKeyType::simple>;
template class CacheDictionaryUpdateUnit<DictionaryKeyType::complex>;

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::CacheDictionaryUpdateQueue(
    String dictionary_name_for_logs_,
    CacheDictionaryUpdateQueueConfiguration configuration_,
    UpdateFunction && update_func_)
    : dictionary_name_for_logs(std::move(dictionary_name_for_logs_))
    , configuration(configuration_)
    , update_func(std::move(update_func_))
    , update_queue(configuration.max_update_queue_size)
    , update_pool(configuration.max_threads_for_updates)
{
    for (size_t i = 0; i < configuration.max_threads_for_updates; ++i)
        update_pool.scheduleOrThrowOnError([this] { updateThreadFunction(); });
}

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::~CacheDictionaryUpdateQueue()
{
    try {
        if (!finished)
            stopAndWait();
    }
    catch (...)
    {
        /// TODO: Write log
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::tryPushToUpdateQueueOrThrow(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr)
{
    if (finished)
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
    if (finished)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "CacheDictionaryUpdateQueue finished");

    std::unique_lock<std::mutex> update_lock(update_mutex);

    bool result = is_update_finished.wait_for(
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
    finished = true;
    update_queue.clear();

    for (size_t i = 0; i < configuration.max_threads_for_updates; ++i)
    {
        auto empty_finishing_ptr = std::make_shared<CacheDictionaryUpdateUnit<dictionary_key_type>>();
        update_queue.push(empty_finishing_ptr);
    }

    update_pool.wait();
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::updateThreadFunction()
{
    setThreadName("UpdQueue");

    while (!finished)
    {
        CacheDictionaryUpdateUnitPtr<dictionary_key_type> unit_to_update;
        update_queue.pop(unit_to_update);

        if (finished)
            break;

        try
        {
            /// Update
            update_func(unit_to_update);

            /// Notify thread about finished updating the bunch of ids
            /// where their own ids were included.
            std::unique_lock<std::mutex> lock(update_mutex);

            unit_to_update->is_done = true;
            is_update_finished.notify_all();
        }
        catch (...)
        {
            std::unique_lock<std::mutex> lock(update_mutex);

            unit_to_update->current_exception = std::current_exception(); // NOLINT(bugprone-throw-keyword-missing)
            is_update_finished.notify_all();
        }
    }
}

template class CacheDictionaryUpdateQueue<DictionaryKeyType::simple>;
template class CacheDictionaryUpdateQueue<DictionaryKeyType::complex>;

}
