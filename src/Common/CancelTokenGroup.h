#pragma once

#include <Common/CancelToken.h>
#include <Common/LockMemoryExceptionInThread.h>

#include <mutex>
#include <unordered_map>
#include <utility>


namespace DB
{

// Collection of `CancelTokens` of a thread group responsible for a specific cancellable query or task.
// Note that adding a thread into multiple groups simultaneously is considered an undefined behaviour.
class CancelTokenGroup
{
public:
    // Add current thread to this group.
    void enterGroup()
    {
        CancelToken & token = CancelToken::local();
        std::unique_lock lock(mutex);
        if (tokens[&token]++ == 0) // if new thread has been inserted
        {
            token.reset(); // This ensures we never bring a cancellation signal from different query into this group
            if (canceled) // Ensure we cannot add not cancelled thread into cancelled group
                canceled(token);
        }
    }

    // Remove current thread from this group.
    void exitGroup()
    {
        CancelToken & token = CancelToken::local();
        std::unique_lock lock(mutex);
        chassert(tokens[&token] > 0);
        if (--tokens[&token] == 0) // if thread should be removed from group
        {
            tokens.erase(&token);
            token.disable();
        }
    }

    // Cancel group and signal all current threads.
    // Note that `args` are saved for later to cancel every new thread.
    template <typename ... Args>
    void cancelGroup(Args && ... args)
    {
        std::unique_lock lock(mutex);
        if (canceled)
            return; // repeated cancel signals are ignored

        {
            // We do not want this tiny allocation to throw
            LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
            canceled = [=] (CancelToken & token)
            {
                // Note that if thread signals itself, this would not throw an exception
                // Throwing is postponed until `CancelToken::raise()` is called at some cancellation point
                CancelToken::signal(token.thread_id, args...);
            };
        }
        for (auto & [token, _] : tokens)
            canceled(*token);
    }

private:
    std::mutex mutex;
    std::function<void(CancelToken &)> canceled;
    std::unordered_map<CancelToken *, int> tokens;
};

}
