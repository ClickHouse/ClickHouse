#pragma once
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/OvercommitTracker.h>
#include <Core/Block.h>


namespace DB
{

class InternalTextLogsQueue : public ConcurrentBoundedQueue<MutableColumns>
{
public:
    /// You should not push logs in the queue if their priority greater max_priority
    int max_priority;

    InternalTextLogsQueue();

    static Block getSampleBlock();
    static MutableColumns getSampleColumns();

    template <typename... Args>
    bool push(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::push(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool emplace(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::emplace(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool pop(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::pop(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool tryPush(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::tryPush(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool tryEmplace(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::tryEmplace(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool tryPop(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::tryPop(std::forward<Args>(args)...);
    }

    template <typename... Args>
    void clear(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::clear(std::forward<Args>(args)...);
    }

    template <typename... Args>
    void clearAndFinish(Args &&... args)
    {
        OvercommitTrackerBlockerInThread blocker;
        return ConcurrentBoundedQueue::clearAndFinish(std::forward<Args>(args)...);
    }

    /// Is used to pass block from remote server to the client
    void pushBlock(Block && log_block);

    /// Converts priority from Poco::Message::Priority to a string
    static const char * getPriorityName(int priority);
};

using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;

}


