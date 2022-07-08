#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Block.h>


namespace DB
{

class InternalTextLogsQueue
{
private:
    ConcurrentBoundedQueue<MutableColumns> queue;

public:
    /// You should not push logs in the queue if their priority greater max_priority
    int max_priority;

    InternalTextLogsQueue();

    static Block getSampleBlock();
    static MutableColumns getSampleColumns();

    /// Is used to pass block from remote server to the client
    void pushBlock(Block && log_block);

    /// Converts priority from Poco::Message::Priority to a string
    static const char * getPriorityName(int priority);

    /// ConcurrentBoundedQueue interface.
    void emplace(MutableColumns && columns);
    bool tryPop(MutableColumns & columns);
};

using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;

}


