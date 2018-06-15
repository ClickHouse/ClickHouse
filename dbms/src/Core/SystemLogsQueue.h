#pragma once
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Block.h>


namespace DB
{

class SystemLogsQueue : public ConcurrentBoundedQueue<MutableColumns>
{
public:
    /// You should not push logs in the queue if their priority greater max_priority
    int max_priority;

    SystemLogsQueue();

    static Block getSampleBlock();
    static MutableColumns getSampleColumns();

    /// Converts priority from Poco::Message::Priority to a string
    static const char * getPriorityName(int priority);
};

using SystemLogsQueuePtr = std::shared_ptr<SystemLogsQueue>;

}



