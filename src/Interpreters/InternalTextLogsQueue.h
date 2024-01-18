#pragma once
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/OvercommitTracker.h>
#include <Common/re2.h>
#include <Core/Block.h>

namespace DB
{

class InternalTextLogsQueue : public ConcurrentBoundedQueue<MutableColumns>
{
public:
    /// You should not push logs in the queue if their priority greater max_priority
    int max_priority;

    InternalTextLogsQueue();

    bool isNeeded(int priority, const String & source) const;

    static Block getSampleBlock();
    static MutableColumns getSampleColumns();

    /// Is used to pass block from remote server to the client
    void pushBlock(Block && log_block);

    /// Converts priority from Poco::Message::Priority to a string
    static std::string_view getPriorityName(int priority);

    void setSourceRegexp(const String & regexp);
private:
    /// If not null, you should only push logs which are matched with this regexp
    std::unique_ptr<re2::RE2> source_regexp;
};

using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;

}


