#pragma once

#include <Common/ThreadStatus.h>

namespace DB
{

class TaskObserverMetrics : public boost::noncopyable
{
public:
    TaskObserverMetrics() : thread_group(std::make_shared<ThreadGroupStatus>()) { }
    ~TaskObserverMetrics() { }

    bool doResume()
    {
        CurrentThread::attachTo(thread_group);
        return true;
    }

    bool doSuspend()
    {
        CurrentThread::detachQueryIfNotDetached();
        return true;
    }


private:
    ThreadGroupStatusPtr thread_group;
};

}
