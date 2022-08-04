#pragma once

#include <Common/ThreadStatus.h>

namespace DB
{

class TaskObserverMetrics : public boost::noncopyable
{
public:
    TaskObserverMetrics() : thread_group(std::make_shared<ThreadGroupStatus>()) { }
    ~TaskObserverMetrics() { }

    void doResume()
    {
        CurrentThread::attachTo(thread_group);
    }

    void doSuspend()
    {
        CurrentThread::detachQueryIfNotDetached();
    }


private:
    ThreadGroupStatusPtr thread_group;
};

}
