#pragma once
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <memory>
#include <ext/shared_ptr_helper.h>
#include <mutex>


namespace Poco
{
    class Logger;
}


namespace DB
{

class QueryStatus;
class ThreadStatus;
struct ScopeCurrentThread;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


class ThreadStatus : public ext::shared_ptr_helper<ThreadStatus>
{
public:

    UInt32 poco_thread_number = 0;
    QueryStatus * parent_query = nullptr;
    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;
    int os_thread_id = -1;
    std::mutex mutex;

    void init(QueryStatus * parent_query_, ProfileEvents::Counters * parent_counters, MemoryTracker * parent_memory_tracker);
    void onStart();
    void onExit();

    /// Reset all references and metrics
    void reset();

    static void setCurrentThreadParentQuery(QueryStatus * parent_process);
    static void setCurrentThreadFromSibling(const ThreadStatusPtr & sibling_thread);
    ThreadStatusPtr getCurrent() const;

    ~ThreadStatus();

    friend struct ScopeCurrentThread;

//protected:
    ThreadStatus();

    bool initialized = false;
    bool thread_exited = false;
    Poco::Logger * log;

    struct Impl;
    std::shared_ptr<Impl> impl;
};

extern thread_local ThreadStatusPtr current_thread;

}
