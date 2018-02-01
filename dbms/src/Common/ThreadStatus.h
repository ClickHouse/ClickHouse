#pragma once
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <memory>
#include <ext/shared_ptr_helper.h>

namespace Poco
{
    class Logger;
}


namespace DB
{

struct QueryStatus;
struct ThreadStatus;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


struct ThreadStatus : public ext::shared_ptr_helper<ThreadStatus>, public std::enable_shared_from_this<ThreadStatus>
{
    UInt32 poco_thread_number = 0;
    QueryStatus * parent_query = nullptr;

    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;

    void init(QueryStatus * parent_query_, ProfileEvents::Counters * parent_counters, MemoryTracker * parent_memory_tracker);
    void onStart();
    void onExit();

    /// Reset all references and metrics
    void reset();

    static void setCurrentThreadParentQuery(QueryStatus * parent_process);
    static void setCurrentThreadFromSibling(const ThreadStatusPtr & sibling_thread);

    ~ThreadStatus();

protected:
    ThreadStatus();

    bool initialized = false;
    Poco::Logger * log;

    struct Payload;
    std::shared_ptr<Payload> payload;
};


extern thread_local ThreadStatusPtr current_thread;

}
