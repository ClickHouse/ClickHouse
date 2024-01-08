#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>

#include <Common/EventFD.h>

#include <Processors/Chunk.h>

namespace DB
{

class StreamSubscription
{
public:
    void push(Chunk chunk);
    std::list<Chunk> extractAll();

    // returns event_fd's native handle for unix systems
    // otherwise returns nullopt
    std::optional<int> fd() const;

    // disables subscription
    void disable();

private:
    // data
    std::mutex mutex;
    std::list<Chunk> ready_chunks;

    // synchronization
    std::atomic<bool> is_disabled{false};

#if defined(OS_LINUX)
    EventFD new_chunks_event;
#else
    std::condition_variable empty_chunks;
#endif
};

}
