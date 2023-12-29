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

    // cancels waiting for new data
    void cancel();

private:
    // data
    std::mutex mutex;
    std::list<Chunk> ready_chunks;

#if defined(OS_LINUX)
    EventFD new_chunks_event;
#else
    bool cancelled = false;
    std::condition_variable empty_chunks;
#endif
};

using StreamSubscriptionPtr = std::shared_ptr<StreamSubscription>;

}
