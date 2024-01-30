#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>

#include <Common/EventFD.h>

#include <Core/Block.h>

namespace DB
{

class StreamSubscription
{
public:
    void push(Block block);
    BlocksList extractAll();

    // returns event_fd's native handle for unix systems
    // otherwise returns nullopt
    std::optional<int> fd() const;

    // disables subscription
    void disable();

private:
    // data
    std::mutex mutex;
    BlocksList ready_blocks;

    // synchronization
    std::atomic<bool> is_disabled{false};

#if defined(OS_LINUX)
    EventFD new_blocks_event;
#else
    std::condition_variable empty_blocks;
#endif
};

}
