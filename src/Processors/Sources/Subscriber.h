#pragma once

#include <mutex>
#include <condition_variable>
#include <queue>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct Subscriber
{
    Subscriber(BlocksPtr blocks_): blocks(blocks_) {}
    std::mutex mutex;
    std::condition_variable condition;
    BlocksPtr blocks;
};

}
