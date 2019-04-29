#pragma once

#include <Common/ThreadPool.h>

namespace DB
{

class ThreadGroupStatus;
using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;

/// Attach/detach thread from ThreadPool to specified thread_group.
class ThreadGroupThreadPoolCallbacks : public ThreadPoolCallbacks
{
public:
    explicit ThreadGroupThreadPoolCallbacks(ThreadGroupStatusPtr thread_group_);

    void onThreadStart() override;
    void onThreadFinish() override;

private:
    ThreadGroupStatusPtr thread_group;
};

}

