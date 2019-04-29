#include <Interpreters/ThreadGroupThreadPoolCallbacks.h>
#include <Common/CurrentThread.h>

namespace DB
{

ThreadGroupThreadPoolCallbacks::ThreadGroupThreadPoolCallbacks(ThreadGroupStatusPtr thread_group_)
    : thread_group(std::move(thread_group_))
{
}

void ThreadGroupThreadPoolCallbacks::onThreadStart()
{
    if (thread_group)
        DB::CurrentThread::attachToIfDetached(thread_group);
}

void ThreadGroupThreadPoolCallbacks::onThreadFinish()
{
    if (thread_group)
        DB::CurrentThread::detachQueryIfNotDetached();
}

}
