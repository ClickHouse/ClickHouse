#include <Common/CurrentThread.h>

namespace DB
{

void CurrentThread::detachFromGroupIfNotDetached()
{
}

void CurrentThread::attachToGroup(const ThreadGroupPtr &)
{
}

void ThreadStatus::initGlobalProfiler(UInt64, UInt64)
{
}

}
