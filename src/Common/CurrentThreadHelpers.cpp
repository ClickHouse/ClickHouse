#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/CurrentThreadHelpers.h>

namespace DB
{

bool currentThreadHasGroup()
{
    return DB::CurrentThread::hasGroup();
}

LogsLevel currentThreadLogsLevel()
{
    return DB::CurrentThread::get().getClientLogsLevel();
}
}
