#include <Common/CurrentThread.h>
#include <Common/CurrentThreadHelpers.h>

namespace DB
{

bool currentThreadHasGroup()
{
    return DB::CurrentThread::getGroup() != nullptr;
}

LogsLevel currentThreadLogsLevel()
{
    return DB::CurrentThread::get().getClientLogsLevel();
}
}
