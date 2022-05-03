#include <Common/CurrentThread.h>

namespace DB
{

static String getQueryId()
{
    if (!CurrentThread::isInitialized() || !CurrentThread::get().getQueryContext())
        return "";
    return CurrentThread::getQueryId().toString();
}

}
