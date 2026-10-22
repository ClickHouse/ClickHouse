#include <Common/QueryCancellationBlockerInThread.h>

constinit FiberLocal<uint64_t, FiberLocalSlot::QUERY_CANCELLATION_BLOCKER_COUNTER> QueryCancellationBlockerInThread::counter;

QueryCancellationBlockerInThread::QueryCancellationBlockerInThread()
{
    counter = counter + 1;
}

QueryCancellationBlockerInThread::~QueryCancellationBlockerInThread()
{
    counter = counter - 1;
}
