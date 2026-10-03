#pragma once

#include <cstdint>

#include <Common/FiberLocal.h>

/// Prevents `CurrentThread::checkIfNotCancelled` from throwing in cleanup code
/// that must complete after a query has already been cancelled.
class QueryCancellationBlockerInThread
{
private:
    static constinit FiberLocal<uint64_t, FiberLocalSlot::QUERY_CANCELLATION_BLOCKER_COUNTER> counter;

public:
    QueryCancellationBlockerInThread();
    ~QueryCancellationBlockerInThread();

    QueryCancellationBlockerInThread(const QueryCancellationBlockerInThread &) = delete;
    QueryCancellationBlockerInThread & operator=(const QueryCancellationBlockerInThread &) = delete;

    static bool isBlocked() { return counter > 0; }
};
