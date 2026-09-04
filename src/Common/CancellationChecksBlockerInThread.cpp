#include <Common/CancellationChecksBlockerInThread.h>
#include <base/defines.h>

#include <cstdint>

namespace DB
{

namespace
{

thread_local constinit uint64_t counter = 0;

}

CancellationChecksBlockerInThread::CancellationChecksBlockerInThread()
{
    ++counter;
}

CancellationChecksBlockerInThread::~CancellationChecksBlockerInThread()
{
    chassert(counter > 0);
    --counter;
}

bool CancellationChecksBlockerInThread::isBlocked()
{
    return counter > 0;
}

}
