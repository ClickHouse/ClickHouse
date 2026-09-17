#include <IO/ReadCancellationToken.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

ReadCancellationToken ReadCancellationToken::create()
{
    ReadCancellationToken token;
    token.cancelled = std::make_shared<std::atomic_bool>(false);
    return token;
}

bool ReadCancellationToken::cancel() const noexcept
{
    chassert(cancelled);
    return !cancelled->exchange(true, std::memory_order_relaxed);
}

void ReadCancellationToken::checkIfNotCancelled() const
{
    CurrentThread::checkIfNotCancelled();
    if (isCancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "MergeTree read was cancelled by the client");
}

}
