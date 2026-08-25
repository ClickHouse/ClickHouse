#include <Common/HTTPConnectionInfo.h>

#include <atomic>

namespace DB
{

static thread_local HTTPConnectionInfo current_connection_info;

UInt64 nextHTTPConnectionId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    current_connection_info = info;
}

HTTPConnectionInfo takeCurrentHTTPConnectionInfo()
{
    HTTPConnectionInfo result = current_connection_info;
    current_connection_info = {};
    return result;
}

}
