#include <Common/HTTPConnectionInfo.h>

#include <atomic>

namespace DB
{

static thread_local HTTPConnectionInfo current_connection_info;

/// Whether requests issued on this thread belong to a blob storage operation, and are therefore
/// worth recording. See `HTTPConnectionInfoScope`.
static thread_local bool capture_connection_info = false;

UInt64 nextHTTPConnectionId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

HTTPConnectionInfoScope::HTTPConnectionInfoScope()
    : previously_enabled(capture_connection_info)
{
    capture_connection_info = true;
    current_connection_info = {};
}

HTTPConnectionInfoScope::~HTTPConnectionInfoScope()
{
    /// Note that what was published inside the scope stays in the slot on purpose - the log entry
    /// describing the request is written after the scope ends.
    capture_connection_info = previously_enabled;
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    if (!capture_connection_info)
        return;

    current_connection_info = info;
}

HTTPConnectionInfo takeCurrentHTTPConnectionInfo()
{
    HTTPConnectionInfo result = current_connection_info;
    current_connection_info = {};
    return result;
}

}
