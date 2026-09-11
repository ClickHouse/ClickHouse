#include <Common/HTTPConnectionInfo.h>

#include <atomic>

namespace DB
{

static thread_local HTTPConnectionInfo current_connection_info;

/// Whether the current thread is inside an `HTTPConnectionInfoScope`, i.e. issuing a blob storage
/// request whose log entry will take the published connection.
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
    /// Whatever was published inside the scope and not taken is dropped here, so that the slot is
    /// never populated outside of a scope. Restoring the flag rather than clearing it keeps nested
    /// scopes correct, although nothing nests them today.
    capture_connection_info = previously_enabled;
    current_connection_info = {};
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    if (!capture_connection_info)
        return;

    current_connection_info = info;
}

void clearCurrentHTTPConnectionInfo()
{
    current_connection_info = {};
}

HTTPConnectionInfo takeCurrentHTTPConnectionInfo()
{
    HTTPConnectionInfo result = current_connection_info;
    current_connection_info = {};
    return result;
}

}
