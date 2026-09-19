#include <Common/HTTPConnectionInfo.h>

#include <atomic>

namespace DB
{

static thread_local HTTPConnectionInfo current_connection_info;

/// Whether the current thread is inside an `HTTPConnectionInfoScope`, i.e. issuing a blob storage
/// request whose log entry will take the published connection.
static thread_local bool capture_connection_info = false;

/// How many requests have gone out on this thread inside the current scope. More than one means the
/// entry that is about to be written covers several HTTP requests - retries inside an SDK call, or
/// a credential refresh made on the way - and its timing describes all of them, while the slot
/// holds only the last. Such an entry reports no connection at all rather than an arbitrary one of
/// the sockets it spans.
static thread_local size_t requests_in_scope = 0;

UInt64 nextHTTPConnectionId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

HTTPConnectionInfoScope::HTTPConnectionInfoScope()
    : previously_enabled(capture_connection_info)
    , previous_requests_in_scope(requests_in_scope)
{
    capture_connection_info = true;
    current_connection_info = {};
    requests_in_scope = 0;
}

HTTPConnectionInfoScope::~HTTPConnectionInfoScope()
{
    /// Whatever was published inside the scope and not taken is dropped here, so that the slot is
    /// never populated outside of a scope. Restoring the flag rather than clearing it keeps nested
    /// scopes correct, although nothing nests them today.
    capture_connection_info = previously_enabled;
    current_connection_info = {};
    requests_in_scope = previous_requests_in_scope;
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    if (!capture_connection_info)
        return;

    ++requests_in_scope;
    current_connection_info = info;
}

void clearCurrentHTTPConnectionInfo()
{
    /// The request this connection was published for never reached the wire, so it does not count
    /// as one of the requests the entry spans - otherwise a dead keep-alive socket replaced by a
    /// successful reconnect, which is one request over one socket, would look ambiguous.
    if (requests_in_scope)
        --requests_in_scope;
    current_connection_info = {};
}

HTTPConnectionInfo takeCurrentHTTPConnectionInfo()
{
    HTTPConnectionInfo result = current_connection_info;
    current_connection_info = {};

    /// Several requests under one entry: the entry's elapsed time covers all of them, so naming
    /// the socket of the last one would pair a whole retry sequence - including its backoff sleeps
    /// - with the identity and idle time of the attempt that finally succeeded. Report nothing
    /// instead; a connection column that is only sometimes about the request it sits next to is
    /// worse than an empty one.
    if (requests_in_scope > 1)
        return {};

    return result;
}

}
