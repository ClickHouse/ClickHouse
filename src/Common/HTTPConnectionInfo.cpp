#include <Common/HTTPConnectionInfo.h>

#include <Common/FiberLocal.h>

#include <atomic>

namespace DB
{

namespace
{

/// Fiber-local rather than `thread_local`: a fiber may resume on another thread in the middle of a request,
/// and the fibers that run on one thread must not see each other's scopes.
struct State
{
    HTTPConnectionInfo current_connection_info;

    /// Whether the current thread is inside an enabled `HTTPConnectionInfoScope`, i.e. issuing a blob storage
    /// request whose log entry will take the published connection.
    bool capture_connection_info = false;

    /// How many requests have gone out on this thread inside the current scope. More than one means the
    /// entry that is about to be written covers several HTTP requests - retries inside an SDK call, or
    /// a credential refresh made on the way - and its timing describes all of them, while the slot
    /// holds only the last. Such an entry reports no connection at all rather than an arbitrary one of
    /// the sockets it spans.
    size_t requests_in_scope = 0;

    /// Set by `beginHTTPConnectionInfoTransportAttempt` and consumed by the next
    /// `beginHTTPConnectionInfoRequest`, so that an attempt counted by the transport before its
    /// connection setup is not counted again when the pooled session sends it.
    bool transport_attempt_pending = false;

    /// Whether the attempt counted by the transport may publish its connection, decided when it was
    /// counted. Kept apart from `current_request_publishes`, because a helper request - such as a proxy
    /// lookup - can be sent between the two calls.
    bool transport_attempt_publishes = true;

    /// Whether the request being issued may publish its connection. False for helper traffic - a token or
    /// credential request made on the way to the logged one - which counts towards `requests_in_scope`
    /// but must never be reported as the connection of the logged request.
    bool current_request_publishes = true;

    /// Set while an `HTTPConnectionInfoHelperRequests` is alive on this thread.
    bool helper_requests = false;
};

constinit FiberLocal<State, FiberLocalSlot::HTTP_CONNECTION_INFO> state;

}

UInt64 nextHTTPConnectionId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

HTTPConnectionInfoScope::HTTPConnectionInfoScope(bool enabled)
{
    State & local = state.get();
    previously_enabled = local.capture_connection_info;
    previous_requests_in_scope = local.requests_in_scope;
    previous_transport_attempt_pending = local.transport_attempt_pending;
    previous_transport_attempt_publishes = local.transport_attempt_publishes;
    previous_current_request_publishes = local.current_request_publishes;
    previous_helper_requests = local.helper_requests;

    local.capture_connection_info = enabled;
    local.current_connection_info = {};
    local.requests_in_scope = 0;
    local.transport_attempt_pending = false;
    local.transport_attempt_publishes = true;
    local.current_request_publishes = true;
    local.helper_requests = false;
}

HTTPConnectionInfoScope::~HTTPConnectionInfoScope()
{
    /// Whatever was published inside the scope and not taken is dropped here, so that the slot is
    /// never populated outside of a scope. Restoring the flag rather than clearing it keeps nested
    /// scopes correct, although nothing nests them today.
    State & local = state.get();
    local.capture_connection_info = previously_enabled;
    local.current_connection_info = {};
    local.requests_in_scope = previous_requests_in_scope;
    local.transport_attempt_pending = previous_transport_attempt_pending;
    local.transport_attempt_publishes = previous_transport_attempt_publishes;
    local.current_request_publishes = previous_current_request_publishes;
    local.helper_requests = previous_helper_requests;
}

HTTPConnectionInfoHelperRequests::HTTPConnectionInfoHelperRequests()
{
    State & local = state.get();
    previous_helper_requests = local.helper_requests;
    local.helper_requests = true;
}

HTTPConnectionInfoHelperRequests::~HTTPConnectionInfoHelperRequests()
{
    state->helper_requests = previous_helper_requests;
}

bool isHTTPConnectionInfoCaptureEnabled()
{
    return state->capture_connection_info;
}

void beginHTTPConnectionInfoTransportAttempt(bool logged_request)
{
    State & local = state.get();
    if (!local.capture_connection_info)
        return;

    ++local.requests_in_scope;
    local.current_connection_info = {};
    local.transport_attempt_pending = true;
    local.transport_attempt_publishes = logged_request && !local.helper_requests;
}

void beginHTTPConnectionInfoRequest()
{
    State & local = state.get();
    if (!local.capture_connection_info)
        return;

    local.current_connection_info = {};

    /// A helper request - such as a proxy lookup made by the S3 client after it counted its attempt -
    /// is a request of its own and must not take over the attempt counted for the logged one, unless
    /// that attempt is itself helper traffic.
    if (local.transport_attempt_pending && (!local.helper_requests || !local.transport_attempt_publishes))
    {
        /// Already counted by the transport before it set up the connection, which also decided
        /// whether the request may publish it.
        local.transport_attempt_pending = false;
        local.current_request_publishes = local.transport_attempt_publishes;
        return;
    }

    /// Counted per request rather than per socket: a dead keep-alive socket replaced by a reconnect
    /// inside one `sendRequest` is still one request, and does not make the entry ambiguous.
    ++local.requests_in_scope;
    local.current_request_publishes = !local.helper_requests;
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    State & local = state.get();
    if (!local.capture_connection_info || !local.current_request_publishes)
        return;

    local.current_connection_info = info;
}

HTTPConnectionInfo takeCurrentHTTPConnectionInfo()
{
    State & local = state.get();
    HTTPConnectionInfo result = local.current_connection_info;
    local.current_connection_info = {};

    /// Several requests under one entry: the entry's elapsed time covers all of them, so naming
    /// the socket of the last one would pair a whole retry sequence - including its backoff sleeps
    /// - with the identity and idle time of the attempt that finally succeeded. Report nothing
    /// instead; a connection column that is only sometimes about the request it sits next to is
    /// worse than an empty one.
    if (local.requests_in_scope > 1)
        return {};

    return result;
}

}
