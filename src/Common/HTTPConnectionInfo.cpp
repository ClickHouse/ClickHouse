#include <Common/HTTPConnectionInfo.h>

#include <atomic>

namespace DB
{

static thread_local HTTPConnectionInfo current_connection_info;

/// Whether the current thread is inside an enabled `HTTPConnectionInfoScope`, i.e. issuing a blob storage
/// request whose log entry will take the published connection.
static thread_local bool capture_connection_info = false;

/// How many requests have gone out on this thread inside the current scope. More than one means the
/// entry that is about to be written covers several HTTP requests - retries inside an SDK call, or
/// a credential refresh made on the way - and its timing describes all of them, while the slot
/// holds only the last. Such an entry reports no connection at all rather than an arbitrary one of
/// the sockets it spans.
static thread_local size_t requests_in_scope = 0;

/// Set by `beginHTTPConnectionInfoTransportAttempt` and consumed by the next
/// `beginHTTPConnectionInfoRequest`, so that an attempt counted by the transport before its
/// connection setup is not counted again when the pooled session sends it.
static thread_local bool transport_attempt_pending = false;

/// Whether the attempt counted by the transport may publish its connection, decided when it was
/// counted. Kept apart from `current_request_publishes`, because a helper request - such as a proxy
/// lookup - can be sent between the two calls.
static thread_local bool transport_attempt_publishes = true;

/// Whether the request being issued may publish its connection. False for helper traffic - a token or
/// credential request made on the way to the logged one - which counts towards `requests_in_scope`
/// but must never be reported as the connection of the logged request.
static thread_local bool current_request_publishes = true;

/// Set while an `HTTPConnectionInfoHelperRequests` is alive on this thread.
static thread_local bool helper_requests = false;

UInt64 nextHTTPConnectionId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

HTTPConnectionInfoScope::HTTPConnectionInfoScope(bool enabled)
    : previously_enabled(capture_connection_info)
    , previous_requests_in_scope(requests_in_scope)
    , previous_transport_attempt_pending(transport_attempt_pending)
    , previous_transport_attempt_publishes(transport_attempt_publishes)
    , previous_current_request_publishes(current_request_publishes)
    , previous_helper_requests(helper_requests)
{
    capture_connection_info = enabled;
    current_connection_info = {};
    requests_in_scope = 0;
    transport_attempt_pending = false;
    transport_attempt_publishes = true;
    current_request_publishes = true;
    helper_requests = false;
}

HTTPConnectionInfoScope::~HTTPConnectionInfoScope()
{
    /// Whatever was published inside the scope and not taken is dropped here, so that the slot is
    /// never populated outside of a scope. Restoring the flag rather than clearing it keeps nested
    /// scopes correct, although nothing nests them today.
    capture_connection_info = previously_enabled;
    current_connection_info = {};
    requests_in_scope = previous_requests_in_scope;
    transport_attempt_pending = previous_transport_attempt_pending;
    transport_attempt_publishes = previous_transport_attempt_publishes;
    current_request_publishes = previous_current_request_publishes;
    helper_requests = previous_helper_requests;
}

HTTPConnectionInfoHelperRequests::HTTPConnectionInfoHelperRequests()
    : previous_helper_requests(helper_requests)
{
    helper_requests = true;
}

HTTPConnectionInfoHelperRequests::~HTTPConnectionInfoHelperRequests()
{
    helper_requests = previous_helper_requests;
}

bool isHTTPConnectionInfoCaptureEnabled()
{
    return capture_connection_info;
}

void beginHTTPConnectionInfoTransportAttempt(bool logged_request)
{
    if (!capture_connection_info)
        return;

    ++requests_in_scope;
    current_connection_info = {};
    transport_attempt_pending = true;
    transport_attempt_publishes = logged_request && !helper_requests;
}

void beginHTTPConnectionInfoRequest()
{
    if (!capture_connection_info)
        return;

    current_connection_info = {};

    /// A helper request - such as a proxy lookup made by the S3 client after it counted its attempt -
    /// is a request of its own and must not take over the attempt counted for the logged one, unless
    /// that attempt is itself helper traffic.
    if (transport_attempt_pending && (!helper_requests || !transport_attempt_publishes))
    {
        /// Already counted by the transport before it set up the connection, which also decided
        /// whether the request may publish it.
        transport_attempt_pending = false;
        current_request_publishes = transport_attempt_publishes;
        return;
    }

    /// Counted per request rather than per socket: a dead keep-alive socket replaced by a reconnect
    /// inside one `sendRequest` is still one request, and does not make the entry ambiguous.
    ++requests_in_scope;
    current_request_publishes = !helper_requests;
}

void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info)
{
    if (!capture_connection_info || !current_request_publishes)
        return;

    current_connection_info = info;
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
