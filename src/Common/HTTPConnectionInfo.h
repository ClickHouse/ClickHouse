#pragma once

#include <Core/Types.h>

namespace DB
{

/// Identity and age of the HTTP connection that carried one request.
///
/// Written by the pooled session just before the request goes out, then taken — once — by
/// whoever logs that request (currently `system.blob_storage_log`). The handoff is a
/// thread-local because Poco's HTTP client is synchronous: the request and the log entry
/// describing it always run on the same thread, one right after the other.
///
/// The point of recording this is that a keep-alive connection is not equally fast over its
/// whole life. Two requests to the same object storage, issued at the same moment from the
/// same node, can differ several-fold in time-to-first-byte, and the connection they landed
/// on is the obvious suspect. None of that is visible from the aggregate ProfileEvents:
/// `DiskConnectionsReused` says a session came out of the pool, not how long it had been
/// sitting there.
struct HTTPConnectionInfo
{
    /// Process-unique id, assigned when the socket is established. The only field here that is
    /// safe to group by: the OS recycles both the port and the inode as soon as a socket closes,
    /// and it does so promptly enough that consecutive connections to the same endpoint routinely
    /// come back with the same pair.
    UInt64 id = 0;

    /// Local TCP port, and the socket inode as reported by fstat. Unique only among sockets that
    /// are open at the same moment, so use them to join against a packet capture or a
    /// `/proc/net/tcp` row taken at the time - not to tell one connection from the next.
    UInt16 local_port = 0;
    UInt64 socket_inode = 0;

    /// How many requests this socket had already sent before this one. 0 means this is its
    /// first request. Like the two fields below it is scoped to the socket, not to the session
    /// object wrapping it, so a reconnect starts the count over and all three stay consistent.
    UInt32 requests_served = 0;

    /// Since the current TCP connection was established.
    UInt64 age_microseconds = 0;

    /// Since the previous request on this socket was sent — i.e. how long the connection sat
    /// idle in the pool before being handed out for this request. 0 on a socket's first request.
    UInt64 idle_microseconds = 0;

    /// False when no blob storage request has been made on this thread since the last one was
    /// accounted for: an operation on local object storage, or the tail events of a batch that
    /// shared a single request (a batched delete attributes the connection to its first event).
    bool has_value = false;
};

/// Hand out the next connection id. Called once per established socket.
UInt64 nextHTTPConnectionId();

/// Arms the current thread for recording the connection behind a blob storage request.
///
/// Publishing is opt-in rather than automatic, because the pool is shared: `StorageURL`, the
/// dictionary sources, the proxy resolver and the various REST catalogs all borrow connections
/// from it and none of them logs anything. Were every borrow to publish, a request made by one of
/// them would sit in the slot until the next blob storage event on that thread took it - and a row
/// for local or HDFS object storage, which uses no HTTP connection at all, would report the socket
/// of an unrelated `StorageURL` read. So only the object storage clients (S3, Azure) open this
/// scope, and only requests issued inside it are recorded.
///
/// The scope covers issuing the request, not logging it: the log entry is written after the client
/// returns, so the value deliberately outlives the scope, and is cleared when it is taken. Entering
/// the scope also drops whatever was left in the slot, so a request that fails before reaching the
/// wire reports no connection instead of the previous one.
class HTTPConnectionInfoScope
{
public:
    HTTPConnectionInfoScope();
    ~HTTPConnectionInfoScope();

    HTTPConnectionInfoScope(const HTTPConnectionInfoScope &) = delete;
    HTTPConnectionInfoScope & operator=(const HTTPConnectionInfoScope &) = delete;

private:
    bool previously_enabled;
};

/// Publish the connection that is about to serve a request on this thread. Does nothing outside of
/// an `HTTPConnectionInfoScope`.
void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info);

/// Return the info for the most recent recorded request on this thread, and clear it. Clearing is
/// deliberate: without it a later log entry that made no request of its own would silently
/// inherit some earlier connection's numbers.
HTTPConnectionInfo takeCurrentHTTPConnectionInfo();

}
