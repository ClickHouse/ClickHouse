#pragma once

#include <Core/Types.h>

namespace DB
{

/// Identity and age of the HTTP connection that carried one request.
///
/// Written by the pooled session just before the request goes out, then taken — once — by
/// whoever logs that request (currently `system.blob_storage_log`), inside an
/// `HTTPConnectionInfoScope` that the logging code opens around both. The handoff is a
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
    /// accounted for: an operation on local object storage, a request that failed before reaching
    /// the wire, or the tail events of a batch that shared a single request (a batched delete
    /// attributes the connection to its first event).
    bool has_value = false;
};

/// Hand out the next connection id. Called once per established socket.
UInt64 nextHTTPConnectionId();

/// Marks the current thread as issuing a blob storage request that is about to be logged.
///
/// The pool is shared: `StorageURL`, the dictionary sources, the proxy resolver, the REST catalogs,
/// the SDKs' own credential refreshes and the post-upload existence checks all send requests through
/// it, and none of them produces a `system.blob_storage_log` row. So publishing is opt-in, and the
/// opt-in belongs to the code that writes the row - not to the HTTP client, which cannot tell a
/// logged request from a helper one. The scope is opened right before the request is issued and
/// stays open until the log entry has been written, and it clears the slot both on entry and on
/// exit. A request issued outside of a scope publishes nothing; a request issued inside one whose
/// entry is, for whatever reason, never written leaves nothing behind. Either way, the slot is empty
/// whenever no scope is open, which is what lets a row for local or HDFS object storage - which uses
/// no HTTP connection at all - report zeroes.
///
/// When several requests go out inside one scope, the last one wins: an SDK retry supersedes the
/// failed attempt it replaces, and a credential refresh made on the way is superseded by the request
/// it was made for.
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

/// Publish the connection that is about to serve a request on this thread. Called by the pooled
/// session; does nothing outside of an `HTTPConnectionInfoScope`.
void setCurrentHTTPConnectionInfo(const HTTPConnectionInfo & info);

/// Drop whatever is published on this thread, without reading it. Called by the pooled session
/// when the connection it published for the request in flight is discarded before that request
/// reaches the wire — a borrowed keep-alive socket that turns out to be dead. The row the caller
/// writes for such a failure has to report zeroes rather than the identity of a socket that never
/// carried it.
void clearCurrentHTTPConnectionInfo();

/// Return the info for the most recent recorded request on this thread, and clear it. Clearing is
/// deliberate: a batched delete writes one entry per object for a single request, and only the
/// first of them should carry the connection. Must be called inside the scope that made the request.
HTTPConnectionInfo takeCurrentHTTPConnectionInfo();

}
