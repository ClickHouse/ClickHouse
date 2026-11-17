#pragma once

#include <base/types.h>


namespace Poco::Net
{
    class SocketImpl;
}

namespace DB
{

/// A channel binding for `ssh_key` authentication over the native TCP protocol.
///
/// The signature the client makes over the challenge must only be valid on the connection it was made on.
/// To achieve this, both ends mix a value derived from their own end of the connection into the signed message.
/// Each end derives it independently from its own socket; the value is never sent over the wire,
/// and neither its type nor whether TLS is used is ever taken from the peer.
struct SSHKeyAuthChannelBinding
{
    /// "tls-exporter" for a TLS connection, "none" for a plaintext connection.
    String type;
    /// The 32-byte RFC 9266 `tls-exporter` value for a TLS connection, empty for a plaintext connection.
    String value;
};

/// Derives the channel binding from the local end of an established connection.
/// Throws if the connection uses TLS but its session cannot provide a safe binding
/// (for example, a TLS 1.2 session without the Extended Master Secret extension).
/// It never falls back to the "none" binding for a TLS connection.
/// On a TLS 1.2 connection, it disables renegotiation for the rest of the connection (RFC 9266, section 4.2).
SSHKeyAuthChannelBinding getSSHKeyAuthChannelBinding(const Poco::Net::SocketImpl * socket_impl);

/// Builds the message that the client signs and the server verifies, in the layout used
/// since `DBMS_MIN_REVISION_WITH_SSH_CHANNEL_BINDING`. Every field is length-prefixed,
/// so the boundaries between the fields cannot be confused.
String buildSSHKeyAuthMessage(
    UInt64 protocol_version,
    const String & default_database,
    const String & user,
    const String & challenge,
    const SSHKeyAuthChannelBinding & binding);

}
