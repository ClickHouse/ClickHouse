#pragma once

#include <base/strong_typedef.h>
#include <Common/SSHWrapper.h>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <optional>
#include <string>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class JWTProvider;

struct ConnectionParameters
{
    String host;
    UInt16 port{};
    std::string default_database;
    std::string user;
    std::string password;
    /// Set when `password` carries a time-based one-time password after a '+'. The server accepts every such
    /// code once, so a session opened later (a background job) needs a fresh one: see `renewSecondFactor`.
    bool has_second_factor = false;
    std::string password_without_second_factor;
    std::string proto_send_chunked = "notchunked";
    std::string proto_recv_chunked = "notchunked";
    std::string quota_key;
    SSHKey ssh_private_key;
    std::string jwt;
#if USE_JWT_CPP && USE_SSL
    std::shared_ptr<JWTProvider> jwt_provider;
#endif
    Protocol::Secure security = Protocol::Secure::Disable;
    std::string tls_sni_override;
    std::string bind_host;
    Protocol::Compression compression = Protocol::Compression::Enable;
    ConnectionTimeouts timeouts;

    /// The address of `host` that is already known to accept connections on `port`, if any.
    /// It is tried first, so that the addresses that do not answer do not delay the connection.
    std::optional<Poco::Net::SocketAddress> preferred_address;

    /// A connection to `preferred_address` that has already been established, to be taken over by the
    /// `Connection` instead of opening a second one to the same endpoint (see `Connection::setAdoptedSocket`).
    std::optional<Poco::Net::StreamSocket> adopted_socket;

    using Database = StrongTypedef<String, struct DatabaseTag>;
    using Host = StrongTypedef<String, struct HostTag>;

    ConnectionParameters() = default;
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config_, const Host & host_, const Database & database_);
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config_, const Host & host_, const Database & database_, std::optional<UInt16> port_);

#if USE_JWT_CPP && USE_SSL
    /// Replaces `jwt` with the current token of `jwt_provider`, which refreshes it when it is expired or about to expire.
    /// For parameters copied from a login that happened a while ago, e.g. by a background job.
    void refreshJWT();
#endif

    /// Replaces the one-time password of `password` with a freshly entered one. Asks on the terminal, so call it
    /// from the thread that owns it. Returns false if there is nothing to ask with (no terminal) or nothing was entered.
    /// Does nothing and returns true if the login did not use a one-time password.
    bool renewSecondFactor();

    static UInt16 getPortFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & connection_host);

    /// Ask to enter the user's password if password option contains this value.
    /// "\n" is used because there is hardly a chance that a user would use '\n' as password.
    static constexpr std::string_view ASK_PASSWORD = "\n";

    static ConnectionParameters createForEmbedded(const String & user, const String & database);
};

}
