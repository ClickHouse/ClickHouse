#pragma once

#include <Core/Types.h>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/ProfileEvents.h>

#include "config.h"

#include <atomic>
#include <mutex>
#include <optional>

#if USE_SSL
#    include <Common/Crypto/KeyPair.h>
#endif

namespace DB
{
class TCPServer;

#if USE_SSL
/// The RSA key pair of the `sha256_password` authentication, with which a client encrypts its password on a
/// connection without TLS: the private key of the server certificate, or a generated one if there is none.
/// It is obtained on first use rather than at startup, where generating it took about 0.1 s of CPU for every
/// MySQL listener, while most servers never get a connection that needs it.
class MySQLKeyPairHolder
{
public:
    explicit MySQLKeyPairHolder(LoggerPtr log_) : log(std::move(log_)) {}

    KeyPair & get();

private:
    LoggerPtr log;
    std::once_flag initialized;
    std::optional<KeyPair> key_pair;
};
#endif

class MySQLHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;

#if USE_SSL
    MySQLKeyPairHolder keypair;

    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    bool secure_required = false;

    /// If set, overrides the `default_session_user` server setting for this listener.
    std::optional<String> default_session_user;

    std::atomic<unsigned> last_connection_id = 0;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
public:
    explicit MySQLHandlerFactory(
        IServer & server_,
        bool secure_required_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end(),
        std::optional<String> default_session_user_ = {});

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
};

}
