#pragma once

#include "config.h"

/// TLSTransport.{h,cpp} always exist so the cmake source glob stays stable;
/// without USE_SSL this header contributes nothing (Cluster rejects the
/// Ssl/SaslSsl protocols with ErrorKind::NotSupported in that case).

#if USE_SSL

#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Net/TCPTransport.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocket.h>

#include <optional>

namespace Kafka::Net
{

/// TLS transport: TCPTransport provides connect, the poll-with-deadline-and-
/// wakeup machinery and the raw fd; on top of the connected non-blocking
/// socket a Poco::Net::SecureStreamSocket runs the TLS session. All waiting
/// stays in TCPTransport::waitReady — the secure socket is only ever asked
/// for non-blocking I/O and reports WANT_READ/WANT_WRITE sentinels back.
class TLSTransport : public TCPTransport
{
public:
    TLSTransport(std::shared_ptr<const WakeupSource> wakeup, TlsConfig tls_config_);

    void connect(const String & host, UInt16 port, const Deadline & deadline) override;
    size_t readSome(char * data, size_t size, const Deadline & deadline) override;
    void writeAll(std::string_view data, const Deadline & deadline) override;
    bool readable() override;
    void close() override;
    bool isOpen() const override;

private:
    /// Client SSL context built from tls_config.
    Poco::Net::Context::Ptr makeContext() const;

    TlsConfig tls_config;

    /// Engaged by a successful attach in connect(), disengaged by close();
    /// engagement doubles as the "attached" flag. Deliberately an optional
    /// and not a plain member: the default SecureStreamSocket constructor
    /// pulls SSLManager's global default client context (it reads the
    /// application configuration and loads CA stores) — a side effect that
    /// constructing a transport must not have.
    std::optional<Poco::Net::SecureStreamSocket> secure_socket;
};

}

#endif
