#pragma once

#include <Common/Logger.h>
#include <Common/SecretString.h>
#include <Storages/NATS/NATSClient.h>
#include <base/types.h>

#include <memory>

namespace DB
{

struct NATSConfiguration
{
    String url;
    std::vector<String> servers;

    String username;
    SecretString password;
    SecretString token;
    String credential_file;
    SecretString credentials;
    String ca_file;
    String client_cert_file;
    String client_key_file;

    UInt64 max_connect_tries{};
    int reconnect_wait{};

    bool secure{};

    /// The settings of the client. A credentials file is read here, so it is read again on every connect.
    NATSClient::ConnectionSettings getConnectionSettings() const;
};

/// Loads the TLS material, which reports a file that cannot be read or parsed without opening a connection.
void validateNATSCertificates(const NATSConfiguration & configuration);

/// A connection to NATS, which `connect` establishes again once it is lost.
/// Not thread-safe: a connection is used by one thread at a time.
class NATSConnection
{
public:
    NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_);
    ~NATSConnection();

    /// Whether there is a connection, nothing has failed and the server has not closed it. Reads what is already there.
    bool isConnected();

    /// Whether there is no connection or something has failed already. Does not touch the socket.
    [[nodiscard]] bool hasFailed() const { return !connection || !connection->getError().empty(); }

    /// Connects, closing the previous connection first. Throws `CANNOT_CONNECT_NATS`.
    void connect();

    void disconnect();

    /// The current connection. Requires a successful `connect` before.
    NATSClient::Connection & get();

    [[nodiscard]] int getReconnectWait() const { return configuration.reconnect_wait; }
    [[nodiscard]] UInt64 getMaxConnectTries() const { return configuration.max_connect_tries; }

    /// The addresses can contain credentials, so they are not shown.
    [[nodiscard]] String connectionInfoForLog() const;

    /// Why the connection was lost or could not be established, e.g. `Authorization Violation`.
    [[nodiscard]] String lastErrorForLog() const;

private:
    const NATSConfiguration configuration;
    LoggerPtr log;
    std::unique_ptr<NATSClient::Connection> connection;
    String last_error;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}
