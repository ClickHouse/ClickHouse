#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Common/SecretString.h>
#include <Storages/StorageArrowFlight.h>

#include <arrow/flight/Flight.grpc.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>

#include <optional>


namespace DB
{

/// A connection to an Arrow Flight server: a gRPC channel, and the token of the server when it authenticates with
/// a user name and a password. Connected on first use.
class ArrowFlightConnection
{
public:
    using Stub = arrow::flight::protocol::FlightService::Stub;

    explicit ArrowFlightConnection(const StorageArrowFlight::Configuration & config);

    const String & getHost() const { return host; }
    int getPort() const { return port; }

    /// The stub of the Flight service. Connects and authenticates first if needed, within `timeout_sec` (0 = none).
    std::shared_ptr<Stub> getStub(UInt64 timeout_sec) const;

    /// Sets the deadline of a call and the header of the authentication.
    void prepareCall(grpc::ClientContext & context, UInt64 timeout_sec) const;

    /// The error of a failed call as the Arrow Flight client of the Apache Arrow library reports it, e.g.
    /// "IOError: Flight returned timeout error, with message: Deadline Exceeded. Detail: TimedOut".
    static String formatError(const grpc::Status & status, const grpc::ClientContext & context);

    std::shared_ptr<ArrowFlightConnection> clone() const;
    std::shared_ptr<ArrowFlightConnection> cloneWithHostAndPort(const String & host_, int port_) const;

private:
    void connect(UInt64 timeout_sec) const;
    static void setDeadline(grpc::ClientContext & context, UInt64 timeout_sec);
    static String loadCertificate(const String & path);

    ArrowFlightConnection(const ArrowFlightConnection & src);

    String host;
    int port;
    const bool use_basic_authentication;
    const String username;
    const SecretString password;
    const bool enable_ssl;
    const String ssl_ca;
    const String ssl_override_hostname;

    mutable std::shared_ptr<Stub> stub TSA_GUARDED_BY(mutex);
    /// The value of the `authorization` header of the calls: the token the server gave in the handshake.
    mutable SecretString authorization TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}

#endif
