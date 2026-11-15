#pragma once

#include "config.h"

#if USE_ARROWFLIGHT

#include <Common/ThreadPool.h>
#include <Server/ArrowFlight/FlightStatus.h>
#include <Server/ArrowFlight/commandSelector.h>
#include <Server/IGRPCServer.h>
#include <Server/IServer.h>

#include <arrow/flight/Flight.grpc.pb.h>
#include <grpcpp/server.h>

#include <Poco/Net/SocketAddress.h>


namespace DB
{

namespace ArrowFlight { class CallsData; }
class AuthMiddleware;
class AuthMiddlewareFactory;

/// The Arrow Flight (and Arrow Flight SQL) server: a gRPC service implementing `Flight.proto`, which carries the
/// results of queries as Arrow IPC messages written by the native `ArrowStream` writer.
class ArrowFlightServer : public IGRPCServer
{
public:
    explicit ArrowFlightServer(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_);

    ~ArrowFlightServer() override;

    void start() override;

    void stop() override;

    UInt16 portNumber() const override;

    size_t currentThreads() const override { return 0; } // TODO

    size_t currentConnections() const override { return 0; } // TODO

private:
    class Service;
    friend class Service;

    using Status = ArrowFlight::Status;
    using FlightDescriptor = arrow::flight::protocol::FlightDescriptor;

    Status getFlightInfo(const AuthMiddleware & auth, const FlightDescriptor & request, arrow::flight::protocol::FlightInfo & info);
    Status pollFlightInfo(const AuthMiddleware & auth, const FlightDescriptor & request, arrow::flight::protocol::PollInfo & info);
    Status getSchema(const AuthMiddleware & auth, const FlightDescriptor & request, arrow::flight::protocol::SchemaResult & schema);
    Status doGet(const AuthMiddleware & auth, const arrow::flight::protocol::Ticket & request,
        grpc::ServerWriter<arrow::flight::protocol::FlightData> & writer);
    Status doPut(const AuthMiddleware & auth,
        grpc::ServerReaderWriter<arrow::flight::protocol::PutResult, arrow::flight::protocol::FlightData> & stream);
    Status doAction(const AuthMiddleware & auth, const arrow::flight::protocol::Action & action,
        grpc::ServerWriter<arrow::flight::protocol::Result> & writer);

    using DecodeResult = std::tuple<std::string, ArrowFlight::SchemaModifier, ArrowFlight::BlockModifier, ArrowFlight::TablePtr>;

    [[nodiscard]] ArrowFlight::Result<DecodeResult> decodeDescriptor(
        const FlightDescriptor & descriptor,
        bool for_put_operation,
        const std::string & username) const;

    /// Authenticates a call, runs `func` and reports its outcome to the client.
    grpc::Status handleCall(
        grpc::ServerContext & context, std::string_view method_name, const std::function<Status(const AuthMiddleware &)> & func);

    Status tryRunAndLogIfError(std::string_view method_name, const std::function<Status()> & func) const;
    Status evaluatePollDescriptor(const String & poll_descriptor);

    IServer & server;
    LoggerPtr log;
    const Poco::Net::SocketAddress address_to_listen;
    std::optional<ThreadFromGlobalPool> cleanup_thread;
    bool initialized = false;
    std::atomic<bool> stopped = false;

    const UInt64 tickets_lifetime_seconds;
    const bool cancel_ticket_after_do_get;
    const UInt64 poll_descriptors_lifetime_seconds;
    const bool cancel_poll_descriptor_after_poll_flight_info;
    const UInt64 max_prepared_statements_per_user;
    const Int64 prepared_statements_lifetime_seconds;

    std::unique_ptr<ArrowFlight::CallsData> calls_data;
    std::unique_ptr<AuthMiddlewareFactory> auth_factory;
    std::unique_ptr<Service> service;
    std::unique_ptr<grpc::Server> grpc_server;
};

}

#endif
