#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <atomic>
#include <memory>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/GRPCServer.h>
#include <Server/IServer.h>
#include <Server/TCPServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>

namespace DB
{

class ArrowFlightHandler : public IGRPCServer, public arrow::flight::FlightServerBase
{
public:
    explicit ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_);

    ~ArrowFlightHandler() override;

    void start() override;

    void stop() override;

    UInt16 portNumber() const override;

    size_t currentThreads() const override { return 0; } // TODO

    size_t currentConnections() const override { return 0; } // TODO

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext &,
        const arrow::flight::Criteria *,
        std::unique_ptr<arrow::flight::FlightListing> * listings) override;

    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & request,
        std::unique_ptr<arrow::flight::FlightInfo> * info) override;

    arrow::Status PollFlightInfo(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & request,
        std::unique_ptr<arrow::flight::PollInfo> * info) override;

    arrow::Status GetSchema(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & request,
        std::unique_ptr<arrow::flight::SchemaResult> * schema) override;

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::Ticket & request,
        std::unique_ptr<arrow::flight::FlightDataStream> * stream) override;

    arrow::Status DoPut(
        const arrow::flight::ServerCallContext & context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

    arrow::Status DoExchange(
        const arrow::flight::ServerCallContext & context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMessageWriter> writer) override;

    arrow::Status DoAction(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::Action & action,
        std::unique_ptr<arrow::flight::ResultStream> * result) override;

    arrow::Status ListActions(const arrow::flight::ServerCallContext & context, std::vector<arrow::flight::ActionType> * actions) override;

private:
    IServer & server;
    LoggerPtr log;
    const Poco::Net::SocketAddress address_to_listen;
    std::optional<ThreadFromGlobalPool> server_thread;
    bool initialized = false;
    std::atomic<bool> stopped = false;

    virtual std::unique_ptr<Session> createSession(const arrow::flight::ServerCallContext & context);
};
}

#endif
