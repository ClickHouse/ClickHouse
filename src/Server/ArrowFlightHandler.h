#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Common/ThreadPool.h>
#include <Server/GRPCServer.h>
#include <arrow/flight/server.h>


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

    arrow::Status DoAction(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::Action & action,
        std::unique_ptr<arrow::flight::ResultStream> * result) override;

private:
    arrow::Status tryRunAndLogIfError(std::string_view method_name, std::function<arrow::Status()> && func) const;
    arrow::Status evaluatePollDescriptor(const String & poll_descriptor);

    IServer & server;
    LoggerPtr log;
    const Poco::Net::SocketAddress address_to_listen;
    std::optional<ThreadFromGlobalPool> server_thread;
    std::optional<ThreadFromGlobalPool> cleanup_thread;
    bool initialized = false;
    std::atomic<bool> stopped = false;

    const UInt64 tickets_lifetime_seconds;
    const bool cancel_ticket_after_do_get;
    const UInt64 poll_descriptors_lifetime_seconds;
    const bool cancel_poll_descriptor_after_poll_flight_info;

    class CallsData;
    std::unique_ptr<CallsData> calls_data;
};

}

#endif
