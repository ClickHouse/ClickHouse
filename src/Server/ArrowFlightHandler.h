#pragma once

#include <Interpreters/Session.h>
#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Server/GRPCServer.h>
#include "config.h"
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <Interpreters/Session.h>

namespace DB
{

class ArrowFlightHandler : public IGRPCServer, public arrow::flight::FlightServerBase
{
public:
    explicit ArrowFlightHandler(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_);

    virtual ~ArrowFlightHandler() override;

    virtual void start() override;

    virtual void stop() override;

    virtual UInt16 portNumber() const override;

    virtual size_t currentThreads() const override { return 0; } // TODO

    virtual size_t currentConnections() const override { return 0; } // TODO
    
    virtual arrow::Status ListFlights(
        const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

    virtual arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext& context,
                               const arrow::flight::FlightDescriptor& request,
                               std::unique_ptr<arrow::flight::FlightInfo>* info) override;

    virtual arrow::Status PollFlightInfo(const arrow::flight::ServerCallContext& context,
                                const arrow::flight::FlightDescriptor& request,
                                std::unique_ptr<arrow::flight::PollInfo>* info) override;

    virtual arrow::Status GetSchema(const arrow::flight::ServerCallContext& context,
                           const arrow::flight::FlightDescriptor& request,
                           std::unique_ptr<arrow::flight::SchemaResult>* schema) override;

    virtual arrow::Status DoGet(const arrow::flight::ServerCallContext& context, const arrow::flight::Ticket& request,
                       std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

    virtual arrow::Status DoPut(const arrow::flight::ServerCallContext& context,
                       std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                       std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

    virtual arrow::Status DoExchange(const arrow::flight::ServerCallContext& context,
                            std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                            std::unique_ptr<arrow::flight::FlightMessageWriter> writer) override;

    virtual arrow::Status DoAction(const arrow::flight::ServerCallContext& context, const arrow::flight::Action& action,
                          std::unique_ptr<arrow::flight::ResultStream>* result) override;

    virtual arrow::Status ListActions(const arrow::flight::ServerCallContext& context,
                             std::vector<arrow::flight::ActionType>* actions) override;

private:
    IServer & server;
    LoggerPtr log;
    arrow::flight::Location location;
    const Poco::Net::SocketAddress address_to_listen;
    std::unique_ptr<Session> session;
};
}
