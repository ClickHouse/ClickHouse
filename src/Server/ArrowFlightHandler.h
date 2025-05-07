#pragma once

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

namespace DB
{

class ArrowFlightHandler : public IGRPCServer, public arrow::flight::FlightServerBase
{
private:
    [[maybe_unused]] IServer & server;
    [[maybe_unused]] LoggerPtr log;
    [[maybe_unused]] Int32 last_connection_id = 0;
    [[maybe_unused]] arrow::flight::Location location;
    const Poco::Net::SocketAddress address_to_listen;

public:
    explicit ArrowFlightHandler(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_);

    ~ArrowFlightHandler() override;

    void start() override;

    void stop() override;

    UInt16 portNumber() const override;

    size_t currentThreads() const override { return 0; } // TODO

    size_t currentConnections() const override { return 0; } // TODO
    
    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

};
}
