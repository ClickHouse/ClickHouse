#pragma once

#include <Common/CurrentMetrics.h>
#include <Poco/Net/TCPServerConnection.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "IRoutineServer.h"

#include <arrow/flight/server.h>

namespace CurrentMetrics
{
extern const Metric ArrowFlightConnection;
}

namespace DB
{

class ArrowFlightServer : public IRoutineServer, public arrow::flight::FlightServerBase
{
public:
    ArrowFlightServer(IServer & server_, const Poco::Net::SocketAddress & address_);

    std::string getLocation() const;

    void start() override;

    void stop() override;

    int currentConnections() const override;

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::Criteria * criteria,
        std::unique_ptr<arrow::flight::FlightListing> * listings
    ) override;

    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & request,
        std::unique_ptr<arrow::flight::FlightInfo> * info
    ) override;

    arrow::Status GetSchema(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & request,
        std::unique_ptr<arrow::flight::SchemaResult> * schema
    ) override;

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::Ticket & request,
        std::unique_ptr<arrow::flight::FlightDataStream> * data_stream
    ) override;

//    arrow::Status DoPut(
//        const arrow::flight::ServerCallContext & context,
//        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
//        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer
//    ) override;

private:
    static std::string getIpRepresentation(const Poco::Net::IPAddress & address);

private:
    Poco::Logger * log = &Poco::Logger::get("ArrowFlightServer");

    IServer & server;
    arrow::flight::Location location;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ArrowFlightConnection};
};

}
