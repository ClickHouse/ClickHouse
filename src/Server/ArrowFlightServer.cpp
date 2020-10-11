#include "ArrowFlightServer.h"
#include <arrow/flight/test_util.h> // FIXME: Remove it before merge

namespace
{

arrow::Status GetBatchForFlight(const arrow::flight::Ticket & ticket, std::shared_ptr<arrow::RecordBatchReader> * out) {
    if (ticket.ticket == "ticket-ints-1") {
        arrow::flight::BatchVector batches;
        RETURN_NOT_OK(arrow::flight::ExampleIntBatches(&batches));
        *out = std::make_shared<arrow::flight::BatchIterator>(batches[0]->schema(), batches);
        return arrow::Status::OK();
    } else if (ticket.ticket == "ticket-dicts-1") {
        arrow::flight::BatchVector batches;
        RETURN_NOT_OK(arrow::flight::ExampleDictBatches(&batches));
        *out = std::make_shared<arrow::flight::BatchIterator>(batches[0]->schema(), batches);
        return arrow::Status::OK();
    } else {
        return arrow::Status::NotImplemented("no stream implemented for this ticket");
    }
}

} // FIXME: Remove it before merge

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

ArrowFlightServer::ArrowFlightServer(IServer & server_, std::string host, int port)
    : server(server_)
{
    auto parse_location_status = arrow::flight::Location::ForGrpcTcp(host, port, &location);
    if (!parse_location_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Invalid location {}:{} for Arrow Flight Server: {}",
                        host,
                        port,
                        parse_location_status.ToString());
}

std::string ArrowFlightServer::getLocation() const {
    return location.ToString();
}

void ArrowFlightServer::start() {
    arrow::flight::FlightServerOptions options(location);
    auto init_status = Init(options);
    if (!init_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed init Arrow Flight Server: {}",
                        init_status.ToString());

    // Exit with a clean error code (0) on SIGTERM
    // ARROW_CHECK_OK(handler->SetShutdownOnSignals({SIGTERM}));

    auto serve_status = Serve();
    if (!serve_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed serve Arrow Flight: {}",
                        serve_status.ToString());
}

void ArrowFlightServer::stop() {
    auto status = Shutdown();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed shutdown Arrow Flight: {}",
                        status.ToString());
}

int ArrowFlightServer::currentConnections() const {
    return 0; // FIXME: implement
}

arrow::Status ArrowFlightServer::ListFlights(
    const arrow::flight::ServerCallContext & /* context */,
    const arrow::flight::Criteria * criteria,
    std::unique_ptr<arrow::flight::FlightListing> * listings)
{
    std::vector<arrow::flight::FlightInfo> flights = arrow::flight::ExampleFlightInfo();
    if (criteria && criteria->expression != "") {
        // For test purposes, if we get criteria, return no results
        flights.clear();
    }
    *listings = std::unique_ptr<arrow::flight::FlightListing>(new arrow::flight::SimpleFlightListing(flights));
    return arrow::Status::OK();
}

arrow::Status ArrowFlightServer::GetFlightInfo(
    const arrow::flight::ServerCallContext & /* context */,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * out_info)
{
    // Test that Arrow-C++ status codes can make it through gRPC
    if (request.type == arrow::flight::FlightDescriptor::DescriptorType::CMD &&
        request.cmd == "status-outofmemory") {
        return arrow::Status::OutOfMemory("Sentinel");
    }

    std::vector<arrow::flight::FlightInfo> flights = arrow::flight::ExampleFlightInfo();

    for (const auto& info : flights) {
        if (info.descriptor().Equals(request)) {
            *out_info = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(info));
            return arrow::Status::OK();
        }
    }
    return arrow::Status::Invalid("Flight not found: ", request.ToString());
}

arrow::Status ArrowFlightServer::GetSchema(
    const arrow::flight::ServerCallContext & /* context */,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * schema)
{
    std::vector<arrow::flight::FlightInfo> flights = arrow::flight::ExampleFlightInfo();

    for (const auto& info : flights) {
        if (info.descriptor().Equals(request)) {
            *schema =
                std::unique_ptr<arrow::flight::SchemaResult>(new arrow::flight::SchemaResult(info.serialized_schema()));
            return arrow::Status::OK();
        }
    }
    return arrow::Status::Invalid("Flight not found: ", request.ToString());
}

arrow::Status ArrowFlightServer::DoGet(
    const arrow::flight::ServerCallContext & /* context */,
    const arrow::flight::Ticket & request,
    std::unique_ptr<arrow::flight::FlightDataStream> * data_stream)
{
    // Test for ARROW-5095
    if (request.ticket == "ARROW-5095-fail") {
        return arrow::Status::UnknownError("Server-side error");
    }
    if (request.ticket == "ARROW-5095-success") {
        return arrow::Status::OK();
    }

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    RETURN_NOT_OK(GetBatchForFlight(request, &batch_reader));

    *data_stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(batch_reader));
    return arrow::Status::OK();
}

}
