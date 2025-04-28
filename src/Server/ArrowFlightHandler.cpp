#include "ArrowFlightHandler.h"
#include <memory>
#include <arrow/result.h>
#include <arrow/flight/api.h>
#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/compute/api.h>
#include <arrow/array.h>
#include <Common/logger_useful.h>

namespace DB
{

    namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

    ArrowFlightHandler::ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightHandler"))
    , address_to_listen(address_to_listen_)
{
    auto parse_location_status = arrow::flight::Location::ForGrpcTcp(address_to_listen_.host().toString(), address_to_listen_.port());
    if (!parse_location_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Invalid address {} for Arrow Flight Server: {}",
                        address_to_listen_.toString(),
                        parse_location_status->ToString());
    }
    location = std::move(parse_location_status).ValueOrDie();
}

ArrowFlightHandler::~ArrowFlightHandler() {
}

void ArrowFlightHandler::start() {
    setThreadName("ArrowFlight");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::ARROW_FLIGHT);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(last_connection_id++);
    
    arrow::flight::FlightServerOptions options(location);
    auto init_status = Init(options);
    if (!init_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed init Arrow Flight Server: {}",
                        init_status.ToString());
    }

    auto serve_status = Serve();
    if (!serve_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed serve Arrow Flight: {}",
                        serve_status.ToString());
    } 
}

void ArrowFlightHandler::stop() {
    auto status = Shutdown();
    if (!status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed shutdown Arrow Flight: {}",
                        status.ToString());
    }
}

UInt16 ArrowFlightHandler::portNumber() const {
    return address_to_listen.port();
}

arrow::Status ArrowFlightHandler::ListFlights(
    const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {

    LOG_TRACE(log, "Sent handshake");

    std::vector<arrow::flight::FlightInfo> flights;
    *listings = std::unique_ptr<arrow::flight::FlightListing>(
        new arrow::flight::SimpleFlightListing(std::move(flights)));
    return arrow::Status::OK();
}

}
