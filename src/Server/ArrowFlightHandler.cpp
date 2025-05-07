#include "ArrowFlightHandler.h"
#include <memory>
#include <arrow/result.h>
#include <arrow/flight/api.h>
#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/compute/api.h>
#include <arrow/array.h>

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
}

ArrowFlightHandler::~ArrowFlightHandler() {
}

void ArrowFlightHandler::start() {
    setThreadName("ArrowFlight");

    auto parse_location_status = arrow::flight::Location::ForGrpcTcp("127.0.0.1", 8888);
    if (!parse_location_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Invalid address {} for Arrow Flight Server: {}",
                        "123",
                        parse_location_status->ToString());
    location = std::move(parse_location_status).ValueOrDie();
    
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

void ArrowFlightHandler::stop() {
    auto status = Shutdown();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed shutdown Arrow Flight: {}",
                        status.ToString());
}

UInt16 ArrowFlightHandler::portNumber() const {
    return address_to_listen.port();
}

arrow::Status ArrowFlightHandler::ListFlights(
    const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {

  std::vector<arrow::flight::FlightInfo> flights;
  *listings = std::unique_ptr<arrow::flight::FlightListing>(
      new arrow::flight::SimpleFlightListing(std::move(flights)));
  return arrow::Status::OK();
}

}
