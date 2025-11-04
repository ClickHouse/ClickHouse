#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Storages/StorageArrowFlight.h>
#include <arrow/flight/client.h>


namespace DB
{

class ArrowFlightConnection
{
public:
    explicit ArrowFlightConnection(const StorageArrowFlight::Configuration & config);

    const String & getHost() const { return host; }
    int getPort() const { return port; }

    std::shared_ptr<arrow::flight::FlightClient> getClient() const;
    std::shared_ptr<const arrow::flight::FlightCallOptions> getOptions() const;

    /// Makes another connection with the same parameters.
    std::shared_ptr<ArrowFlightConnection> clone() const;

    /// Makes another connection with the same parameters except for the host and port.
    std::shared_ptr<ArrowFlightConnection> cloneWithHostAndPort(const String & host_, int port_) const;

private:
    void connect() const TSA_REQUIRES(mutex);
    static String loadCertificate(const String & path);

    ArrowFlightConnection(const ArrowFlightConnection & src);

    String host;
    int port;
    const bool use_basic_authentication;
    const String username;
    const String password;
    const bool enable_ssl;
    const String ssl_ca;
    const String ssl_override_hostname;
    mutable std::shared_ptr<arrow::flight::FlightClient> client TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const arrow::flight::FlightCallOptions> options TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}

#endif
