#include <Storages/ArrowFlight/ArrowFlightConnection.h>

#if USE_ARROWFLIGHT
#include <algorithm>
#include <limits>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int BAD_ARGUMENTS;
}

ArrowFlightConnection::ArrowFlightConnection(const StorageArrowFlight::Configuration & config)
    : host(config.host)
    , port(config.port)
    , use_basic_authentication(config.use_basic_authentication)
    , username(config.username)
    , password(config.password)
    , enable_ssl(config.enable_ssl)
    , ssl_ca(config.ssl_ca)
    , ssl_override_hostname(config.ssl_override_hostname)
{
}

arrow::flight::TimeoutDuration ArrowFlightConnection::toTimeoutDuration(UInt64 timeout_sec)
{
    /// Zero is Arrow's own "no deadline"; it has to come before the clamp, because a zero
    /// TimeoutDuration would mean "deadline already reached".
    if (timeout_sec == 0)
        return arrow::flight::TimeoutDuration(-1);

    /// Arrow builds the deadline as now() + timeout and narrows the sum to the clock's microsecond
    /// rep, so a bound is only usable while that sum stays representable. Half of the rep's range is
    /// left for now(), which still keeps every value a caller can mean.
    static constexpr UInt64 max_timeout_sec = static_cast<UInt64>(std::numeric_limits<Int64>::max() / 2 / 1'000'000);
    return arrow::flight::TimeoutDuration(std::min(timeout_sec, max_timeout_sec));
}

std::shared_ptr<arrow::flight::FlightClient> ArrowFlightConnection::getClient(UInt64 timeout_sec) const
{
    connect(toTimeoutDuration(timeout_sec));

    std::lock_guard lock{mutex};
    return client;
}

arrow::flight::FlightCallOptions ArrowFlightConnection::getCallOptions(UInt64 timeout_sec) const
{
    auto timeout = toTimeoutDuration(timeout_sec);
    connect(timeout);

    std::lock_guard lock{mutex};
    auto call_options = *options;
    call_options.timeout = timeout;
    return call_options;
}

void ArrowFlightConnection::connect(arrow::flight::TimeoutDuration timeout) const
{
    {
        std::lock_guard lock{mutex};
        if (client)
            return;
    }

    auto location_result = enable_ssl ? arrow::flight::Location::ForGrpcTls(host, port) : arrow::flight::Location::ForGrpcTcp(host, port);
    if (!location_result.ok())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Arrow Flight endpoint specified: {}", location_result.status().ToString());
    }
    auto location = std::move(location_result).ValueOrDie();

    auto client_options = arrow::flight::FlightClientOptions::Defaults();

    if (enable_ssl)
    {
        if (!ssl_ca.empty())
            client_options.tls_root_certs = loadCertificate(ssl_ca);
        client_options.override_hostname = ssl_override_hostname;
    }

    auto client_result = arrow::flight::FlightClient::Connect(location, client_options);
    if (!client_result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to connect to Arrow Flight server: {}", client_result.status().ToString());
    }
    auto new_client = std::move(client_result).ValueOrDie();

    auto new_options = std::make_shared<arrow::flight::FlightCallOptions>();

    if (use_basic_authentication)
    {
        arrow::flight::FlightCallOptions auth_options;
        auth_options.timeout = timeout;

        auto auth_result = new_client->AuthenticateBasicToken(auth_options, username, password);
        if (!auth_result.ok())
        {
            throw Exception(
                ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to authenticate Arrow Flight server: {}", auth_result.status().ToString());
        }
        auto auth_token = std::move(auth_result).ValueOrDie();
        new_options->headers.push_back(auth_token);
    }

    /// Destroyed after the lock below is released: dropping a client shuts its gRPC transport down,
    /// which is the kind of call this function keeps off the locked path.
    std::shared_ptr<arrow::flight::FlightClient> superseded;
    std::lock_guard lock{mutex};

    /// Published only now, and only if nobody published first: a connection whose handshake failed
    /// must not be reused, and every query has to see the same authenticated client.
    if (client)
        superseded = std::move(new_client);
    else
    {
        client = std::move(new_client);
        options = std::move(new_options);
    }
}

String ArrowFlightConnection::loadCertificate(const String & path)
{
    ReadBufferFromFile buf{path};
    String str;
    readStringUntilEOF(str, buf);
    buf.close();
    return str;
}

std::shared_ptr<ArrowFlightConnection> ArrowFlightConnection::clone() const
{
    return std::shared_ptr<ArrowFlightConnection>{new ArrowFlightConnection(*this)};
}

std::shared_ptr<ArrowFlightConnection> ArrowFlightConnection::cloneWithHostAndPort(const String & host_, int port_) const
{
    auto res = clone();
    res->host = host_;
    res->port = port_;
    return res;
}

ArrowFlightConnection::ArrowFlightConnection(const ArrowFlightConnection & src)
    : host(src.host)
    , port(src.port)
    , use_basic_authentication(src.use_basic_authentication)
    , username(src.username)
    , password(src.password)
    , enable_ssl(src.enable_ssl)
    , ssl_ca(src.ssl_ca)
    , ssl_override_hostname(src.ssl_override_hostname)
{
}

}

#endif
