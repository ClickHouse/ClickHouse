#include <Storages/ArrowFlight/ArrowFlightConnection.h>

#if USE_ARROWFLIGHT
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

std::shared_ptr<arrow::flight::FlightClient> ArrowFlightConnection::getClient() const
{
    std::lock_guard lock{mutex};
    connect();
    return client;
}

std::shared_ptr<const arrow::flight::FlightCallOptions> ArrowFlightConnection::getOptions() const
{
    std::lock_guard lock{mutex};
    connect();
    return options;
}

void ArrowFlightConnection::connect() const
{
    if (client)
        return;

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
    client = std::move(client_result).ValueOrDie();

    auto res_options = std::make_shared<arrow::flight::FlightCallOptions>();
    options = res_options;

    if (use_basic_authentication)
    {
        auto auth_result = client->AuthenticateBasicToken({}, username, password);
        if (!auth_result.ok())
        {
            throw Exception(
                ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to authenticate Arrow Flight server: {}", auth_result.status().ToString());
        }
        auto auth_token = std::move(auth_result).ValueOrDie();
        res_options->headers.push_back(auth_token);
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
