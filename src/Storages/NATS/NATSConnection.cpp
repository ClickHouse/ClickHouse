#include <Storages/NATS/NATSConnection.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <cstdlib>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

NATSClient::TLSSettings getTLSSettings(const NATSConfiguration & configuration)
{
    NATSClient::TLSSettings tls;
    tls.secure = configuration.secure;
    tls.ca_file = configuration.ca_file;
    tls.client_cert_file = configuration.client_cert_file;
    tls.client_key_file = configuration.client_key_file;

    /// Use `CLICKHOUSE_NATS_TLS_SECURE=0` to skip the verification of the server certificate.
    const char * value = std::getenv("CLICKHOUSE_NATS_TLS_SECURE"); // NOLINT(concurrency-mt-unsafe) // this is safe on Linux glibc/Musl, but potentially not safe on other platforms
    tls.verify_server = !(value && std::string_view(value) == "0");
    return tls;
}

}

NATSClient::ConnectionSettings NATSConfiguration::getConnectionSettings() const
{
    NATSClient::ConnectionSettings settings;

    /// Every address is validated and rebuilt by `StorageNATS` as a single one, so it is not split here:
    /// a password may contain a comma.
    if (!url.empty())
    {
        settings.servers.push_back(NATSClient::ServerAddress::parse(url));
    }
    else
    {
        for (const auto & server : servers)
            if (!server.empty())
                settings.servers.push_back(NATSClient::ServerAddress::parse(server));
    }
    if (settings.servers.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No NATS server is specified: set `nats_url` or `nats_server_list`");

    if (!username.empty() && !password.empty())
    {
        settings.authentication.user = username;
        settings.authentication.password = password;
    }
    settings.authentication.token = token;
    if (!credentials.empty())
    {
        settings.authentication.credentials = credentials;
    }
    else if (!credential_file.empty())
    {
        ReadBufferFromFile file(credential_file);
        String content;
        readStringUntilEOF(content, file);
        settings.authentication.credentials = SecretString(content);
    }

    settings.tls = getTLSSettings(*this);
    return settings;
}

void validateNATSCertificates(const NATSConfiguration & configuration)
{
    NATSClient::validateTLSSettings(getTLSSettings(configuration));
}

NATSConnection::NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_)
    : configuration(configuration_)
    , log(std::move(log_))
{
}

NATSConnection::~NATSConnection()
{
    disconnect();
}

String NATSConnection::connectionInfoForLog() const
{
    if (!configuration.url.empty())
        return "url: [hidden]";
    return "cluster: [hidden]";
}

String NATSConnection::lastErrorForLog() const
{
    if (connection && !connection->getError().empty())
        return connection->getError();
    return last_error.empty() ? "none" : last_error;
}

bool NATSConnection::isConnected()
{
    if (!connection)
        return false;
    if (connection->isUsable())
        return true;
    last_error = connection->getError();
    return false;
}

void NATSConnection::connect()
{
    disconnect();
    try
    {
        connection = std::make_unique<NATSClient::Connection>(configuration.getConnectionSettings(), log);
        last_error.clear();
    }
    catch (...)
    {
        last_error = getCurrentExceptionMessage(false);
        throw;
    }
}

void NATSConnection::disconnect()
{
    if (!connection)
        return;
    if (!connection->getError().empty())
        last_error = connection->getError();
    connection->close();
    connection.reset();
}

NATSClient::Connection & NATSConnection::get()
{
    if (!connection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no connection to NATS");
    return *connection;
}

}
