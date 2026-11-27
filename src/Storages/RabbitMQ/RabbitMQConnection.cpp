#include <Storages/RabbitMQ/RabbitMQConnection.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AMQP::ConnectionSettings RabbitMQConfiguration::getConnectionSettings() const
{
    AMQP::ConnectionSettings settings;
    if (!connection_string.empty())
    {
        settings.address = AMQP::Address::parse(std::string_view(connection_string));
    }
    else
    {
        settings.address.host = host;
        settings.address.port = port;
        settings.address.user = username;
        settings.address.password = password;
        settings.address.vhost = vhost;
        settings.address.secure = secure;
    }
    settings.request_timeout = Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(RABBITMQ_REQUEST_TIMEOUT_MS) * 1000);
    return settings;
}

String RabbitMQConfiguration::getAddressForLog() const
{
    if (!connection_string.empty())
    {
        const auto address = AMQP::Address::parse(std::string_view(connection_string));
        return fmt::format("{}:{}", address.host, address.port);
    }
    return fmt::format("{}:{}", host, port);
}


RabbitMQConnection::RabbitMQConnection(const RabbitMQConfiguration & configuration_, LoggerPtr log_)
    : configuration(configuration_)
    , log(std::move(log_))
{
}

RabbitMQConnection::~RabbitMQConnection()
{
    disconnect();
}

bool RabbitMQConnection::isConnected()
{
    return connection && connection->isUsable();
}

void RabbitMQConnection::connect()
{
    disconnect();
    const auto settings = configuration.getConnectionSettings();
    LOG_DEBUG(log, "Connecting to RabbitMQ {} (user: {}, vhost: {})", connectionInfoForLog(), settings.address.user, settings.address.vhost);
    connection = std::make_unique<AMQP::Connection>(settings, log);
}

void RabbitMQConnection::disconnect()
{
    if (!connection)
        return;
    connection->close(Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(RABBITMQ_CLOSE_TIMEOUT_MS) * 1000));
    connection.reset();
}

AMQP::Connection & RabbitMQConnection::get()
{
    if (!connection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no connection to RabbitMQ {}", connectionInfoForLog());
    return *connection;
}

}
