#include <Common/ODBCBridgeHelper.h>

#include <sstream>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <common/logger_useful.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
}

ODBCBridgeHelper::ODBCBridgeHelper(
    const Configuration & config_, const Poco::Timespan & http_timeout_, const std::string & connection_string_)
    : config(config_), http_timeout(http_timeout_), connection_string(connection_string_)
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", DEFAULT_HOST);

    ping_url.setHost(bridge_host);
    ping_url.setPort(bridge_port);
    ping_url.setScheme("http");
    ping_url.setPath(PING_HANDLER);
}

void ODBCBridgeHelper::startODBCBridge() const
{
    Poco::Path path{config.getString("application.dir", "")};
    path.setFileName("clickhouse-odbc-bridge");

    if (!Poco::File(path).exists())
        throw Exception("clickhouse binary is not found", ErrorCodes::EXTERNAL_EXECUTABLE_NOT_FOUND);

    std::stringstream command;
    command << path.toString() << " ";
    command << "--http-port " << config.getUInt("odbc_bridge.port", DEFAULT_PORT) << ' ';
    command << "--listen-host " << config.getString("odbc_bridge.listen_host", DEFAULT_HOST) << ' ';
    command << "--http-timeout " << http_timeout.totalMicroseconds() << ' ';
    if (config.has("logger.odbc_bridge_log"))
        command << "--log-path " << config.getString("logger.odbc_bridge_log") << ' ';
    if (config.has("logger.odbc_bridge_errlog"))
        command << "--err-log-path " << config.getString("logger.odbc_bridge_errlog") << ' ';
    if (config.has("logger.odbc_bridge_level"))
        command << "--log-level " << config.getString("logger.odbc_bridge_level") << ' ';
    command << "&"; /// we don't want to wait this process

    auto command_str = command.str();
    LOG_TRACE(log, "Starting clickhouse-odbc-bridge with command: " << command_str);

    auto cmd = ShellCommand::execute(command_str);
    cmd->wait();
}

std::vector<std::pair<std::string, std::string>> ODBCBridgeHelper::getURLParams(const std::string & cols, size_t max_block_size) const
{
    std::vector<std::pair<std::string, std::string>> result;

    result.emplace_back("connection_string", connection_string); /// already validated
    result.emplace_back("columns", cols);
    result.emplace_back("max_block_size", std::to_string(max_block_size));

    return result;
}

bool ODBCBridgeHelper::checkODBCBridgeIsRunning() const
{
    try
    {
        ReadWriteBufferFromHTTP buf(ping_url, Poco::Net::HTTPRequest::HTTP_GET, nullptr);
        return checkString(ODBCBridgeHelper::PING_OK_ANSWER, buf);
    }
    catch (...)
    {
        return false;
    }
}

void ODBCBridgeHelper::startODBCBridgeSync() const
{
    if (!checkODBCBridgeIsRunning())
    {
        LOG_TRACE(log, "clickhouse-odbc-bridge is not running, will try to start it");
        startODBCBridge();
        bool started = false;
        for (size_t counter : ext::range(1, 20))
        {
            LOG_TRACE(log, "Checking clickhouse-odbc-bridge is running, try " << counter);
            if (checkODBCBridgeIsRunning())
            {
                started = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!started)
            throw Exception("ODBCBridgeHelper: clickhouse-odbc-bridge is not responding", ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING);
    }
}

Poco::URI ODBCBridgeHelper::getMainURI() const
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", DEFAULT_HOST);

    Poco::URI main_uri;
    main_uri.setHost(bridge_host);
    main_uri.setPort(bridge_port);
    main_uri.setScheme("http");
    main_uri.setPath(MAIN_HANDLER);
    return main_uri;
}

Poco::URI ODBCBridgeHelper::getColumnsInfoURI() const
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", DEFAULT_HOST);

    Poco::URI columns_info_uri;
    columns_info_uri.setHost(bridge_host);
    columns_info_uri.setPort(bridge_port);
    columns_info_uri.setScheme("http");
    columns_info_uri.setPath(COL_INFO_HANDLER);
    return columns_info_uri;
}
}
