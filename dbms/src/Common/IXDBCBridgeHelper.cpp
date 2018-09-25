#include <Common/IXDBCBridgeHelper.h>

#include <sstream>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <Common/config.h>
#include <common/logger_useful.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
}

IXDBCBridgeHelper::IXDBCBridgeHelper(
    const Configuration & config_, const Poco::Timespan & http_timeout_, const std::string & connection_string_)
    : http_timeout(http_timeout_), connection_string(connection_string_), config(config_)
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", DEFAULT_HOST);

    ping_url.setHost(bridge_host);
    ping_url.setPort(bridge_port);
    ping_url.setScheme("http");
    ping_url.setPath(PING_HANDLER);
}


std::vector<std::pair<std::string, std::string>> IXDBCBridgeHelper::getURLParams(const std::string & cols, size_t max_block_size) const
{
    std::vector<std::pair<std::string, std::string>> result;

    result.emplace_back("connection_string", connection_string); /// already validated
    result.emplace_back("columns", cols);
    result.emplace_back("max_block_size", std::to_string(max_block_size));

    return result;
}

bool IXDBCBridgeHelper::checkBridgeIsRunning() const
{
    try
    {
        ReadWriteBufferFromHTTP buf(ping_url, Poco::Net::HTTPRequest::HTTP_GET, nullptr);
        return checkString(IXDBCBridgeHelper::PING_OK_ANSWER, buf);
    }
    catch (...)
    {
        return false;
    }
}

void IXDBCBridgeHelper::startBridgeSync() const
{
    if (!checkBridgeIsRunning())
    {
        LOG_TRACE(log, "clickhouse-odbc-bridge is not running, will try to start it");
        startBridge();
        bool started = false;
        for (size_t counter : ext::range(1, 20))
        {
            LOG_TRACE(log, "Checking clickhouse-odbc-bridge is running, try " << counter);
            if (checkBridgeIsRunning())
            {
                started = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!started)
            throw Exception("IXDBCBridgeHelper: clickhouse-odbc-bridge is not responding", ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING);
    }
}

Poco::URI IXDBCBridgeHelper::getMainURI() const
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", ping_url.getPort());
    std::string bridge_host = config.getString("odbc_bridge.host", ping_url.getHost());

    Poco::URI main_uri;
    main_uri.setHost(bridge_host);
    main_uri.setPort(bridge_port);
    main_uri.setScheme("http");
    main_uri.setPath(MAIN_HANDLER);
    return main_uri;
}

Poco::URI IXDBCBridgeHelper::getColumnsInfoURI() const
{
    size_t bridge_port = config.getUInt("odbc_bridge.port", ping_url.getPort());
    std::string bridge_host = config.getString("odbc_bridge.host", ping_url.getHost());

    Poco::URI columns_info_uri;
    columns_info_uri.setHost(bridge_host);
    columns_info_uri.setPort(bridge_port);
    columns_info_uri.setScheme("http");
    columns_info_uri.setPath(COL_INFO_HANDLER);
    return columns_info_uri;
}
}
