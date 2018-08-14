#include <Common/ODBCBridgeHelper.h>

#include <sstream>
#include <Common/validateODBCConnectionString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
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
ODBCBridgeHelper::ODBCBridgeHelper(const Context & context_global_, const std::string & connection_string_)
    : context_global(context_global_), connection_string(validateODBCConnectionString(connection_string_))
{
    const auto & config = context_global.getConfigRef();
    size_t bridge_port = config.getUInt("odbc_bridge.port", DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", DEFAULT_HOST);

    ping_url.setHost(bridge_host);
    ping_url.setPort(bridge_port);
    ping_url.setScheme("http");
    ping_url.setPath(PING_HANDLER);
}
void ODBCBridgeHelper::startODBCBridge() const
{
    const auto & config = context_global.getConfigRef();
    const auto & settings = context_global.getSettingsRef();
    Poco::Path path{config.getString("application.dir", "")};
    path.setFileName("clickhouse-odbc-bridge");

    if (!path.isFile())
        throw Exception("clickhouse-odbc-bridge is not found", ErrorCodes::EXTERNAL_EXECUTABLE_NOT_FOUND);

    std::stringstream command;
    command << path.toString() << ' ';
    command << "--http-port " << config.getUInt("odbc_bridge.port", DEFAULT_PORT) << ' ';
    command << "--listen-host " << config.getString("odbc_bridge.listen_host", DEFAULT_HOST) << ' ';
    command << "--http-timeout " << settings.http_receive_timeout.value.totalSeconds() << ' ';
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

std::vector<std::pair<std::string, std::string>> ODBCBridgeHelper::getURLParams(const NamesAndTypesList & cols, size_t max_block_size) const
{
    std::vector<std::pair<std::string, std::string>> result;

    result.emplace_back("connection_string", connection_string); /// already validated
    result.emplace_back("columns", cols.toString());
    result.emplace_back("max_block_size", std::to_string(max_block_size));

    return result;
}

bool ODBCBridgeHelper::checkODBCBridgeIsRunning() const
{
    try
    {
        ReadWriteBufferFromHTTP buf(ping_url, ODBCBridgeHelper::PING_METHOD, nullptr);
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
}
