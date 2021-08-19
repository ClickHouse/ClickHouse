#include "IBridgeHelper.h"

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Poco/URI.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
}


Poco::URI IBridgeHelper::getMainURI() const
{
    auto uri = createBaseURI();
    uri.setPath(MAIN_HANDLER);
    return uri;
}


Poco::URI IBridgeHelper::getPingURI() const
{
    auto uri = createBaseURI();
    uri.setPath(PING_HANDLER);
    return uri;
}


bool IBridgeHelper::checkBridgeIsRunning() const
{
    try
    {
        ReadWriteBufferFromHTTP buf(
            getPingURI(), Poco::Net::HTTPRequest::HTTP_GET, {}, ConnectionTimeouts::getHTTPTimeouts(getContext()));
        return checkString(PING_OK_ANSWER, buf);
    }
    catch (...)
    {
        return false;
    }
}


void IBridgeHelper::startBridgeSync() const
{
    if (!checkBridgeIsRunning())
    {
        LOG_TRACE(getLog(), "{} is not running, will try to start it", serviceAlias());
        startBridge(startBridgeCommand());
        bool started = false;

        uint64_t milliseconds_to_wait = 10; /// Exponential backoff
        uint64_t counter = 0;

        while (milliseconds_to_wait < 10000)
        {
            ++counter;
            LOG_TRACE(getLog(), "Checking {} is running, try {}", serviceAlias(), counter);

            if (checkBridgeIsRunning())
            {
                started = true;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds_to_wait));
            milliseconds_to_wait *= 2;
        }

        if (!started)
            throw Exception("BridgeHelper: " + serviceAlias() + " is not responding",
                ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING);
    }
}


std::unique_ptr<ShellCommand> IBridgeHelper::startBridgeCommand() const
{
    if (startBridgeManually())
        throw Exception(serviceAlias() + " is not running. Please, start it manually", ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING);

    const auto & config = getConfig();
    /// Path to executable folder
    Poco::Path path{config.getString("application.dir", "/usr/bin")};

    std::vector<std::string> cmd_args;
    path.setFileName(serviceFileName());

    cmd_args.push_back("--http-port");
    cmd_args.push_back(std::to_string(config.getUInt(configPrefix() + ".port", getDefaultPort())));
    cmd_args.push_back("--listen-host");
    cmd_args.push_back(config.getString(configPrefix() + ".listen_host", DEFAULT_HOST));
    cmd_args.push_back("--http-timeout");
    cmd_args.push_back(std::to_string(getHTTPTimeout().totalMicroseconds()));
    if (config.has("logger." + configPrefix() + "_log"))
    {
        cmd_args.push_back("--log-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_log"));
    }
    if (config.has("logger." + configPrefix() + "_errlog"))
    {
        cmd_args.push_back("--err-log-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_errlog"));
    }
    if (config.has("logger." + configPrefix() + "_stdout"))
    {
        cmd_args.push_back("--stdout-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_stdout"));
    }
    if (config.has("logger." + configPrefix() + "_stderr"))
    {
        cmd_args.push_back("--stderr-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_stderr"));
    }
    if (config.has("logger." + configPrefix() + "_level"))
    {
        cmd_args.push_back("--log-level");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_level"));
    }

    LOG_TRACE(getLog(), "Starting {}", serviceAlias());

    return ShellCommand::executeDirect(path.toString(), cmd_args, ShellCommandDestructorStrategy(true));
}

}
