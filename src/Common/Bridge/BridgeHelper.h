#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <common/logger_useful.h>


namespace DB
{

class BridgeHelper
{

public:
    //static constexpr inline auto DEFAULT_FORMAT = "RowBinary";

    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    virtual const String serviceAlias() const = 0;

    virtual const String configPrefix() const = 0;

    virtual const Poco::URI & pingURL() const = 0;

    virtual const Context & getContext() const = 0;

    virtual Poco::Logger * getLog() const = 0;

    virtual const Poco::Util::AbstractConfiguration & getConfig() const = 0;

    virtual const String getDefaultHost() const = 0;

    virtual size_t getDefaultPort() const = 0;

    virtual bool startBridgeManually() const = 0;

    virtual void startBridge(std::unique_ptr<ShellCommand> cmd) const = 0;

    virtual const Poco::Timespan getHTTPTimeout() const = 0;

    void startBridgeSync() const;

    virtual ~BridgeHelper() = default;

private:
    bool checkBridgeIsRunning() const;

    std::unique_ptr<ShellCommand> startBridgeCommand() const;
};

}
