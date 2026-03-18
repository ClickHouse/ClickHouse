#pragma once

#include <Interpreters/Context.h>
#include <Common/ShellCommand.h>

#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Base class for server-side bridge helpers, e.g. xdbc-bridge and library-bridge.
/// Contains helper methods to check/start bridge sync
class IBridgeHelper: protected WithContext
{

public:
    static constexpr auto DEFAULT_HOST = "127.0.0.1";
    static constexpr auto DEFAULT_FORMAT = "RowBinary";
    static constexpr auto PING_OK_ANSWER = "Ok.";

    static const inline std::string PING_METHOD = Poco::Net::HTTPRequest::HTTP_GET;
    static const inline std::string MAIN_METHOD = Poco::Net::HTTPRequest::HTTP_POST;

    explicit IBridgeHelper(ContextPtr context_) : WithContext(context_) {}

    virtual ~IBridgeHelper() = default;

    virtual Poco::URI getMainURI() const = 0;

    virtual Poco::URI getPingURI() const = 0;

    void startBridgeSync();

protected:
    /// Check bridge is running. Can also check something else in the mean time.
    virtual bool bridgeHandShake() = 0;

    virtual String serviceAlias() const = 0;

    virtual String serviceFileName() const = 0;

    virtual unsigned getDefaultPort() const = 0;

    virtual bool startBridgeManually() const = 0;

    virtual void startBridge(std::unique_ptr<ShellCommand> cmd) const = 0;

    virtual String configPrefix() const = 0;

    virtual const Poco::Util::AbstractConfiguration & getConfig() const = 0;

    virtual LoggerPtr getLog() const = 0;

    virtual Poco::Timespan getHTTPTimeout() const = 0;

    virtual Poco::URI createBaseURI() const = 0;


private:
    std::unique_ptr<ShellCommand> startBridgeCommand();
};

}
