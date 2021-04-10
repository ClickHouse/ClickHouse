#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/ShellCommand.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <common/logger_useful.h>


namespace DB
{

/// Common base class for XDBC and Library bridge helpers.
/// Contains helper methods to check/start bridge sync.
class IBridgeHelper: protected WithContext
{

public:
    static constexpr inline auto DEFAULT_HOST = "127.0.0.1";
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    static const inline std::string PING_METHOD = Poco::Net::HTTPRequest::HTTP_GET;
    static const inline std::string MAIN_METHOD = Poco::Net::HTTPRequest::HTTP_POST;

    explicit IBridgeHelper(ContextPtr context_) : WithContext(context_) {}
    virtual ~IBridgeHelper() = default;

    void startBridgeSync() const;

    Poco::URI getMainURI() const;

    Poco::URI getPingURI() const;


protected:
    /// clickhouse-odbc-bridge, clickhouse-library-bridge
    virtual String serviceAlias() const = 0;

    virtual String serviceFileName() const = 0;

    virtual size_t getDefaultPort() const = 0;

    virtual bool startBridgeManually() const = 0;

    virtual void startBridge(std::unique_ptr<ShellCommand> cmd) const = 0;

    virtual String configPrefix() const = 0;

    virtual const Poco::Util::AbstractConfiguration & getConfig() const = 0;

    virtual Poco::Logger * getLog() const = 0;

    virtual const Poco::Timespan & getHTTPTimeout() const = 0;

    virtual Poco::URI createBaseURI() const = 0;


private:
    bool checkBridgeIsRunning() const;

    std::unique_ptr<ShellCommand> startBridgeCommand() const;
};

}
