#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Common/BridgeProtocolVersion.h>

namespace DB
{

// Common base class to access the clickhouse-library-bridge.
class LibraryBridgeHelper : public IBridgeHelper
{
protected:
    explicit LibraryBridgeHelper(ContextPtr context_);

    void startBridge(std::unique_ptr<ShellCommand> cmd) const override;

    String serviceAlias() const override { return "clickhouse-library-bridge"; }

    String serviceFileName() const override { return serviceAlias(); }

    size_t getDefaultPort() const override { return DEFAULT_PORT; }

    bool startBridgeManually() const override { return false; }

    String configPrefix() const override { return "library_bridge"; }

    const Poco::Util::AbstractConfiguration & getConfig() const override { return config; }

    Poco::Logger * getLog() const override { return log; }

    Poco::Timespan getHTTPTimeout() const override { return http_timeout; }

    Poco::URI createBaseURI() const override;

    static constexpr inline size_t DEFAULT_PORT = 9012;

    const Poco::Util::AbstractConfiguration & config;
    Poco::Logger * log;
    const Poco::Timespan http_timeout;
    std::string bridge_host;
    size_t bridge_port;
    ConnectionTimeouts http_timeouts;
    Poco::Net::HTTPBasicCredentials credentials{};
};

}
