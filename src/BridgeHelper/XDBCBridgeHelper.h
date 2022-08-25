#pragma once

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessType.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/BridgeProtocolVersion.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <base/range.h>
#include <BridgeHelper/IBridgeHelper.h>

#include <Common/config.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Class for Helpers for XDBC-bridges, provide utility methods, not main request.
class IXDBCBridgeHelper : public IBridgeHelper
{

public:
    explicit IXDBCBridgeHelper(ContextPtr context_) : IBridgeHelper(context_) {}

    virtual std::vector<std::pair<std::string, std::string>> getURLParams(UInt64 max_block_size) const = 0;

    virtual Poco::URI getColumnsInfoURI() const = 0;

    virtual IdentifierQuotingStyle getIdentifierQuotingStyle() = 0;

    virtual bool isSchemaAllowed() = 0;

    virtual String getName() const = 0;
};

using BridgeHelperPtr = std::shared_ptr<IXDBCBridgeHelper>;


template <typename BridgeHelperMixin>
class XDBCBridgeHelper : public IXDBCBridgeHelper
{

public:
    static constexpr inline auto DEFAULT_PORT = BridgeHelperMixin::DEFAULT_PORT;
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto COL_INFO_HANDLER = "/columns_info";
    static constexpr inline auto IDENTIFIER_QUOTE_HANDLER = "/identifier_quote";
    static constexpr inline auto SCHEMA_ALLOWED_HANDLER = "/schema_allowed";

    XDBCBridgeHelper(
            ContextPtr context_,
            Poco::Timespan http_timeout_,
            const std::string & connection_string_)
        : IXDBCBridgeHelper(context_->getGlobalContext())
        , log(&Poco::Logger::get(BridgeHelperMixin::getName() + "BridgeHelper"))
        , connection_string(connection_string_)
        , http_timeout(http_timeout_)
        , config(context_->getGlobalContext()->getConfigRef())
    {
        bridge_host = config.getString(BridgeHelperMixin::configPrefix() + ".host", DEFAULT_HOST);
        bridge_port = config.getUInt(BridgeHelperMixin::configPrefix() + ".port", DEFAULT_PORT);
    }

protected:
    Poco::URI getPingURI() const override
    {
        auto uri = createBaseURI();
        uri.setPath(PING_HANDLER);
        return uri;
    }


    Poco::URI getMainURI() const override
    {
        auto uri = createBaseURI();
        uri.setPath(MAIN_HANDLER);
        uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
        return uri;
    }


    bool bridgeHandShake() override
    {
        try
        {
            ReadWriteBufferFromHTTP buf(getPingURI(), Poco::Net::HTTPRequest::HTTP_GET, {}, ConnectionTimeouts::getHTTPTimeouts(getContext()), credentials);
            return checkString(PING_OK_ANSWER, buf);
        }
        catch (...)
        {
            return false;
        }
    }

    auto getConnectionString() const { return connection_string; }

    String getName() const override { return BridgeHelperMixin::getName(); }

    size_t getDefaultPort() const override { return DEFAULT_PORT; }

    String serviceAlias() const override { return BridgeHelperMixin::serviceAlias(); }

    /// Same for odbc and jdbc
    String serviceFileName() const override { return "clickhouse-odbc-bridge"; }

    String configPrefix() const override { return BridgeHelperMixin::configPrefix(); }

    Poco::Timespan getHTTPTimeout() const override { return http_timeout; }

    const Poco::Util::AbstractConfiguration & getConfig() const override { return config; }

    Poco::Logger * getLog() const override { return log; }

    bool startBridgeManually() const override { return BridgeHelperMixin::startBridgeManually(); }

    Poco::URI createBaseURI() const override
    {
        Poco::URI uri;
        uri.setHost(bridge_host);
        uri.setPort(bridge_port);
        uri.setScheme("http");
        return uri;
    }

    void startBridge(std::unique_ptr<ShellCommand> cmd) const override
    {
        getContext()->addBridgeCommand(std::move(cmd));
    }


private:
    using Configuration = Poco::Util::AbstractConfiguration;

    Poco::Logger * log;
    std::string connection_string;
    Poco::Timespan http_timeout;
    std::string bridge_host;
    size_t bridge_port;

    const Configuration & config;

    std::optional<IdentifierQuotingStyle> quote_style;
    std::optional<bool> is_schema_allowed;

    Poco::Net::HTTPBasicCredentials credentials{};


protected:
    using URLParams = std::vector<std::pair<std::string, std::string>>;

    Poco::URI getColumnsInfoURI() const override
    {
        auto uri = createBaseURI();
        uri.setPath(COL_INFO_HANDLER);
        uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
        return uri;
    }

    URLParams getURLParams(UInt64 max_block_size) const override
    {
        std::vector<std::pair<std::string, std::string>> result;

        result.emplace_back("connection_string", connection_string); /// already validated
        result.emplace_back("max_block_size", std::to_string(max_block_size));

        return result;
    }

    bool isSchemaAllowed() override
    {
        if (!is_schema_allowed.has_value())
        {
            startBridgeSync();

            auto uri = createBaseURI();
            uri.setPath(SCHEMA_ALLOWED_HANDLER);
            uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
            uri.addQueryParameter("connection_string", getConnectionString());

            ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(getContext()), credentials);

            bool res;
            readBoolText(res, buf);
            is_schema_allowed = res;
        }

        return *is_schema_allowed;
    }

    IdentifierQuotingStyle getIdentifierQuotingStyle() override
    {
        if (!quote_style.has_value())
        {
            startBridgeSync();

            auto uri = createBaseURI();
            uri.setPath(IDENTIFIER_QUOTE_HANDLER);
            uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
            uri.addQueryParameter("connection_string", getConnectionString());

            ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(getContext()), credentials);

            std::string character;
            readStringBinary(character, buf);
            if (character.length() > 1)
                throw Exception(
                    "Failed to parse quoting style from '" + character + "' for service " + BridgeHelperMixin::serviceAlias(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else if (character.length() == 0)
                quote_style = IdentifierQuotingStyle::None;
            else if (character[0] == '`')
                quote_style = IdentifierQuotingStyle::Backticks;
            else if (character[0] == '"')
                quote_style = IdentifierQuotingStyle::DoubleQuotes;
            else
                throw Exception("Can not map quote identifier '" + character + "' to enum value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return *quote_style;
    }
};


struct JDBCBridgeMixin
{
    static constexpr inline auto DEFAULT_PORT = 9019;

    static String configPrefix()
    {
        return "jdbc_bridge";
    }

    static String serviceAlias()
    {
        return "clickhouse-jdbc-bridge";
    }

    static String getName()
    {
        return "JDBC";
    }

    static AccessType getSourceAccessType()
    {
        return AccessType::JDBC;
    }

    static bool startBridgeManually()
    {
        return true;
    }
};


struct ODBCBridgeMixin
{
    static constexpr inline auto DEFAULT_PORT = 9018;

    static String configPrefix()
    {
        return "odbc_bridge";
    }

    static String serviceAlias()
    {
        return "clickhouse-odbc-bridge";
    }

    static String getName()
    {
        return "ODBC";
    }

    static AccessType getSourceAccessType()
    {
        return AccessType::ODBC;
    }

    static bool startBridgeManually()
    {
        return false;
    }
};

}
