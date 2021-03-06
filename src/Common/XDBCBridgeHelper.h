#pragma once

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Access/AccessType.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include <Common/Bridge/BridgeHelper.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/**
 * Class for Helpers for Xdbc-bridges, provide utility methods, not main request
 */
class IXDBCBridgeHelper : public BridgeHelper
{

public:
    virtual std::vector<std::pair<std::string, std::string>> getURLParams(const std::string & cols, UInt64 max_block_size) const = 0;

    virtual Poco::URI getMainURI() const = 0;

    virtual Poco::URI getColumnsInfoURI() const = 0;

    virtual IdentifierQuotingStyle getIdentifierQuotingStyle() = 0;

    virtual bool isSchemaAllowed() = 0;

    virtual const String getName() const = 0;
};

using BridgeHelperPtr = std::shared_ptr<IXDBCBridgeHelper>;


template <typename BridgeHelperMixin>
class XDBCBridgeHelper : public IXDBCBridgeHelper
{

private:
    Poco::Timespan http_timeout;
    std::string connection_string;
    Poco::URI ping_url;
    Poco::Logger * log = &Poco::Logger::get(BridgeHelperMixin::getName() + "BridgeHelper");
    std::optional<IdentifierQuotingStyle> quote_style;
    std::optional<bool> is_schema_allowed;

protected:
    auto getConnectionString() const { return connection_string; }

public:
    using Configuration = Poco::Util::AbstractConfiguration;

    const Context & context;
    const Configuration & config;

    static constexpr inline auto DEFAULT_HOST = "127.0.0.1";
    static constexpr inline auto DEFAULT_PORT = BridgeHelperMixin::DEFAULT_PORT;
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto COL_INFO_HANDLER = "/columns_info";
    static constexpr inline auto IDENTIFIER_QUOTE_HANDLER = "/identifier_quote";
    static constexpr inline auto SCHEMA_ALLOWED_HANDLER = "/schema_allowed";


    XDBCBridgeHelper(const Context & global_context_, const Poco::Timespan & http_timeout_, const std::string & connection_string_)
        : http_timeout(http_timeout_), connection_string(connection_string_), context(global_context_), config(context.getConfigRef())
    {
        size_t bridge_port = config.getUInt(BridgeHelperMixin::configPrefix() + ".port", DEFAULT_PORT);
        std::string bridge_host = config.getString(BridgeHelperMixin::configPrefix() + ".host", DEFAULT_HOST);

        ping_url.setHost(bridge_host);
        ping_url.setPort(bridge_port);
        ping_url.setScheme("http");
        ping_url.setPath(PING_HANDLER);
    }


    const String getDefaultHost() const override
    {
        return DEFAULT_HOST;
    }

    size_t getDefaultPort() const override
    {
        return DEFAULT_PORT;
    }

    const String getName() const override
    {
        return BridgeHelperMixin::getName();
    }

    const String serviceAlias() const override
    {
        return BridgeHelperMixin::serviceAlias();
    }

    const String configPrefix() const override
    {
        return BridgeHelperMixin::configPrefix();
    }

    const Poco::URI & pingURL() const override
    {
        return ping_url;
    }

    const Context & getContext() const override
    {
        return context;
    }

    Poco::Logger * getLog() const override
    {
        return log;
    }

    const Poco::Util::AbstractConfiguration & getConfig() const override
    {
        return config;
    }

    const Poco::Timespan getHTTPTimeout() const override
    {
        return http_timeout;
    }

    bool startBridgeManually() const override
    {
        return BridgeHelperMixin::startBridgeManually();
    }

    void startBridge(std::unique_ptr<ShellCommand> cmd) const override
    {
        context.addXDBCBridgeCommand(std::move(cmd));
    }

    IdentifierQuotingStyle getIdentifierQuotingStyle() override
    {
        if (!quote_style.has_value())
        {
            startBridgeSync();

            auto uri = createBaseURI();
            uri.setPath(IDENTIFIER_QUOTE_HANDLER);
            uri.addQueryParameter("connection_string", getConnectionString());

            ReadWriteBufferFromHTTP buf(
                uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));
            std::string character;
            readStringBinary(character, buf);
            if (character.length() > 1)
                throw Exception("Failed to parse quoting style from '" + character + "' for service " + BridgeHelperMixin::serviceAlias(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

    bool isSchemaAllowed() override
    {
        if (!is_schema_allowed.has_value())
        {
            startBridgeSync();

            auto uri = createBaseURI();
            uri.setPath(SCHEMA_ALLOWED_HANDLER);
            uri.addQueryParameter("connection_string", getConnectionString());

            ReadWriteBufferFromHTTP buf(
                uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));

            bool res;
            readBoolText(res, buf);
            is_schema_allowed = res;
        }

        return *is_schema_allowed;
    }

    /**
     * @todo leaky abstraction - used by external API's
     */
    std::vector<std::pair<std::string, std::string>> getURLParams(const std::string & cols, UInt64 max_block_size) const override
    {
        std::vector<std::pair<std::string, std::string>> result;

        result.emplace_back("connection_string", connection_string); /// already validated
        result.emplace_back("columns", cols);
        result.emplace_back("max_block_size", std::to_string(max_block_size));

        return result;
    }

    /**
     * URI to fetch the data from external service
     */
    Poco::URI getMainURI() const override
    {
        auto uri = createBaseURI();
        uri.setPath(MAIN_HANDLER);
        return uri;
    }

    /**
     * URI to retrieve column description from external service
     */
    Poco::URI getColumnsInfoURI() const override
    {
        auto uri = createBaseURI();
        uri.setPath(COL_INFO_HANDLER);
        return uri;
    }

protected:
    Poco::URI createBaseURI() const
    {
        Poco::URI uri;
        uri.setHost(ping_url.getHost());
        uri.setPort(ping_url.getPort());
        uri.setScheme("http");
        return uri;
    }

};

struct JDBCBridgeMixin
{
    static constexpr inline auto DEFAULT_PORT = 9019;

    static const String configPrefix()
    {
        return "jdbc_bridge";
    }

    static const String serviceAlias()
    {
        return "clickhouse-jdbc-bridge";
    }

    static const String getName()
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

    static const String configPrefix()
    {
        return "odbc_bridge";
    }

    static const String serviceAlias()
    {
        return "clickhouse-odbc-bridge";
    }

    static const String getName()
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
