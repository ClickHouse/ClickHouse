#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_EXECUTABLE_NOT_FOUND;
}
/** Base class for Helpers for Xdbc-bridges, provide utility methods, not main request
  */
class IXDBCBridgeHelper
{
private:
    Poco::Timespan http_timeout;

    std::string connection_string;

    Poco::URI ping_url;

    Poco::Logger * log = &Poco::Logger::get("XDBCBridgeHelper");

public:

    using Configuration = Poco::Util::AbstractConfiguration;

    const Configuration & config;

    static constexpr inline auto DEFAULT_HOST = "localhost";
    static constexpr inline auto DEFAULT_PORT = 0;
    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto COL_INFO_HANDLER = "/columns_info";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    IXDBCBridgeHelper(const Configuration & config_, const Poco::Timespan & http_timeout_, const std::string & connection_string_);

    virtual ~IXDBCBridgeHelper() {}

    std::vector<std::pair<std::string, std::string>> getURLParams(const std::string & cols, size_t max_block_size) const;

    /* External method for spawning bridge instance */
    void startBridgeSync() const;

    /* Data is fetched via this URI */
    Poco::URI getMainURI() const;

    /* A table schema is inferred via this URI */
    Poco::URI getColumnsInfoURI() const;

private:

    bool checkBridgeIsRunning() const;

    /* Contains logic for instantiation of the bridge instance */
    virtual void startBridge() const = 0;
};

    using BridgeHelperPtr = std::shared_ptr<IXDBCBridgeHelper>;
}