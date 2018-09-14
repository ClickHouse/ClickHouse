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
/** Helper for odbc-bridge, provide utility methods, not main request
  */
class ODBCBridgeHelper
{
private:

    using Configuration = Poco::Util::AbstractConfiguration;

    const Configuration & config;
    Poco::Timespan http_timeout;

    std::string connection_string;

    Poco::URI ping_url;

    Poco::Logger * log = &Poco::Logger::get("ODBCBridgeHelper");

public:
    static constexpr inline size_t DEFAULT_PORT = 9018;

    static constexpr inline auto DEFAULT_HOST = "localhost";
    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto COL_INFO_HANDLER = "/columns_info";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    ODBCBridgeHelper(const Configuration & config_, const Poco::Timespan & http_timeout_, const std::string & connection_string_);

    std::vector<std::pair<std::string, std::string>> getURLParams(const std::string & cols, size_t max_block_size) const;
    bool checkODBCBridgeIsRunning() const;

    void startODBCBridge() const;
    void startODBCBridgeSync() const;

    Poco::URI getMainURI() const;
    Poco::URI getColumnsInfoURI() const;
};
}
