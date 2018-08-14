#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>

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
    const Context & context_global;

    std::string connection_string;

    Poco::URI ping_url;

    Poco::Logger * log = &Poco::Logger::get("ODBCBridgeHelper");

public:
    static constexpr inline size_t DEFAULT_PORT = 9018;

    static constexpr inline auto DEFAULT_HOST = "localhost";
    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_HANDLER = "/ping";
    static constexpr inline auto MAIN_HANDLER = "/";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    static const inline std::string PING_METHOD = Poco::Net::HTTPRequest::HTTP_GET;
    static const inline std::string MAIN_METHOD = Poco::Net::HTTPRequest::HTTP_POST;

    ODBCBridgeHelper(const Context & context_global_, const std::string & connection_string_);

    std::vector<std::pair<std::string, std::string>> getURLParams(const NamesAndTypesList & cols, size_t max_block_size) const;
    bool checkODBCBridgeIsRunning() const;

    void startODBCBridge() const;
    void startODBCBridgeSync() const;
};
}
