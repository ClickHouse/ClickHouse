#include <Common/JDBCBridgeHelper.h>

#include <sstream>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <Common/config.h>
#include <common/logger_useful.h>
#include <ext/range.h>


namespace DB
{

void JDBCBridgeHelper::startBridge() const {
    throw Exception("JDBC bridge does not support automatic instantiation");
}

JDBCBridgeHelper::JDBCBridgeHelper(const Configuration & config_,
        const Poco::Timespan & http_timeout_,
        const std::string & connection_string_) : IXDBCBridgeHelper(config_, http_timeout_, connection_string_),
        log(&Logger::get("JDBCBridge"))
{
    log = &Poco::Logger::get("ODBCBridgeHelper");
}
}
