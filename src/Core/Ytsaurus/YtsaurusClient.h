#pragma once

#include "config.h"

#if USE_YTSAURUS

#include "YtsaurusQueries.h"

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Poco/URI.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>

#include <boost/noncopyable.hpp>

namespace Poco
{
class Logger;
}

namespace ytsaurus
{

const uint16_t DEFAULT_PROXY_PORT = 80;



enum class YtsaurusNodeType : uint8_t
{
    STATIC_TABLE = 0,
    DYNAMIC_TABLE, 
    ANOTHER,
};

class YtsaurusClient : private boost::noncopyable
{
public:

    struct ConnectionInfo
    {
        String base_uri;
        String auth_token;
        String api_version = "v3";
    };

    explicit YtsaurusClient(const ConnectionInfo & connection_info_, size_t num_tries = 3);

    const ConnectionInfo & getConnectionInfo() { return connection_info; }
    DB::ReadBufferPtr readTable(const String & path);

    YtsaurusNodeType getNodeType(const String & path);

private:

    YtsaurusNodeType getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr json_ptr);

    DB::ReadBufferPtr execQuery(const YtsaurusQueryPtr query);

    ConnectionInfo connection_info;
    [[maybe_unused]] size_t num_tries;

    LoggerPtr log;
};

using YtsaurusClientPtr = std::unique_ptr<YtsaurusClient>;

}

#endif
