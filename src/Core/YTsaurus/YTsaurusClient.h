#pragma once

#include "config.h"

#if USE_YTSAURUS

#include "YTsaurusQueries.h"

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>


#include <boost/noncopyable.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{

enum class YTsaurusNodeType : uint8_t
{
    STATIC_TABLE = 0,
    DYNAMIC_TABLE,
    ANOTHER,
};

class YTsaurusClient : private boost::noncopyable
{
public:
    struct ConnectionInfo
    {
        String http_proxy_url;
        String oauth_token;
        String api_version = "v3";
    };

    explicit YTsaurusClient(ContextPtr context_, const ConnectionInfo & connection_info_);

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

    DB::ReadBufferPtr readTable(const String & cypress_path);

    DB::ReadBufferPtr selectRows(const String & cypress_path);

    YTsaurusNodeType getNodeType(const String & cypress_path);

private:

    YTsaurusNodeType getNodeTypeFromAttributes(Poco::JSON::Object::Ptr json_ptr);

    ReadBufferPtr execQuery(YTsaurusQueryPtr query);
    ContextPtr context;

    ConnectionInfo connection_info;
    LoggerPtr log;
};

using YTsaurusClientPtr = std::unique_ptr<YTsaurusClient>;

}

#endif
