#pragma once
#include <unordered_map>
#include <utility>
#include "config.h"

#if USE_YTSAURUS

#include <Core/YTsaurus/YTsaurusQueries.h>
#include <Core/Block_fwd.h>
#include <Poco/JSON/Object.h>
#include <Storages/ColumnsDescription.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/URI.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <IO/ReadWriteBufferFromHTTP.h>


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
    using URI = Poco::URI;
    static const uint16_t HTTP_PROXY_DEFAULT_PORT = 80;

    struct ConnectionInfo
    {
        std::vector<String> http_proxy_urls;
        String oauth_token;
        String api_version = "v3";
        bool encode_utf8 = false;
        bool enable_heavy_proxy_redirection = true;
    };

    explicit YTsaurusClient(ContextPtr context_, const ConnectionInfo & connection_info_);

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

    ReadBufferPtr readTable(const String & cypress_path);

    ReadBufferPtr lookupRows(const String & cypress_path, const Block & lookup_block_input);

    ReadBufferPtr selectRows(const String & cypress_path);

    YTsaurusNodeType getNodeType(const String & cypress_path);

    struct SchemaDescription
    {
        bool is_strict;
        std::unordered_map<String, DataTypePtr> columns;
    };

    SchemaDescription getTableSchema(const String & cypress_path);

    bool checkSchemaCompatibility(const String & table_path, const SharedHeader & sample_block, String & reason);
private:
    Poco::JSON::Object::Ptr getTableInfo(const String & cypress_path);

    Poco::Dynamic::Var getTableAttribute(const String & cypress_path, const String & attribute_name);

    YTsaurusNodeType getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr & json_ptr);

    Poco::Dynamic::Var getMetadata(const String & path);


    ReadBufferPtr createQueryRWBuffer(const URI& uri,  const ReadWriteBufferFromHTTP::OutStreamCallback& out_callback, const std::string & http_method);
    ReadBufferPtr executeQuery(YTsaurusQueryPtr query, const ReadWriteBufferFromHTTP::OutStreamCallback && out_callback = nullptr);

    URI getHeavyProxyURI(const URI& uri);

    ContextPtr context;

    const ConnectionInfo connection_info;
    LoggerPtr log;
    size_t recently_used_url_index = 0;
};

using YTsaurusClientPtr = std::shared_ptr<YTsaurusClient>;

}

#endif
