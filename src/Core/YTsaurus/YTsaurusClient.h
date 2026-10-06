#pragma once
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

#include <memory>
#include <unordered_map>
#include <utility>

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

    YTsaurusClient(const YTsaurusClient & other);

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

    ReadBufferPtr readTable(const String & cypress_path, const std::pair<size_t, size_t> & rows_range);

    ReadBufferPtr lookupRows(const String & cypress_path, const Block & lookup_block_input);

    ReadBufferPtr selectRows(const String & cypress_path, const String& column_names_str);

    ReadBufferPtr selectRows(const String & cypress_path, const ColumnsWithTypeAndName& columns);

    YTsaurusNodeType getNodeType(const String & cypress_path);

    String startTx(size_t timeout_ms);

    void commitTx(const String & transaction_id);

    String getNodeIdFromLock(const String & lock_id);

    String lock(const String & cypress_path, const String & transaction_id);

    size_t getTableNumberOfRows(const String& table_path);

    struct SchemaDescription
    {
        bool is_strict;
        std::unordered_map<String, DataTypePtr> columns;
    };

    SchemaDescription getTableSchema(const String & cypress_path);

    bool checkSchemaCompatibility(const String & table_path, const SharedHeader & sample_block, String & reason, bool allow_nullable);
private:
    Poco::JSON::Object::Ptr getNodeMetadata(const String & cypress_path);

    Poco::Dynamic::Var getNodeAttribute(const String & cypress_path, const String & attribute_name);

    YTsaurusNodeType getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr & json_ptr);

    Poco::Dynamic::Var getMetadata(const String & path);


    ReadBufferPtr createQueryRWBuffer(const URI& uri,  const ReadWriteBufferFromHTTP::OutStreamCallback& out_callback, const std::string & http_method);
    ReadBufferPtr executeQuery(YTsaurusQueryPtr query, const ReadWriteBufferFromHTTP::OutStreamCallback && out_callback = nullptr);

    URI getHeavyProxyURI(const URI& uri);

    ContextPtr context;

    const ConnectionInfo connection_info;
    LoggerPtr log;
    size_t recently_used_url_index = 0;
    constexpr static String LOCKS_STORAGE_CYPRESS_PATH = "//sys/locks";
};

using YTsaurusClientPtr = std::shared_ptr<YTsaurusClient>;

class YTsaurusTableLock
{
public:
    YTsaurusTableLock(YTsaurusClientPtr client_, const String& cypress_path_, size_t transaction_timeout_ms);
    String getNodePath() const
    {
        return node_cypress_path;
    }
    ~YTsaurusTableLock();
private:
    YTsaurusClientPtr client;
    String transaction_id;
    String lock_id;
    String node_cypress_path;
};

using YTsaurusTableLockPtr = std::shared_ptr<YTsaurusTableLock>;

}

#endif
