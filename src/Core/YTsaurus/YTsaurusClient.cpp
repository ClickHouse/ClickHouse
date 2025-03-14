#include "config.h"

#if USE_YTSAURUS

#include "YTsaurusClient.h"

#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/JSON/Parser.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

namespace ytsaurus
{

YTsaurusClient::YTsaurusClient(const ConnectionInfo & connection_info_, size_t num_tries_)
    : connection_info(connection_info_), num_tries(num_tries_), log(getLogger("YTsaurusClient"))
{
}


DB::ReadBufferPtr YTsaurusClient::readTable(const String & path)
{
    YTsaurusQueryPtr read_table_query(new YTsaurusReadTableQuery(path));
    return execQuery(read_table_query);
}

YTsaurusNodeType YTsaurusClient::getNodeType(const String & path)
{
    String attributes_path = path + "/@";
    YTsaurusQueryPtr get_query(new YTsaurusGetQuery(attributes_path));
    auto buf = execQuery(get_query);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & json_ptr = json.extract<Poco::JSON::Object::Ptr>();
    return getNodeTypeFromAttributes(json_ptr);
}


YTsaurusNodeType YTsaurusClient::getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr json_ptr)
{
    if (!json_ptr->has("type"))
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'type'.");

    if (json_ptr->getValue<String>("type") == "table")
    {
        if (!json_ptr->has("dynamic"))
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'dynamic'.");

        return json_ptr->getValue<bool>("dynamic") ? YTsaurusNodeType::DYNAMIC_TABLE : YTsaurusNodeType::STATIC_TABLE;
    }
    else
    {
        return YTsaurusNodeType::ANOTHER;
    }
}

DB::ReadBufferPtr YTsaurusClient::execQuery(const YTsaurusQueryPtr query)
{
    Poco::URI uri(connection_info.base_uri.c_str());
    uri.setPath(fmt::format("/api/{}/{}", connection_info.api_version, query->getQueryName()));

    for (const auto & query_param : query->getQueryParameters())
    {
        uri.addQueryParameter(query_param.name, query_param.value);
    }

    DB::HTTPHeaderEntries http_headers{
        {"Accept", "application/json"},
        {"Authorization", fmt::format("OAuth {}", connection_info.auth_token)},
    };

    /// RN i don't know how to not keep whole table in memory in general case.
    /// For yt dynamic tables we can make smth like paginator, but what about static tables?
    /// Does any one using big static tables?
    ///

    LOG_TRACE(log, "URI {} , query type {}", uri.toString(), query->getQueryName());
    Poco::Net::HTTPBasicCredentials creds;
    auto buf = DB::BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(DB::HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
                   .withHeaders(http_headers)
                   .create(creds);

    return DB::ReadBufferPtr(std::move(buf));
}

}
#endif
