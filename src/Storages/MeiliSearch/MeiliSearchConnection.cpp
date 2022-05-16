#include <sstream>
#include <string_view>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <Common/Exception.h>

#include <Poco/StreamCopier.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

MeiliSearchConnection::MeiliSearchConnection(const MeiliConfig & conf) : config{conf}
{
    Poco::URI uri(config.connection_string);
    session.setHost(uri.getHost());
    session.setPort(uri.getPort());
}

String MeiliSearchConnection::execPostQuery(const String & url, std::string_view post_fields) const
{
    Poco::URI uri(url);

    String path(uri.getPathAndQuery());
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest req(Poco::Net::HTTPRequest::HTTP_POST, path, Poco::Net::HTTPMessage::HTTP_1_1);
    req.setContentType("application/json");

    if (!config.key.empty())
        req.add("Authorization", "Bearer " + config.key);

    req.setContentLength(post_fields.length());

    std::ostream & os = session.sendRequest(req);
    os << post_fields;

    Poco::Net::HTTPResponse res;
    std::istream & is = session.receiveResponse(res);

    // need to separate MeiliSearch response from other situations
    // in order to handle it properly
    if (res.getStatus() / 100 == 2 || res.getStatus() / 100 == 4)
    {
        String response_buffer;
        Poco::StreamCopier::copyToString(is, response_buffer);
        return response_buffer;
    }
    else
        throw Exception(ErrorCodes::NETWORK_ERROR, res.getReason());
}

String MeiliSearchConnection::execGetQuery(const String & url, const std::unordered_map<String, String> & query_params) const
{
    Poco::URI uri(url);
    for (const auto & kv : query_params)
    {
        uri.addQueryParameter(kv.first, kv.second);
    }

    String path(uri.getPathAndQuery());
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest req(Poco::Net::HTTPRequest::HTTP_GET, path, Poco::Net::HTTPMessage::HTTP_1_1);

    if (!config.key.empty())
        req.add("Authorization", "Bearer " + config.key);

    session.sendRequest(req);

    Poco::Net::HTTPResponse res;
    std::istream & is = session.receiveResponse(res);

    // need to separate MeiliSearch response from other situations
    // in order to handle it properly
    if (res.getStatus() / 100 == 2 || res.getStatus() / 100 == 4)
    {
        String response_buffer;
        Poco::StreamCopier::copyToString(is, response_buffer);
        return response_buffer;
    }
    else
        throw Exception(ErrorCodes::NETWORK_ERROR, res.getReason());
}


String MeiliSearchConnection::searchQuery(const std::unordered_map<String, String> & query_params) const
{
    WriteBufferFromOwnString post_fields;

    post_fields << "{";

    auto it = query_params.begin();
    while (it != query_params.end())
    {
        post_fields << it->first << ":" << it->second;
        ++it;
        if (it != query_params.end())
            post_fields << ",";
    }

    post_fields << "}";

    String url = config.connection_string + "search";
    return execPostQuery(url, post_fields.str());
}

String MeiliSearchConnection::updateQuery(std::string_view data) const
{
    String url = config.connection_string + "documents";
    return execPostQuery(url, data);
}

String MeiliSearchConnection::getDocumentsQuery(const std::unordered_map<String, String> & query_params) const
{
    String url = config.connection_string + "documents";
    return execGetQuery(url, query_params);
}

}
