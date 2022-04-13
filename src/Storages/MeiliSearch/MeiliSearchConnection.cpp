#include <sstream>
#include <string_view>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <Common/Exception.h>


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

void MeiliSearchConnection::execQuery(const String & url, std::string_view post_fields, std::string & response_buffer) const
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
        response_buffer = String(std::istreambuf_iterator<char>(is), {});
    else
        throw Exception(ErrorCodes::NETWORK_ERROR, res.getReason());
}

String MeiliSearchConnection::searchQuery(const std::unordered_map<String, String> & query_params) const
{
    std::string response_buffer;

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

    execQuery(url, post_fields.str(), response_buffer);

    return response_buffer;
}

String MeiliSearchConnection::updateQuery(std::string_view data) const
{
    String response_buffer;

    String url = config.connection_string + "documents";

    execQuery(url, data, response_buffer);

    return response_buffer;
}

}
