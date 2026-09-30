#include <IO/HTTPCommon.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Net/HTTPRequest.h>
#include <Interpreters/Context.h>
#include <IO/copyData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

String responseExcerpt(const Poco::JSON::Object::Ptr & response)
{
    static constexpr size_t max_excerpt_size = 1024;

    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response->stringify(stream);
    String excerpt = stream.str();
    if (excerpt.size() > max_excerpt_size)
    {
        excerpt.resize(max_excerpt_size);
        excerpt += "...";
    }
    return excerpt;
}

Poco::JSON::Object::Ptr parseJSONObject(const String & data)
{
    try
    {
        Poco::JSON::Parser parser;
        auto object = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
        if (!object)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse Elasticsearch response: not a JSON object");
        return object;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse Elasticsearch response: {}", e.displayText());
    }
}

}

ElasticsearchClient::ElasticsearchClient(ElasticsearchConfiguration config_, ContextPtr context_)
    : config(config_)
    , context(context_)
{
}

ElasticsearchClient::IndexPage ElasticsearchClient::searchIndex(bool fetch_source) const
{
    const auto uri = Poco::URI(config.url + "/" + config.index + "/" + "_search");
    Poco::JSON::Object request_body;
    Poco::JSON::Object query;
    query.set("match_all", Poco::JSON::Object());
    request_body.set("query", query);
    if (!fetch_source)
        request_body.set("_source", false);

    std::ostringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body);
    auto response_json = sendRequestToElastic(Poco::Net::HTTPRequest::HTTP_POST, uri, body.str());

    auto hits = response_json->getObject("hits");
    if (!hits)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits' object: {}", responseExcerpt(response_json));

    auto hits_array = hits->getArray("hits");
    if (!hits_array)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits.hits' array: {}", responseExcerpt(response_json));

    return hits_array;
}

Poco::JSON::Object::Ptr ElasticsearchClient::sendRequestToElastic(const String & method, const Poco::URI & uri, const String & request_body) const
{
    HTTPHeaderEntries headers;
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
    if (!request_body.empty())
    {
        headers.emplace_back("Content-Type", "application/json");
        out_stream_callback = [&request_body](std::ostream & os) { os << request_body; };
    }
    auto buf = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(method)
        .withSettings(context->getReadSettings())
        .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withOutCallback(std::move(out_stream_callback))
        .create(credentials);

    WriteBufferFromOwnString response;
    copyData(*buf, response);
    response.finalize();

    return parseJSONObject(response.str());
}

}
