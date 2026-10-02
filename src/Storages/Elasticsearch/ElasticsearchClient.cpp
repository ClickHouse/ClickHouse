#include <IO/HTTPCommon.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <IO/copyData.h>

#include <base/types.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
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

ElasticsearchClient::IndexPage ElasticsearchClient::searchIndex(bool fetch_source)
{
    static constexpr std::string_view search = "_search";
    static constexpr std::string_view parameters = "?allow_partial_search_results=false"; 

    if (pit_id.empty())
        setPointInTime();

    const auto uri = Poco::URI(fmt::format("{}/{}{}", config.url, search, parameters));

    Poco::JSON::Object request_body;

    request_body.set("size", config.page_size);

    Poco::JSON::Object point_in_time;
    point_in_time.set("id", pit_id);
    point_in_time.set("keep_alive", config.keep_alive);

    request_body.set("pit", point_in_time);

    Poco::JSON::Array sort;
    Poco::JSON::Object shard_doc;
    shard_doc.set("_shard_doc", "asc");
    sort.add(shard_doc);
    request_body.set("sort", sort);

    if (last_document_order_no)
        request_body.set("search_after", last_document_order_no);

    request_body.set("track_total_hits", false);

    Poco::JSON::Object query;

    query.set("match_all", Poco::JSON::Object());
    request_body.set("query", query);
    if (!fetch_source)
        request_body.set("_source", false);

    std::ostringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body);
    auto response_json = sendRequestToElastic(Poco::Net::HTTPRequest::HTTP_POST, uri, body.str());

    validateResponse(response_json);

    if (response_json->optValue<bool>("timed_out", false))
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Elasticsearch request timed out"); 

    auto hits = response_json->getObject("hits");
    if (!hits)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits' object: {}", responseExcerpt(response_json));

    auto hits_array = hits->getArray("hits");
    if (!hits_array)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits.hits' array: {}", responseExcerpt(response_json));

    pit_id = response_json->optValue<String>("pit_id", "");

    if (pit_id.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'pit_id': {}", responseExcerpt(response_json));

    if (hits_array->size() == 0)
        return hits_array;

    auto last_document = hits_array->getObject(static_cast<unsigned int>(hits_array->size()) - 1);
    last_document_order_no = last_document->getArray("sort");

    return hits_array;
}

void ElasticsearchClient::setPointInTime()
{
    static constexpr std::string_view point_in_time = "_pit";
    const String parameters = fmt::format("?keep_alive={}&allow_no_indices=false", config.keep_alive);
    const auto uri = Poco::URI(fmt::format("{}/{}/{}{}", config.url, config.index, point_in_time, parameters));
    auto response_json = sendRequestToElastic(Poco::Net::HTTPRequest::HTTP_POST, uri, "");

    validateResponse(response_json);

    pit_id = response_json->optValue<String>("id", "");

    if (pit_id.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'pit_id': {}", responseExcerpt(response_json));

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

void ElasticsearchClient::validateResponse(Poco::JSON::Object::Ptr response_json) const
{
    auto error = response_json->getObject("error");

    if (error)
    {
        auto response_code = response_json->optValue<Int64>("status", 0);
        auto type = error->getValue<String>("type");
        String reason = error->optValue<String>("reason", "");

        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Elasticsearch returned an error: (HTTP {}): type: {} reason: {}", response_code, type, reason); 
    }

    auto shards = response_json->getObject("_shards");
    if (!shards)
        return;

    auto failed = shards->getValue<Int64>("failed");
    if (failed == 0)
        return;

    String reason;
    if (auto failures = shards->getArray("failures"); failures && failures->size() > 0)
    {
        auto failure = failures->getObject(0);
        if (auto inner = failure->getObject("reason"))
            reason = inner->optValue<String>("reason", "");
    }
    
    throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
    "Elasticsearch operation failed on {} of {} shards, first failure: {}", failed, shards->getValue<Int64>("total"), reason);
}


}
