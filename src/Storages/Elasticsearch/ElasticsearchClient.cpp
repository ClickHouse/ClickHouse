#include <IO/HTTPCommon.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>

#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <base/types.h>
#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event ElasticsearchPointInTimeCloseFailures;
}

namespace DB
{

namespace ElasticsearchSetting
{
    extern const ElasticsearchSettingsUInt64 page_size;
    extern const ElasticsearchSettingsSeconds keep_alive;
}

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

Poco::JSON::Object::Ptr readJSONObject(ReadBuffer & in)
{
    String data;
    readStringUntilEOF(data, in);
    return parseElasticsearchJSONObject(data);
}

}

ElasticsearchClient::ElasticsearchClient(ElasticsearchConfiguration config_, ContextPtr context_, const ElasticsearchSettings & es_settings)
    : page_size(es_settings[ElasticsearchSetting::page_size])
    , pit_keep_alive(fmt::format("{}s", es_settings[ElasticsearchSetting::keep_alive].totalSeconds()))
    , config(config_)
    , log(getLogger("ElasticsearchClient"))
    , context(context_)
    , read_settings(context_->getReadSettings())
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
    , format_settings(getFormatSettings(context_))
{
    if (config.auth_kind == ElasticsearchConfiguration::AuthKind::Basic)
    {
        credentials.setUsername(config.user);
        credentials.setPassword(config.password);
    }
    auth_headers = config.authorizationHeaders();
}

ElasticsearchClient::~ElasticsearchClient()
{
    if (pit_id.empty())
        return ;

    try
    {
        deletePointInTime();
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::ElasticsearchPointInTimeCloseFailures);
        tryLogCurrentException(log, "Cannot close Elasticsearch point in time");
    }

}

std::unique_ptr<ElasticsearchSearchResponse> ElasticsearchClient::searchIndex(const ElasticsearchHitColumns & hit_columns)
{
    static constexpr std::string_view search = "_search";
    static constexpr std::string_view parameters = "?allow_partial_search_results=false";

    if (pit_id.empty())
        setPointInTime();

    const auto uri = Poco::URI(fmt::format("{}/{}{}", config.url, search, parameters));

    Poco::JSON::Object request_body;

    request_body.set("size", page_size);

    Poco::JSON::Object point_in_time;
    point_in_time.set("id", pit_id);
    point_in_time.set("keep_alive", pit_keep_alive);

    request_body.set("pit", point_in_time);

    Poco::JSON::Array sort;
    Poco::JSON::Object shard_doc;
    shard_doc.set("_shard_doc", "asc");
    sort.add(shard_doc);
    request_body.set("sort", sort);

    if (last_document_order_no)
        request_body.set("search_after", last_document_order_no);

    request_body.set("track_total_hits", false);

    if (!hit_columns.source_pos)
        request_body.set("_source", false);

    std::ostringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body);
    String body_str = std::move(body).str();
    body_str.pop_back();
    body_str += fmt::format(R"(,"query":{}}})", config.query);
    auto buf = sendRequest(Poco::Net::HTTPRequest::HTTP_POST, uri, body_str, read_settings, timeouts);
    return std::make_unique<ElasticsearchSearchResponse>(std::move(buf), hit_columns, format_settings);
}

void ElasticsearchClient::finishPage(const ElasticsearchSearchResponse & response)
{
    pit_id = response.pitId();

    if (response.hitsRead() == 0)
        return;

    Poco::JSON::Array::Ptr sort_values;
    try
    {
        Poco::JSON::Parser parser;
        sort_values = parser.parse(response.lastSort()).extract<Poco::JSON::Array::Ptr>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse 'sort' of the last Elasticsearch hit: {}", e.displayText());
    }

    if (!sort_values || sort_values->size() != 1 || sort_values->isNull(0) || !sort_values->get(0).isInteger())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no valid 'sort' in the last hit: {}", response.lastSort());

    last_document_order_no = sort_values;
}

void ElasticsearchClient::setPointInTime()
{
    static constexpr std::string_view point_in_time = "_pit";

    String encoded_index;
    Poco::URI::encode(config.index, "/", encoded_index);
    const String parameters = fmt::format("?keep_alive={}&allow_no_indices=false", pit_keep_alive);
    const auto uri = Poco::URI(fmt::format("{}/{}/{}{}", config.url, encoded_index, point_in_time, parameters), /*enable_url_encoding=*/ false);
    auto response_json = readJSONObject(*sendRequest(Poco::Net::HTTPRequest::HTTP_POST, uri, "", read_settings, timeouts));

    validateElasticsearchShards(response_json->getObject("_shards"));

    pit_id = response_json->optValue<String>("id", "");

    if (pit_id.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no PIT ID: {}", responseExcerpt(response_json));

}

void ElasticsearchClient::deletePointInTime()
{
    const auto uri = Poco::URI(fmt::format("{}/_pit", config.url));

    Poco::JSON::Object request_body;
    request_body.set("id", pit_id);

    std::ostringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body);
    static constexpr size_t close_timeout_sec = 1;

    auto close_read_settings = read_settings;
    close_read_settings.http_settings.max_tries = 1;
    auto close_timeouts = timeouts;
    close_timeouts.withConnectionTimeout(close_timeout_sec).withSendTimeout(close_timeout_sec).withReceiveTimeout(close_timeout_sec);

    auto response = readJSONObject(*sendRequest(Poco::Net::HTTPRequest::HTTP_DELETE, uri, body.str(), close_read_settings, close_timeouts));

    if (!response->optValue<bool>("succeeded", false))
      throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Elasticsearch did not close the point in time");
}

std::unique_ptr<ReadBuffer> ElasticsearchClient::sendRequest(
    const String & method,
    const Poco::URI & uri,
    const String & request_body,
    const ReadSettings & request_read_settings,
    const ConnectionTimeouts & request_timeouts) const
{
    HTTPHeaderEntries headers = auth_headers;
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
    if (!request_body.empty())
    {
        headers.emplace_back("Content-Type", "application/json");
        out_stream_callback = [request_body](WriteBuffer & os) { writeString(request_body, os); };
    }
    return BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(method)
        .withSettings(request_read_settings)
        .withTimeouts(request_timeouts)
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withOutCallback(std::move(out_stream_callback))
        .create(credentials);
}

}
