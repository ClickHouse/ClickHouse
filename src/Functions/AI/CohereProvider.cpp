#include <Functions/AI/CohereProvider.h>
#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int MALFORMED_AI_PROVIDER_RESPONSE;
}

CohereProvider::CohereProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , uri(endpoint_)
{
}

void CohereProvider::rerank(const AIRerankRequest & ai_rerank_request, const ConnectionTimeouts & timeouts, AIRerankResponse & response)
{
    response = {};

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", ai_rerank_request.model);
    root->set("query", ai_rerank_request.query);

    Poco::JSON::Array::Ptr documents = new Poco::JSON::Array;
    for (const auto & document : ai_rerank_request.documents)
        documents->add(String(document));
    root->set("documents", documents);

    if (ai_rerank_request.top_n > 0)
        root->set("top_n", static_cast<Int64>(ai_rerank_request.top_n)); /// Poco doesn't have UInt type

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = std::move(body_stream).str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty()) /// not all providers need API key
        http_request.set("Authorization", "Bearer " + api_key);
    chassert(!ai_rerank_request.function_name.empty());
    http_request.set("X-ClickHouse-AI-Function", ai_rerank_request.function_name);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    String response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = std::move(ss).str();
    }

    auto status = http_response.getStatus();
    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw AIProviderHTTPException(
            status,
            PreformattedMessage::create("AI provider error: {}", formatProviderError(static_cast<int>(status), response_body)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    /// A malformed body was still charged for, so read the billed units before the checks below can throw.
    if (json_obj->has("meta"))
    {
        auto meta = json_obj->getObject("meta");
        if (meta && meta->has("billed_units"))
        {
            auto billed_units = meta->getObject("billed_units");
            if (billed_units)
            {
                response.input_tokens = billed_units->optValue<UInt64>("input_tokens", 0);
                response.output_tokens = billed_units->optValue<UInt64>("output_tokens", 0);
                response.search_units = billed_units->optValue<UInt64>("search_units", 0);
            }
        }
    }

    auto results = json_obj->getArray("results");
    if (!results)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI rerank response is missing 'results' array");

    /// `results` holds exactly `top_n` entries when set (the caller keeps it within the number of
    /// documents), and one entry per document otherwise.
    chassert(ai_rerank_request.top_n <= ai_rerank_request.documents.size());
    const size_t expected_results = ai_rerank_request.top_n > 0 ? ai_rerank_request.top_n : ai_rerank_request.documents.size();
    if (results->size() != expected_results)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "AI rerank response 'results' has {} entries, expected {} ({} documents sent, 'top_n' = {})",
            results->size(), expected_results, ai_rerank_request.documents.size(), ai_rerank_request.top_n);

    /// Guards against a misbehaving provider returning an out-of-range or duplicate `index`.
    VectorWithMemoryTracking<bool> seen(ai_rerank_request.documents.size(), false);

    response.results.reserve(results->size());
    for (unsigned i = 0; i < results->size(); ++i)
    {
        auto item = results->getObject(i);
        if (!item)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI rerank response 'results[{}]' is not an object", i);

        if (!item->has("index"))
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI rerank response 'results[{}]' is missing 'index'", i);
        if (!item->has("relevance_score"))
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI rerank response 'results[{}]' is missing 'relevance_score'", i);

        UInt64 idx = item->getValue<UInt64>("index");
        if (idx >= ai_rerank_request.documents.size())
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI rerank response 'results[{}].index' = {} is out of range (expected < {})",
                i, idx, ai_rerank_request.documents.size());
        if (seen[idx])
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI rerank response 'results[{}].index' = {} duplicates an earlier entry", i, idx);
        seen[idx] = true;

        response.results.push_back(AIRerankResponse::Result{
            .index = static_cast<UInt32>(idx),
            .relevance_score = static_cast<Float32>(item->getValue<Float64>("relevance_score"))});
    }
}

}
