#include <Functions/AIEmbed/HuggingFaceTEIProvider.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int AI_EMBED_REQUEST_FAILED;
    extern const int AI_EMBED_INVALID_RESPONSE;
    extern const int AI_EMBED_RATE_LIMITED;
}

HuggingFaceTEIProvider::HuggingFaceTEIProvider(
    const String & endpoint_,
    const String & api_key_,
    size_t /*max_batch_size_*/)
    : endpoint(endpoint_)
    , api_key(api_key_)
{
}

std::vector<std::vector<Float32>> HuggingFaceTEIProvider::doRequest(
    const String & model,
    const std::vector<std::string_view> & texts,
    size_t timeout_ms)
{
    Poco::URI uri(endpoint);

    /// Build JSON request body: {"inputs": ["text1", "text2", ...]}
    /// TEI also accepts optional "model" field
    Poco::JSON::Object request_body;

    Poco::JSON::Array input_array;
    for (const auto & text : texts)
        input_array.add(std::string(text));
    request_body.set("inputs", input_array);

    if (!model.empty())
        request_body.set("model", model);

    std::ostringstream body_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body_stream);
    std::string body = body_stream.str();

    Poco::Timespan timeout_span(timeout_ms / 1000, (timeout_ms % 1000) * 1000);
    ConnectionTimeouts timeouts = ConnectionTimeouts()
        .withConnectionTimeout(timeout_span)
        .withSendTimeout(timeout_span)
        .withReceiveTimeout(timeout_span);

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts);

    String path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, path, Poco::Net::HTTPRequest::HTTP_1_1);
    request.setContentType("application/json");
    request.setContentLength(static_cast<std::streamsize>(body.size()));

    if (!api_key.empty())
        request.set("Authorization", "Bearer " + api_key);

    auto & out_stream = session->sendRequest(request);
    out_stream.write(body.data(), body.size());

    Poco::Net::HTTPResponse response;
    auto & in_stream = session->receiveResponse(response);

    std::string response_body;
    Poco::StreamCopier::copyToString(in_stream, response_body);

    auto status = response.getStatus();
    if (status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS)
    {
        throw Exception(
            ErrorCodes::AI_EMBED_RATE_LIMITED,
            "Embedding API rate limit exceeded. Provider 'huggingface_tei' at '{}' returned HTTP 429: {}",
            endpoint, response_body);
    }

    if (status >= 400)
    {
        throw Exception(
            ErrorCodes::AI_EMBED_REQUEST_FAILED,
            "Embedding API request failed. Provider 'huggingface_tei' at '{}' returned HTTP {}: {}",
            endpoint, static_cast<int>(status), response_body);
    }

    /// Parse response: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
    Poco::JSON::Parser parser;
    auto result = parser.parse(response_body);

    /// TEI returns a flat array of arrays
    auto outer_array = result.extract<Poco::JSON::Array::Ptr>();
    if (!outer_array || outer_array->size() != texts.size())
        throw Exception(
            ErrorCodes::AI_EMBED_INVALID_RESPONSE,
            "Invalid response from TEI at '{}': expected {} embeddings, got {}",
            endpoint, texts.size(), outer_array ? outer_array->size() : 0);

    std::vector<std::vector<Float32>> embeddings;
    embeddings.reserve(texts.size());

    for (unsigned int i = 0; i < outer_array->size(); ++i)
    {
        auto inner_array = outer_array->getArray(i);
        if (!inner_array)
            throw Exception(
                ErrorCodes::AI_EMBED_INVALID_RESPONSE,
                "Invalid embedding at index {} in response from TEI at '{}'",
                i, endpoint);

        std::vector<Float32> embedding;
        embedding.reserve(inner_array->size());
        for (unsigned int j = 0; j < inner_array->size(); ++j)
            embedding.push_back(static_cast<Float32>(inner_array->getElement<double>(j)));

        embeddings.push_back(std::move(embedding));
    }

    return embeddings;
}

std::vector<std::vector<Float32>> HuggingFaceTEIProvider::embed(
    const String & model,
    const std::vector<std::string_view> & texts,
    size_t timeout_ms,
    size_t max_retries)
{
    const auto log = getLogger("HuggingFaceTEIProvider");

    for (size_t attempt = 0; attempt <= max_retries; ++attempt)
    {
        try
        {
            return doRequest(model, texts, timeout_ms);
        }
        catch (const Exception & e)
        {
            bool is_rate_limited = (e.code() == ErrorCodes::AI_EMBED_RATE_LIMITED);
            bool is_retryable = is_rate_limited || (e.code() == ErrorCodes::AI_EMBED_REQUEST_FAILED);

            if (!is_retryable || attempt == max_retries)
            {
                if (is_rate_limited)
                    throw Exception(
                        ErrorCodes::AI_EMBED_RATE_LIMITED,
                        "Embedding API rate limit exceeded after {} retries. Provider 'huggingface_tei' at '{}'. "
                        "Consider reducing query concurrency or waiting before retrying. Original error: {}",
                        max_retries, endpoint, e.message());
                throw;
            }

            size_t backoff_ms = 100 * (1 << (2 * attempt));
            LOG_WARNING(log, "TEI embedding request to '{}' failed (attempt {}/{}), retrying in {}ms: {}",
                endpoint, attempt + 1, max_retries + 1, backoff_ms, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }

    UNREACHABLE();
}

}
