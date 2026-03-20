#include <Functions/AIEmbed/OpenAIProvider.h>

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

OpenAIProvider::OpenAIProvider(
    const String & endpoint_,
    const String & api_key_,
    size_t /*max_batch_size_*/,
    const String & api_version_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , api_version(api_version_)
{
}

std::vector<std::vector<Float32>> OpenAIProvider::doRequest(
    const String & model,
    const std::vector<std::string_view> & texts,
    size_t timeout_ms)
{
    Poco::URI uri(endpoint);

    /// Build JSON request body: {"model": "...", "input": ["text1", "text2", ...]}
    Poco::JSON::Object request_body;
    request_body.set("model", model);

    Poco::JSON::Array input_array;
    for (const auto & text : texts)
        input_array.add(std::string(text));
    request_body.set("input", input_array);

    std::ostringstream body_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body.stringify(body_stream);
    std::string body = body_stream.str();

    /// Set up timeouts (Poco::Timespan constructor takes seconds and microseconds)
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

    if (!api_version.empty())
        request.set("api-version", api_version);

    /// Send request
    auto & out_stream = session->sendRequest(request);
    out_stream.write(body.data(), body.size());

    /// Read response
    Poco::Net::HTTPResponse response;
    auto & in_stream = session->receiveResponse(response);

    std::string response_body;
    Poco::StreamCopier::copyToString(in_stream, response_body);

    auto status = response.getStatus();
    if (status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS)
    {
        throw Exception(
            ErrorCodes::AI_EMBED_RATE_LIMITED,
            "Embedding API rate limit exceeded. Provider 'openai' at '{}' returned HTTP 429: {}",
            endpoint, response_body);
    }

    if (status >= 400)
    {
        throw Exception(
            ErrorCodes::AI_EMBED_REQUEST_FAILED,
            "Embedding API request failed. Provider 'openai' at '{}' returned HTTP {}: {}",
            endpoint, static_cast<int>(status), response_body);
    }

    /// Parse response: {"data": [{"embedding": [0.1, 0.2, ...], "index": 0}, ...]}
    Poco::JSON::Parser parser;
    auto result = parser.parse(response_body);
    auto root = result.extract<Poco::JSON::Object::Ptr>();

    if (!root || !root->has("data"))
        throw Exception(
            ErrorCodes::AI_EMBED_INVALID_RESPONSE,
            "Invalid response from embedding API at '{}': missing 'data' field. Response: {}",
            endpoint, response_body.substr(0, 500));

    auto data_array = root->getArray("data");
    if (!data_array || data_array->size() != texts.size())
        throw Exception(
            ErrorCodes::AI_EMBED_INVALID_RESPONSE,
            "Invalid response from embedding API at '{}': expected {} embeddings, got {}",
            endpoint, texts.size(), data_array ? data_array->size() : 0);

    /// OpenAI returns embeddings potentially out of order, so sort by index
    std::vector<std::vector<Float32>> embeddings(texts.size());

    for (unsigned int i = 0; i < data_array->size(); ++i)
    {
        auto item = data_array->getObject(i);
        if (!item || !item->has("embedding") || !item->has("index"))
            throw Exception(
                ErrorCodes::AI_EMBED_INVALID_RESPONSE,
                "Invalid embedding item in response from '{}': missing 'embedding' or 'index' field",
                endpoint);

        size_t idx = item->getValue<size_t>("index");
        if (idx >= texts.size())
            throw Exception(
                ErrorCodes::AI_EMBED_INVALID_RESPONSE,
                "Invalid embedding index {} in response from '{}': out of range for batch of {} texts",
                idx, endpoint, texts.size());

        auto embedding_array = item->getArray("embedding");
        if (!embedding_array)
            throw Exception(
                ErrorCodes::AI_EMBED_INVALID_RESPONSE,
                "Invalid embedding data in response from '{}'",
                endpoint);

        std::vector<Float32> embedding;
        embedding.reserve(embedding_array->size());
        for (unsigned int j = 0; j < embedding_array->size(); ++j)
            embedding.push_back(static_cast<Float32>(embedding_array->getElement<double>(j)));

        embeddings[idx] = std::move(embedding);
    }

    return embeddings;
}

std::vector<std::vector<Float32>> OpenAIProvider::embed(
    const String & model,
    const std::vector<std::string_view> & texts,
    size_t timeout_ms,
    size_t max_retries)
{
    const auto log = getLogger("OpenAIProvider");

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
                        "Embedding API rate limit exceeded after {} retries. Provider 'openai' at '{}'. "
                        "Consider reducing query concurrency or waiting before retrying. Original error: {}",
                        max_retries, endpoint, e.message());
                throw;
            }

            /// Exponential backoff: 100ms, 400ms, 1600ms
            size_t backoff_ms = 100 * (1 << (2 * attempt));
            LOG_WARNING(log, "Embedding request to '{}' failed (attempt {}/{}), retrying in {}ms: {}",
                endpoint, attempt + 1, max_retries + 1, backoff_ms, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }

    UNREACHABLE();
}

}
