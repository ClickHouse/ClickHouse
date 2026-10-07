#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Functions/AI/AnthropicProvider.h>
#include <Functions/AI/TypeSafeProvider.h>
#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPRequest.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

String sanitizeForLog(std::string_view input)
{
    String output;
    output.reserve(input.size());
    for (unsigned char ch : input)
        output.push_back((ch < 0x20 || ch == 0x7F) ? ' ' : static_cast<char>(ch));
    return output;
}

/// Prefers the structured `error.message`/`error.type` fields, falling back to the first 256
/// bytes of the body. Every part is drawn from the (external) response, so control characters are
/// replaced with spaces to stop the endpoint from forging log lines or corrupting a terminal.
String formatProviderError(int status_code, const String & response_body)
{
    try
    {
        Poco::JSON::Parser err_parser;
        auto err_json = err_parser.parse(response_body);
        auto err_obj = err_json.extract<Poco::JSON::Object::Ptr>();
        if (err_obj && err_obj->has("error"))
        {
            auto err = err_obj->getObject("error");
            if (err)
            {
                String msg = err->optValue<String>("message", "");
                String type = err->optValue<String>("type", "");
                if (!msg.empty())
                    return fmt::format("HTTP {} [{}]: {}", status_code, sanitizeForLog(type), sanitizeForLog(msg));
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch) Ok: fall through to the raw-body branch below.
    {
    }

    constexpr size_t max_len = 256;
    const std::string_view snippet = std::string_view(response_body).substr(0, max_len);
    return fmt::format("HTTP {}: {}", status_code, sanitizeForLog(snippet));
}

AIProviderHTTPException::AIProviderHTTPException(Poco::Net::HTTPResponse::HTTPStatus http_status_, PreformattedMessage msg)
    : Exception(std::move(msg), ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
    , http_status(http_status_)
{
}

void IAIProvider::embed(
    const AIEmbeddingRequest & /*ai_embedding_request*/, const ConnectionTimeouts & /*timeouts*/, AIEmbeddingResponse & /*response*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This AI provider does not support embeddings");
}

void IAIProvider::decide(
    const AIDecisionRequest & /*ai_decision_request*/, const ConnectionTimeouts & /*timeouts*/, AIDecisionResponse & /*response*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This AI provider does not support decision requests");
}

String postJSONToAIProvider(
    const Poco::URI & uri, const String & api_key, const String & function_name, const String & body, const ConnectionTimeouts & timeouts)
{
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty()) /// not all providers need API key
        http_request.set("Authorization", "Bearer " + api_key);
    chassert(!function_name.empty());
    http_request.set("X-ClickHouse-AI-Function", function_name);
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

    return response_body;
}

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key, const String & api_version)
{
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        return std::make_unique<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        return std::make_unique<AnthropicProvider>(endpoint, api_key, api_version);
    else if (provider_name == "typesafe")
        return std::make_unique<TypeSafeProvider>(endpoint, api_key);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei', 'typesafe'", provider_name);
}

}
