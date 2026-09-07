#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Functions/AI/AnthropicProvider.h>
#include <Common/Exception.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace
{

/// Replace control characters (including `\t \n \r`) with spaces so a provider's response cannot
/// forge log lines or corrupt a terminal when embedded in a logged exception.
String sanitizeForLog(std::string_view input)
{
    String output;
    output.reserve(input.size());
    for (unsigned char ch : input)
        output.push_back((ch < 0x20 || ch == 0x7F) ? ' ' : static_cast<char>(ch));
    return output;
}

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

AIEmbeddingResponse IAIProvider::embed(const AIEmbeddingRequest & /*ai_embedding_request*/, const ConnectionTimeouts & /*timeouts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This AI provider does not support embeddings");
}

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key, const String & api_version)
{
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        return std::make_unique<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        return std::make_unique<AnthropicProvider>(endpoint, api_key, api_version);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei'", provider_name);
}

}
