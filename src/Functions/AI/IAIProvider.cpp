#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Functions/AI/AnthropicProvider.h>
#include <Common/Exception.h>
#include <Common/HTTPHeaderFilter.h>
#include <Interpreters/Context.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/String.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

String sanitize(std::string_view input)
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
String IAIProvider::formatProviderError(int status_code, const String & response_body, std::string_view api_key) const
{
    auto safe_text = [&](std::string_view text, size_t limit = std::string_view::npos)
    {
        auto contains_value = [&](std::string_view value) { return !value.empty() && text.contains(value); };
        if (contains_value(api_key) || std::any_of(custom_headers.begin(), custom_headers.end(),
                [&](const HTTPHeaderEntry & header)
                {
                    if (contains_value(header.value))
                        return true;
                    if (Poco::icompare(header.name, "Authorization") != 0)
                        return false;
                    std::string_view value(header.value);
                    const auto scheme_begin = value.find_first_not_of(" \t");
                    const auto scheme_end = value.find_first_of(" \t", scheme_begin);
                    if (scheme_end == std::string_view::npos)
                        return false;
                    const auto first = value.find_first_not_of(" \t", scheme_end);
                    if (first == std::string_view::npos)
                        return false;
                    return contains_value(value.substr(first, value.find_last_not_of(" \t") - first + 1));
                }))
            return String("[REDACTED]");
        return sanitize(text.substr(0, limit));
    };

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
                    return fmt::format("HTTP {} [{}]: {}", status_code, safe_text(type), safe_text(msg));
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch) Ok: fall through to the raw-body branch below.
    {
    }

    constexpr size_t max_len = 256;
    return fmt::format("HTTP {}: {}", status_code, safe_text(response_body, max_len));
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

HTTPHeaderEntries IAIProvider::addCustomHeaders(HTTPHeaderEntries headers) const
{
    for (const auto & custom_header : custom_headers)
    {
        const auto same_name = [&](const HTTPHeaderEntry & header) { return Poco::icompare(header.name, custom_header.name) == 0; };
        if (std::any_of(headers.begin(), headers.end(), same_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate AI HTTP header '{}'", custom_header.name);
        headers.push_back(custom_header);
    }
    return headers;
}

AIProviderPtr createAIProvider(
    const ContextPtr & context,
    const String & provider_name,
    const String & endpoint,
    std::string_view api_key,
    const String & api_version,
    HTTPHeaderEntries custom_headers)
{
    /// Validate custom names/values before producing any diagnostics about duplicate names.
    context->getHTTPHeaderFilter().checkHeaders(custom_headers);
    for (const auto & header : custom_headers)
    {
        if (Poco::icompare(header.name, "Content-Length") == 0 || Poco::icompare(header.name, "Transfer-Encoding") == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI functions manage HTTP header '{}' automatically", header.name);
    }
    AIProviderPtr provider;
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        provider = std::make_unique<OpenAIProvider>(endpoint, api_key, std::move(custom_headers));
    else if (provider_name == "anthropic")
        provider = std::make_unique<AnthropicProvider>(endpoint, api_key, api_version, std::move(custom_headers));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei'", provider_name);

    /// Rejects forbidden headers and CR/LF in values (the key comes from a named collection).
    auto headers = provider->getHeaders();
    context->getHTTPHeaderFilter().checkHeaders(headers);
    return provider;
}

}
