#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Functions/AI/AnthropicProvider.h>
#include <Common/Exception.h>
#include <Common/HTTPHeaderFilter.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
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

AIProviderPtr createAIProvider(
    const ContextPtr & context, const String & provider_name, const String & endpoint, const String & api_key, const String & api_version)
{
    AIProviderPtr provider;
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        provider = std::make_unique<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        provider = std::make_unique<AnthropicProvider>(endpoint, api_key, api_version);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei'", provider_name);

    /// Rejects forbidden headers and CR/LF in values (the key comes from a named collection).
    auto headers = provider->getHeaders();
    context->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers);
    return provider;
}

}
