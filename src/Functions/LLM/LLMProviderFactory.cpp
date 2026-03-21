#include <Functions/LLM/ILLMProvider.h>
#include <Functions/LLM/OpenAIProvider.h>
#include <Functions/LLM/AnthropicProvider.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

LLMEmbeddingResponse ILLMProvider::embed(const LLMEmbeddingRequest & /*request*/, const ConnectionTimeouts & /*timeouts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Provider '{}' does not support embeddings", providerName());
}

LLMProviderPtr createLLMProvider(const String & provider_name, const String & endpoint, const String & api_key)
{
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        return std::make_shared<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        return std::make_shared<AnthropicProvider>(endpoint, api_key);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown LLM provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei'", provider_name);
}

}
