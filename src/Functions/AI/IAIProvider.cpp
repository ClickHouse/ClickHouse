#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Functions/AI/AnthropicProvider.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

/// Strip control characters (U+0000..U+001F except \t \n \r) that break JSON serialization.
/// Tabs and newlines are preserved as they're valid in most AI contexts;
/// everything else is replaced with a space.
String IAIProvider::sanitizeTextForAI(const String & input)
{
    String output;
    output.reserve(input.size());
    for (unsigned char ch : input)
    {
        if (ch < 0x20 && ch != '\t' && ch != '\n' && ch != '\r')
            output.push_back(' ');
        else
            output.push_back(static_cast<char>(ch));
    }
    return output;
}


AIEmbeddingResponse IAIProvider::embed(const AIEmbeddingRequest & /*ai_embedding_request*/, const ConnectionTimeouts & /*timeouts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Provider '{}' does not support embeddings", providerName());
}

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key)
{
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        return std::make_shared<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        return std::make_shared<AnthropicProvider>(endpoint, api_key);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'anthropic', 'huggingface', 'tei'", provider_name);
}

}
