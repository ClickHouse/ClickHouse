#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/OpenAIProvider.h>
#include <Common/Exception.h>
#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

AIEmbeddingResponse IAIProvider::embed(const AIEmbeddingRequest & /*request*/, const ConnectionTimeouts & /*timeouts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Provider '{}' does not support embeddings", providerName());
}

String IAIProvider::sanitizeTextForAI(const String & input)
{
    String output;
    output.reserve(input.size());
    for (auto c : input)
    {
        /// Control characters are U+0000..U+001F except \t \n \r
        if (c < 0x20 && c != '\t' && c != '\n' && c != '\r')
            output.push_back(' ');
        else
            output.push_back(c);
    }
    return output;
}

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key)
{
    if (provider_name == "openai" || provider_name == "huggingface" || provider_name == "tei")
        return std::make_shared<OpenAIProvider>(endpoint, api_key);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown AI provider '{}'. Supported: 'openai', 'huggingface', 'tei'", provider_name);
}
}
