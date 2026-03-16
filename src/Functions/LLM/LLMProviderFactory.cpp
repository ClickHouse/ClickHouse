#include <Functions/LLM/ILLMProvider.h>
#include <Functions/LLM/OpenAIProvider.h>
#include <Functions/LLM/AnthropicProvider.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

LLMProviderPtr createLLMProvider(const String & provider_name, const String & endpoint, const String & api_key)
{
    if (provider_name == "openai")
        return std::make_shared<OpenAIProvider>(endpoint, api_key);
    else if (provider_name == "anthropic")
        return std::make_shared<AnthropicProvider>(endpoint, api_key);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown LLM provider '{}'. Supported: 'openai', 'anthropic'", provider_name);
}

}
