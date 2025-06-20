#include <Client/AI/AIProviderFactory.h>
#include <Client/AI/OpenAIProvider.h>
#include <Common/Exception.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

AIProviderPtr AIProviderFactory::createProvider(const AIConfiguration & config)
{
    if (config.model_provider == "openai")
    {
            return std::make_unique<OpenAIProvider>(config);
    }
    
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AI provider type. Currently only 'openai' is supported");
}

AIConfiguration AIProviderFactory::loadConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    AIConfiguration ai_config;
    
    // Load basic settings
    if (config.has("ai.api_key"))
        ai_config.api_key = config.getString("ai.api_key");
    
    if (config.has("ai.model_provider"))
        ai_config.model_provider = config.getString("ai.model_provider");
    
    if (config.has("ai.model"))
        ai_config.model = config.getString("ai.model");
    
    // Load optional settings with defaults
    if (config.has("ai.temperature"))
        ai_config.temperature = config.getDouble("ai.temperature");
    
    if (config.has("ai.max_tokens"))
        ai_config.max_tokens = config.getUInt64("ai.max_tokens");
    
    if (config.has("ai.timeout_seconds"))
        ai_config.timeout_seconds = config.getUInt64("ai.timeout_seconds");
    
    // Load custom system prompt if provided
    if (config.has("ai.system_prompt"))
        ai_config.system_prompt = config.getString("ai.system_prompt");
    
    return ai_config;
}

}
