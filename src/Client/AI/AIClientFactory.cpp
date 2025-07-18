#include <Client/AI/AIClientFactory.h>
#include <boost/algorithm/string.hpp>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

ai::Client AIClientFactory::createClient(const AIConfiguration & config)
{
    if (config.provider == "openai")
    {
        // If API key is empty, create_client() will read from OPENAI_API_KEY env var
        if (config.api_key.empty())
            return ai::openai::create_client();
        else
            return ai::openai::create_client(config.api_key);
    }
    else if (config.provider == "anthropic")
    {
        // If API key is empty, create_client() will read from ANTHROPIC_API_KEY env var
        if (config.api_key.empty())
            return ai::anthropic::create_client();
        else
            return ai::anthropic::create_client(config.api_key);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AI provider: {}. Supported providers: openai, anthropic", config.provider);
    }
}

AIConfiguration AIClientFactory::loadConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    AIConfiguration ai_config;

    // Load basic settings
    if (config.has("ai.api_key"))
        ai_config.api_key = config.getString("ai.api_key");

    if (config.has("ai.provider"))
        ai_config.provider = config.getString("ai.provider");

    if (config.has("ai.model"))
        ai_config.model = config.getString("ai.model");

    // Load optional settings with defaults
    if (config.has("ai.temperature"))
        ai_config.temperature = config.getDouble("ai.temperature");

    if (config.has("ai.max_tokens"))
        ai_config.max_tokens = config.getUInt64("ai.max_tokens");

    if (config.has("ai.timeout_seconds"))
        ai_config.timeout_seconds = config.getUInt64("ai.timeout_seconds");

    if (config.has("ai.max_steps"))
        ai_config.max_steps = config.getUInt64("ai.max_steps");

    // Load custom system prompt if provided
    if (config.has("ai.system_prompt"))
        ai_config.system_prompt = config.getString("ai.system_prompt");

    // Validate the loaded configuration
    if (ai_config.provider != "openai" && ai_config.provider != "anthropic")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid AI provider: {}. Must be 'openai' or 'anthropic'", ai_config.provider);

    return ai_config;
}

}
