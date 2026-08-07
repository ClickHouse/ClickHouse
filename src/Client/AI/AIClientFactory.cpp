#include <Client/AI/AIClientFactory.h>
#include <boost/algorithm/string.hpp>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

AIClientResult AIClientFactory::createClient(const AIConfiguration & config)
{
    auto logger = getLogger("AIClientFactory");
    AIClientResult result;

    // If no provider is specified, try environment-based fallbacks
    if (config.provider.empty())
    {
        LOG_DEBUG(logger, "No AI provider specified, trying environment-based fallbacks");
        auto client = ai::openai::try_create_client();
        if (client.has_value())
        {
            result.client = std::move(client);
            result.inferred_from_env = true;
            result.provider = "openai";
            return result;
        }

        client = ai::anthropic::try_create_client();
        if (client.has_value())
        {
            result.client = std::move(client);
            result.inferred_from_env = true;
            result.provider = "anthropic";
            return result;
        }

        result.no_configuration_found = true;
        return result;
    }
    else if (config.provider == "openai")
    {
        result.provider = "openai";
        result.client = ai::openai::create_client(config.api_key, config.base_url);
        result.inferred_from_env = config.api_key.empty();
        return result;
    }
    else if (config.provider == "anthropic")
    {
        result.provider = "anthropic";
        result.client = ai::anthropic::create_client(config.api_key, config.base_url);
        result.inferred_from_env = config.api_key.empty();
        return result;
    }

    // Unknown provider
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown AI provider: '{}'. Supported providers are: 'openai', 'anthropic'",
        config.provider);
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

    // Load custom base URL for OpenAI-compatible APIs
    if (config.has("ai.base_url"))
        ai_config.base_url = config.getString("ai.base_url");

    if (config.has("ai.enable_schema_access"))
        ai_config.enable_schema_access = config.getBool("ai.enable_schema_access");

    // Validate the loaded configuration
    if (!ai_config.provider.empty() && ai_config.provider != "openai" && ai_config.provider != "anthropic")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid AI provider: '{}'. Must be 'openai' or 'anthropic'", ai_config.provider);

    // Validate that base_url is only used with OpenAI provider
    if (!ai_config.base_url.empty() && ai_config.provider != "openai")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom base_url is only supported with 'openai' provider for OpenAI-compatible APIs");

    return ai_config;
}

}
