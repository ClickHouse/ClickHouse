#include "config.h"

#ifdef USE_CLICKHOUSE_CLOUD_AI

#include <Functions/AIEmbed/CloudProvider.h>
#include <Functions/AIEmbed/OpenAIProvider.h>


namespace DB
{

CloudProvider::CloudProvider(const String & endpoint_, const String & auth_token_)
    : endpoint(endpoint_), auth_token(auth_token_)
{
}

std::vector<std::vector<Float32>> CloudProvider::embed(
    const String & model,
    const std::vector<std::string_view> & texts,
    size_t timeout_ms,
    size_t max_retries)
{
    /// Cloud provider uses OpenAI-compatible format with internal auth token
    OpenAIProvider openai_provider(endpoint, auth_token, 2048, "");
    return openai_provider.embed(model, texts, timeout_ms, max_retries);
}

}

#endif
