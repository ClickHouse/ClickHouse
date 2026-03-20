#pragma once

#include <Functions/AIEmbed/EmbeddingProvider.h>
#include <base/types.h>


namespace DB
{

/// OpenAI-compatible embedding provider.
/// Works with OpenAI, Azure OpenAI, vLLM, LiteLLM, and any OpenAI-compatible API.
/// Sends POST to {endpoint} with {"model": "...", "input": ["text1", "text2", ...]}
class OpenAIProvider : public EmbeddingProvider
{
public:
    OpenAIProvider(
        const String & endpoint_,
        const String & api_key_,
        size_t max_batch_size_,
        const String & api_version_ = "");

    std::vector<std::vector<Float32>> embed(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms,
        size_t max_retries) override;

    String getName() const override { return "openai"; }

private:
    std::vector<std::vector<Float32>> doRequest(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms);

    String endpoint;
    String api_key;
    String api_version;
};

}
