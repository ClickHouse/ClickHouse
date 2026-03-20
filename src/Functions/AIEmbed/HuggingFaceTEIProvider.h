#pragma once

#include <Functions/AIEmbed/EmbeddingProvider.h>
#include <base/types.h>


namespace DB
{

/// HuggingFace Text Embeddings Inference (TEI) provider.
/// Sends POST to {endpoint} with {"inputs": ["text1", "text2", ...]}
/// Response: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
class HuggingFaceTEIProvider : public EmbeddingProvider
{
public:
    HuggingFaceTEIProvider(
        const String & endpoint_,
        const String & api_key_,
        size_t max_batch_size_);

    std::vector<std::vector<Float32>> embed(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms,
        size_t max_retries) override;

    String getName() const override { return "huggingface_tei"; }

private:
    std::vector<std::vector<Float32>> doRequest(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms);

    String endpoint;
    String api_key;
};

}
