#pragma once

#include <base/types.h>

#include <vector>


namespace DB
{

/// Interface for embedding providers (OpenAI-compatible, HuggingFace TEI, Cloud).
/// Each provider knows how to send a batch of texts and return a vector of embeddings.
class EmbeddingProvider
{
public:
    virtual ~EmbeddingProvider() = default;

    /// Embed a batch of texts. Returns one vector per input text (same order).
    /// Throws on unrecoverable error. Retries are handled internally by implementations.
    virtual std::vector<std::vector<Float32>> embed(
        const String & model,
        const std::vector<std::string_view> & texts,
        size_t timeout_ms,
        size_t max_retries) = 0;

    virtual String getName() const = 0;
};

using EmbeddingProviderPtr = std::shared_ptr<EmbeddingProvider>;

}
