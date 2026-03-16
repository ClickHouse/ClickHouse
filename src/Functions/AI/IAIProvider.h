#pragma once

#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <memory>
#include <vector>

namespace DB
{

struct AIEmbeddingRequest
{
    String model;
    std::vector<String> inputs;
    UInt64 dimensions = 0;
};

struct AIEmbeddingResponse
{
    std::vector<std::vector<Float32>> embeddings;
    UInt64 input_tokens = 0;
};

class IAIProvider
{
public:
    virtual ~IAIProvider() = default;

    virtual String providerName() const = 0;

    virtual AIEmbeddingResponse embed(const AIEmbeddingRequest & request, const ConnectionTimeouts & timeouts);

protected:
    /// Strip control characters (like tabs) from the input that can break JSON serialization.
    /// Tabs and newlines are valid in most AI contexts and therefore preserved.
    /// Everything else is replaced with a space.
    String sanitizeTextForAI(const String & input);
};

using AIProviderPtr = std::shared_ptr<IAIProvider>;

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key);

}
