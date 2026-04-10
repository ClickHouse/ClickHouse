#pragma once

#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/JSON/Object.h>
#include <memory>
#include <vector>

namespace DB
{

struct AIRequest
{
    String system_prompt;
    String user_message;
    String response_format_json;
    String model;
    float temperature = 0;
    UInt64 max_tokens = 0;
};

struct AIResponse
{
    String result;
    UInt64 input_tokens = 0;
    UInt64 output_tokens = 0;
    String finish_reason;
};

struct AIEmbeddingRequest
{
    std::vector<String> inputs;
    String model;
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

    virtual AIResponse call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts) = 0;
    virtual AIEmbeddingResponse embed(const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts);
protected:
    String sanitizeTextForAI(const String & input);
};

using AIProviderPtr = std::shared_ptr<IAIProvider>;

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key);

}
