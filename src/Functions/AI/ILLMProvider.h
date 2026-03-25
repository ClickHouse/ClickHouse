#pragma once

#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/JSON/Object.h>
#include <memory>
#include <vector>

namespace DB
{

struct LLMRequest
{
    String system_prompt;
    String user_message;
    String response_format_json;
    String model;
    float temperature = 0;
    UInt64 max_tokens = 1024;
};

struct LLMResponse
{
    String result;
    UInt64 input_tokens = 0;
    UInt64 output_tokens = 0;
    String finish_reason;
};

struct LLMEmbeddingRequest
{
    std::vector<String> inputs;
    String model;
    UInt64 dimensions = 0;
};

struct LLMEmbeddingResponse
{
    std::vector<std::vector<Float32>> embeddings;
    UInt64 input_tokens = 0;
};

class ILLMProvider
{
public:
    virtual ~ILLMProvider() = default;
    virtual LLMResponse call(const LLMRequest & request, const ConnectionTimeouts & timeouts) = 0;
    virtual LLMEmbeddingResponse embed(const LLMEmbeddingRequest & request, const ConnectionTimeouts & timeouts);
    virtual String providerName() const = 0;
protected:
    String sanitizeTextForLLM(const String & input);
};

using LLMProviderPtr = std::shared_ptr<ILLMProvider>;

LLMProviderPtr createLLMProvider(const String & provider_name, const String & endpoint, const String & api_key);

}
