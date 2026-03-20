#pragma once

#include <Functions/LLM/ILLMProvider.h>

namespace DB
{

class OpenAIProvider : public ILLMProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);
    LLMResponse call(const LLMRequest & request, const ConnectionTimeouts & timeouts) override;
    LLMEmbeddingResponse embed(const LLMEmbeddingRequest & request, const ConnectionTimeouts & timeouts) override;
    String providerName() const override { return "openai"; }

private:
    Poco::URI deriveEmbeddingURI() const;

    String endpoint;
    String api_key;
    Poco::URI uri;
};

}
