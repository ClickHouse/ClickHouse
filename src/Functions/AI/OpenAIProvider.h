#pragma once

#include <Functions/AI/ILLMProvider.h>

namespace DB
{

class OpenAIProvider : public ILLMProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);

    String providerName() const override { return "openai"; }

    LLMResponse call(const LLMRequest & request, const ConnectionTimeouts & timeouts) override;
    LLMEmbeddingResponse embed(const LLMEmbeddingRequest & request, const ConnectionTimeouts & timeouts) override;

private:
    Poco::URI deriveEmbeddingURI() const;

    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
