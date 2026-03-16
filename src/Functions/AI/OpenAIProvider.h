#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

class OpenAIProvider : public IAIProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);

    String providerName() const override { return "openai"; }
    AIEmbeddingResponse embed(const AIEmbeddingRequest & request, const ConnectionTimeouts & timeouts) override;

private:
    Poco::URI deriveEmbeddingURI() const;

    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
