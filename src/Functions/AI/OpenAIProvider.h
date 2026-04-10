#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

class OpenAIProvider : public IAIProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);

    String providerName() const override { return "openai"; }

    AIResponse call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts) override;

private:
    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
