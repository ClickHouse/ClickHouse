#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

class AnthropicProvider : public IAIProvider
{
public:
    AnthropicProvider(const String & endpoint_, const String & api_key_);

    String providerName() const override { return "anthropic"; }

    AIResponse call(const AIRequest & request, const ConnectionTimeouts & timeouts) override;

private:
    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
