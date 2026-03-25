#pragma once

#include <Functions/AI/ILLMProvider.h>

namespace DB
{

class AnthropicProvider : public ILLMProvider
{
public:
    AnthropicProvider(const String & endpoint_, const String & api_key_);
    LLMResponse call(const LLMRequest & request, const ConnectionTimeouts & timeouts) override;
    String providerName() const override { return "anthropic"; }

private:
    String endpoint;
    String api_key;
    Poco::URI uri;
};

}
