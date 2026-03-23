#pragma once

#include <Functions/LLM/ILLMProvider.h>

namespace DB
{

class AnthropicProvider : public ILLMProvider
{
public:
    AnthropicProvider(const String & endpoint_, const String & api_key_);
    String providerName() const override { return "anthropic"; }
    LLMResponse call(const LLMRequest & request, const ConnectionTimeouts & timeouts) override;

private:
    String endpoint;
    String api_key;
    Poco::URI uri;
};

}
