#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

/** Anthropic Messages API
  * https://platform.claude.com/docs/en/api/messages
  *
  * Request:
  *   POST /v1/messages
  *   {"model": "claude-sonnet-4-20250514", "system": "Be concise", "messages": [{"role": "user", "content": "Hello"}],
  *    "temperature": 0.7, "max_tokens": 1024}
  *
  * Response:
  *   {"content": [{"type": "text", "text": "Hi!"}], "stop_reason": "end_turn",
  *    "usage": {"input_tokens": 10, "output_tokens": 2}}
  */
class AnthropicProvider : public IAIProvider
{
public:
    AnthropicProvider(const String & endpoint_, const String & api_key_, const String & api_version_);

    String providerName() const override { return "anthropic"; }

    AIResponse call(const AIRequest & request, const ConnectionTimeouts & timeouts) override;

private:
    const String endpoint;
    const String api_key;
    const String api_version;
    const Poco::URI uri;
};

}
