#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

/** OpenAI Chat Completions API
  * https://developers.openai.com/api/reference/resources/chat
  *
  * Also used for OpenAI-compatible APIs (vLLM, Ollama, LiteLLM, HuggingFace TEI)
  * which implement the same request/response format.
  *
  * Request:
  *   POST /v1/chat/completions
  *   {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Hello"}], "temperature": 0.7, "max_tokens": 1024}
  *
  * Response:
  *   {"choices": [{"message": {"content": "Hi!"}, "finish_reason": "stop"}],
  *    "usage": {"prompt_tokens": 5, "completion_tokens": 2}}
  */
class OpenAIProvider : public IAIProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);

    AIResponse call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts) override;

private:
    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
