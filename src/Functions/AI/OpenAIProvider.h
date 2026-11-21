#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

/** OpenAI Chat Completions and Embeddings APIs
  * https://developers.openai.com/api/reference/resources/chat
  * https://developers.openai.com/api/reference/resources/embeddings
  *
  * Also used for OpenAI-compatible APIs (vLLM, Ollama, LiteLLM, HuggingFace TEI)
  * which implement the same request/response format.
  *
  * Chat request:
  *   POST /v1/chat/completions
  *   {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Hello"}], "temperature": 0.7, "max_tokens": 1024}
  *
  * Chat response:
  *   {"choices": [{"message": {"content": "Hi!"}, "finish_reason": "stop"}],
  *    "usage": {"prompt_tokens": 5, "completion_tokens": 2}}
  *
  * Embedding request:
  *   POST /v1/embeddings
  *   {"model": "text-embedding-3-small", "input": ["text 1", "text 2", ...], "dimensions": 256}
  *
  * Embedding response:
  *   {"data": [{"embedding": [0.1, ...], "index": 0}, {"embedding": [0.2, ...], "index": 1}, ...],
  *    "usage": {"prompt_tokens": 8}}
  */
class OpenAIProvider : public IAIProvider
{
public:
    OpenAIProvider(const String & endpoint_, const String & api_key_);

    AIResponse call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts) override;
    bool supportsEmbeddings() const override { return true; }
    AIEmbeddingResponse embed(const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts) override;

private:
    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
