#pragma once

#include <Functions/AI/IAIProvider.h>

#include <Poco/URI.h>

namespace DB
{

/** Cohere Rerank API
  * https://docs.cohere.com/reference/rerank
  *
  * Rerank request (`model` is required):
  *   POST /v2/rerank
  *   {"model": "rerank-v3.5", "query": "capital of France", "documents": ["Paris is the capital of France.", "Berlin is in Germany."], "top_n": 1}
  *
  * Rerank response:
  *   {"results": [{"index": 0, "relevance_score": 0.98}],
  *    "id": "...",
  *    "meta": {"api_version": {"version": "2"}, "billed_units": {"search_units": 1}}}
  *
  * Cohere bills reranking in `search_units`, which are reported in the `AISearchUnits` profile event
  * (they are not used for quota tracking). `meta.billed_units` may also carry `input_tokens` and
  * `output_tokens` (as Cohere's other APIs do), which are fed to the token quota tracker. This lets a
  * Cohere-compatible endpoint that bills by tokens be used with the token limits.
  *
  * The v1 endpoint (`/v1/rerank`) accepts the same request and returns the same fields, so it works too.
  *
  * `results` is sorted by descending `relevance_score` and holds exactly `top_n` entries (one per
  * document when `top_n` is absent). `index` is the document's position in the request's `documents`
  * array.
  */
class CohereProvider : public IAIProvider
{
public:
    CohereProvider(const String & endpoint_, const String & api_key_);

    bool supportsRerank() const override { return true; }
    void rerank(const AIRerankRequest & ai_rerank_request, const ConnectionTimeouts & timeouts, AIRerankResponse & response) override;

private:
    const String endpoint;
    const String api_key;
    const Poco::URI uri;
};

}
