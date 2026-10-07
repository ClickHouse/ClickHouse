#pragma once

#include <Functions/AI/IAIProvider.h>

namespace DB
{

/** TypeSafe System One API, serving decision models such as Jev.
  * There is no chat or embeddings endpoint: only `decide` is supported.
  *
  * Request:
  *   POST /v1/systemone
  *   {"model": "jev-latest", "state": "The server crashes when I log in",
  *    "questions": {"category": {"type": "choice", "instructions": "Classify the text into exactly one of the categories.",
  *                               "criteria": {"bug": "A software defect", "question": "A user question"}}}}
  *
  * Response:
  *   {"model": "jev-1.13.0",
  *    "answers": {"category": {"type": "choice", "choice": "bug", "confidence": 1.0, "probabilities": {"bug": 1.0, "question": 0.0}}},
  *    "usage": {"input_tokens": 337, "output_tokens": 38}}
  */
class TypeSafeProvider : public IAIProvider
{
public:
    TypeSafeProvider(const String & endpoint_, const String & api_key_);

    void call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts, AIResponse & response) override;
    bool supportsDecisions() const override { return true; }
    void decide(const AIDecisionRequest & ai_decision_request, const ConnectionTimeouts & timeouts, AIDecisionResponse & response) override;

private:
    const String api_key;
    const Poco::URI uri;
};

}
