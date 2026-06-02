#pragma once

#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/JSON/Object.h>
#include <memory>

namespace DB
{

/** Parameters for a single AI chat completion request.
  *
  * Each row processed by an AI function produces one AIRequest.
  * The provider serializes it into the HTTP body format expected by the API.
  */
struct AIRequest
{
    /// Constant instruction that guides the model's behavior (persona, format, constraints).
    /// Sent as a system message (OpenAI) or top-level field (Anthropic).
    String system_prompt;

    /// The per-row input text — this is the actual content to process.
    String user_message;

    /// Optional pre-parsed JSON schema that constrains the model to return structured output.
    /// Parsed once per query in FunctionBaseAI::buildResponseFormat, shared across all rows.
    /// For OpenAI, sent as the `response_format` field — enforced via constrained decoding:
    ///   {"type": "json_schema", "json_schema": {"name": "result", "schema": {"type": "object", "properties": {...}}}}
    ///   https://platform.openai.com/docs/guides/structured-outputs
    /// For Anthropic, approximated via a tool-use pattern (see AnthropicProvider):
    ///   https://docs.anthropic.com/en/docs/build-with-claude/tool-use
    Poco::JSON::Object::Ptr response_format;

    /// Model identifier as specified in the named collection (e.g. "gpt-4o-mini", "claude-sonnet-4-20250514").
    String model;

    /// Controls randomness of the response. 0 = deterministic, higher = more creative.
    /// Accepted range depends on the provider (0-2 for OpenAI, 0-1 for Anthropic).
    float temperature = 0;

    /// Maximum number of tokens the model may generate in its response. This is a per-request limit, not a per-query limit.
    UInt64 max_tokens = 0;
};

/// Response from a single AI chat completion request. Returned by IAIProvider::call after parsing the provider's HTTP response.
struct AIResponse
{
    /// The generated text content from the model.
    String result;

    /// Number of tokens in the input (prompt + system prompt), as reported by the provider. Used for quota tracking.
    UInt64 input_tokens = 0;

    /// Number of tokens in the generated output, as reported by the provider. Used for quota tracking.
    UInt64 output_tokens = 0;

    /// Why the model stopped generating. Common values: "stop" (natural end),
    /// "length" (hit max_tokens limit), "end_turn" (Anthropic equivalent of stop).
    String finish_reason;
};

/** Abstract interface for AI provider HTTP clients.
  *
  * Each provider (OpenAI, Anthropic, etc.) implements this interface to handle
  * the provider-specific HTTP request/response format. The provider is created
  * once per query via createAIProvider and reused for all rows.
  */
class IAIProvider
{
public:
    virtual ~IAIProvider() = default;

    /// Send a chat completion request and return the parsed response.
    virtual AIResponse call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts) = 0;
};

using AIProviderPtr = std::unique_ptr<IAIProvider>;

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key, const String & api_version);

}
