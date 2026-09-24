#pragma once

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPResponse.h>
#include <memory>
#include <string_view>

namespace DB
{

class AIProviderHTTPException : public Exception
{
public:
    AIProviderHTTPException(Poco::Net::HTTPResponse::HTTPStatus http_status_, PreformattedMessage msg);

    AIProviderHTTPException * clone() const override { return new AIProviderHTTPException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)

    Poco::Net::HTTPResponse::HTTPStatus getHTTPStatus() const { return http_status; }

private:
    Poco::Net::HTTPResponse::HTTPStatus http_status;

    const char * name() const noexcept override { return "DB::AIProviderHTTPException"; }
    const char * className() const noexcept override { return "DB::AIProviderHTTPException"; }
};

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

    /// SQL name of the AI function that produced this request (e.g. "aiGenerate").
    /// Emitted by OpenAIProvider as the `X-ClickHouse-AI-Function` header; ignored by other providers.
    String function_name;
};

/// Canonical, provider-independent reason the model stopped generating. Each provider maps its
/// native vocabulary onto these values.
enum class FinishReason : UInt8
{
    Complete, /// Full answer produced: natural end, a stop sequence, or Anthropic structured-output `tool_use`.
    Truncated, /// Output was cut off by a token limit (`max_tokens` / `length` / context window exceeded).
    ContentFilter, /// The provider withheld or filtered the content.
    RequiresAction, /// Model stopped expecting the caller to act (run a tool / resume the turn), not a final answer
    Unknown, /// Unrecognized finish reason, potentially new reason introduced in API update.
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

    /// Canonical reason the model stopped generating, normalized from the provider's native value.
    FinishReason finish_reason = FinishReason::Complete;

    /// The provider's raw native reason string, kept verbatim for diagnostics in error messages.
    String raw_finish_reason;
};

/** Parameters for a single AI embedding request.
  *
  * Embedding APIs typically accept multiple inputs per call, so inputs is a vector.
  * The provider serializes it into the HTTP body format expected by the API.
  */
struct AIEmbeddingRequest
{
    /// Texts to embed. Providers send these in a single batched HTTP request.
    VectorWithMemoryTracking<String> inputs;

    /// Model identifier as specified in the named collection (e.g. "text-embedding-3-small").
    String model;

    /// Optional target dimensionality for the output vectors. 0 means use the model's native size.
    /// Supported by OpenAI's `text-embedding-3-*` models; providers that ignore it return the native size.
    UInt64 dimensions = 0;

    /// SQL name of the AI function that produced this request (e.g. "aiEmbed").
    /// Emitted by OpenAIProvider as the `X-ClickHouse-AI-Function` header; ignored by other providers.
    String function_name;
};

/// Response from a single embedding request. `embeddings` is aligned 1:1 with `AIEmbeddingRequest::inputs`.
struct AIEmbeddingResponse
{
    /// One vector per input, in the same order as `AIEmbeddingRequest::inputs`.
    VectorWithMemoryTracking<VectorWithMemoryTracking<Float32>> embeddings;

    /// Number of tokens in the input, as reported by the provider. Used for quota tracking.
    UInt64 input_tokens = 0;
};

/** Parameters for a single AI reranking request.
  *
  * Rerank APIs score a list of documents against one query and return them ordered by relevance.
  * Unlike embeddings, the documents of one request are ranked together and cannot be batched with
  * another request's documents, so each processed row produces exactly one AIRerankRequest.
  */
struct AIRerankRequest
{
    /// The query the documents are ranked against.
    String query;

    /// Documents to rank. Zero-copy views into the row's `ColumnString`; the provider copies them into
    /// the HTTP body. Response results reference these by their position in this vector.
    VectorWithMemoryTracking<std::string_view> documents;

    /// Model identifier as specified in the parameter map or the named collection (e.g. "rerank-v3.5").
    String model;

    /// Number of results to return, most relevant first. 0 means all documents. Must not exceed the
    /// number of `documents`: the provider expects exactly this many entries in its response.
    UInt64 top_n = 0;

    /// SQL name of the AI function that produced this request (currently always "aiRerank").
    /// Emitted as the `X-ClickHouse-AI-Function` header.
    String function_name;
};

/// Response from a single reranking request. `results` is ordered by descending relevance and holds
/// exactly `top_n` entries when set, or one per document otherwise.
struct AIRerankResponse
{
    struct Result
    {
        /// Position of the document in the request's `documents` vector, exactly as returned by the provider.
        UInt32 index = 0;
        /// Relevance of the document to the query, in [0, 1] (higher is more relevant).
        Float32 relevance_score = 0;
    };

    /// Ranked results, most relevant first.
    VectorWithMemoryTracking<Result> results;

    /// Number of input tokens billed, as reported by the provider (`meta.billed_units.input_tokens`).
    /// Used for quota tracking. Cohere bills reranking in `search_units` and omits it, leaving it `0`,
    /// but a Cohere-compatible endpoint that bills by tokens can report it.
    UInt64 input_tokens = 0;

    /// Number of output tokens billed, as reported by the provider (`meta.billed_units.output_tokens`).
    /// Used for quota tracking; `0` when omitted, as with `input_tokens`.
    UInt64 output_tokens = 0;

    /// Billed search units, as reported by the provider (`meta.billed_units.search_units`).
    /// Reported in the `AISearchUnits` profile event; not used for quota tracking.
    UInt64 search_units = 0;
};

/** Abstract interface for AI provider HTTP clients.
  *
  * Each provider (OpenAI, Anthropic, Cohere, etc.) implements this interface to handle
  * the provider-specific HTTP request/response format. The provider is created
  * once per query via createAIProvider and reused for all rows.
  *
  * Chat completions, embeddings and reranking are each optional: a provider overrides
  * `supportsX` and the matching method for every API it exposes, and the AI functions check
  * `supportsX` before sending any request.
  */
class IAIProvider
{
public:
    virtual ~IAIProvider() = default;

    /// Whether this provider exposes a chat completions endpoint.
    virtual bool supportsChat() const { return false; }

    /// Send a chat completion request. Only valid when `supportsChat()` is true. Replaces the contents of
    /// `response`, filling the token counts before the payload is validated: so a failed request can still update token counts.
    virtual void call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts, AIResponse & response);

    /// Whether this provider exposes an embeddings endpoint.
    virtual bool supportsEmbeddings() const { return false; }

    /// Send an embedding request. Only valid when `supportsEmbeddings()` is true. Replaces the contents of
    /// `response`, filling `input_tokens` before the payload is validated: so a failed request can still update token counts
    virtual void embed(
        const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts, AIEmbeddingResponse & response);

    /// Whether this provider exposes a reranking endpoint.
    virtual bool supportsRerank() const { return false; }

    /// Send a reranking request. Only valid when `supportsRerank()` is true. Replaces the contents of
    /// `response`, filling the billed `input_tokens`, `output_tokens` and `search_units` before the payload is
    /// validated, so a billed request that fails validation still has its usage recorded.
    virtual void rerank(const AIRerankRequest & ai_rerank_request, const ConnectionTimeouts & timeouts, AIRerankResponse & response);
};

using AIProviderPtr = std::unique_ptr<IAIProvider>;

AIProviderPtr createAIProvider(const String & provider_name, const String & endpoint, const String & api_key, const String & api_version);

/// Build an error message from a provider's non-200 HTTP response, for use in an exception that is logged.
String formatProviderError(int status_code, const String & response_body);

/// Replace control characters (including `\t \n \r`) with spaces so provider-controlled text cannot
/// forge log lines or corrupt a terminal when embedded in a logged exception.
String sanitizeForLog(std::string_view input);

}
