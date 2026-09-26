#include <Functions/AI/OpenAIProvider.h>
#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int MALFORMED_AI_PROVIDER_RESPONSE;
}

OpenAIProvider::OpenAIProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , uri(endpoint_)
{
}

void OpenAIProvider::call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts, AIResponse & response)
{
    response = {};

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", ai_request.model);
    root->set("temperature", ai_request.temperature);
    root->set("max_tokens", static_cast<Int64>(ai_request.max_tokens)); /// Poco doesn't have UInt type

    Poco::JSON::Array::Ptr messages = new Poco::JSON::Array;

    if (!ai_request.system_prompt.empty())
    {
        Poco::JSON::Object::Ptr sys_msg = new Poco::JSON::Object;
        sys_msg->set("role", "system");
        sys_msg->set("content", ai_request.system_prompt);
        messages->add(sys_msg);
    }

    Poco::JSON::Object::Ptr user_msg = new Poco::JSON::Object;
    user_msg->set("role", "user");
    user_msg->set("content", ai_request.user_message);
    messages->add(user_msg);

    root->set("messages", messages);

    if (ai_request.response_format)
        root->set("response_format", ai_request.response_format);

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = std::move(body_stream).str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty()) /// not all providers need API key
        http_request.set("Authorization", "Bearer " + api_key);
    chassert(!ai_request.function_name.empty());
    http_request.set("X-ClickHouse-AI-Function", ai_request.function_name);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    String response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = std::move(ss).str();
    }

    auto status = http_response.getStatus();
    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw AIProviderHTTPException(
            status,
            PreformattedMessage::create("AI provider error: {}", formatProviderError(static_cast<int>(status), response_body)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    /// A malformed body was still charged for, so read the usage before the checks below can throw.
    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
        {
            response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
            response.output_tokens = usage->optValue<UInt64>("completion_tokens", 0);
        }
    }

    auto choices = json_obj->getArray("choices");
    if (!choices || choices->size() == 0)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "AI chat response is missing or has empty 'choices' array");

    auto choice = choices->getObject(0);
    if (!choice)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "AI chat response is improperly formatted, JSON does not contain a response.");

    auto message = choice->getObject("message");
    if (!message)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "AI chat response is missing output message");

    response.result = message->optValue<String>("content", "");

    /// A structured-output safety refusal arrives as a populated `message.refusal` with a null
    /// `content`, and `finish_reason` stays "stop" because the generation itself ended normally.
    auto refusal = message->optValue<String>("refusal", "");
    if (!refusal.empty())
    {
        response.result = refusal;
        response.raw_finish_reason = "refusal";
        response.finish_reason = FinishReason::ContentFilter;
    }
    else
    {
        /// Map OpenAI's `finish_reason` onto the canonical `FinishReason`. An absent field means the
        /// generation completed normally. OpenAI reuses "stop" for both a natural end and a stop-sequence
        /// hit, so a stop sequence does not look like truncation.
        response.raw_finish_reason = choice->optValue<String>("finish_reason", "stop");
        if (response.raw_finish_reason == "stop")
            response.finish_reason = FinishReason::Complete;
        else if (response.raw_finish_reason == "length")
            response.finish_reason = FinishReason::Truncated;
        else if (response.raw_finish_reason == "content_filter")
            response.finish_reason = FinishReason::ContentFilter;
        else if (response.raw_finish_reason == "tool_calls" || response.raw_finish_reason == "function_call")
            response.finish_reason = FinishReason::RequiresAction;
        else
            response.finish_reason = FinishReason::Unknown;
    }
}

void OpenAIProvider::embed(
    const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts, AIEmbeddingResponse & response)
{
    response.embeddings.clear();
    response.input_tokens = 0;

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", ai_embedding_request.model);

    Poco::JSON::Array::Ptr input_array = new Poco::JSON::Array;
    for (const auto & text : ai_embedding_request.inputs)
        input_array->add(text);
    root->set("input", input_array);

    if (ai_embedding_request.dimensions > 0)
        root->set("dimensions", static_cast<Int64>(ai_embedding_request.dimensions));

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = std::move(body_stream).str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty()) /// not all providers need API key
        http_request.set("Authorization", "Bearer " + api_key);
    chassert(!ai_embedding_request.function_name.empty());
    http_request.set("X-ClickHouse-AI-Function", ai_embedding_request.function_name);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    String response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = std::move(ss).str();
    }

    auto status = http_response.getStatus();
    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw AIProviderHTTPException(
            status,
            PreformattedMessage::create("AI provider error: {}", formatProviderError(static_cast<int>(status), response_body)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    /// A malformed body was still charged for, so read the usage before the checks below can throw.
    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
            response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
    }

    response.embeddings.resize(ai_embedding_request.inputs.size());

    auto data_arr = json_obj->getArray("data");
    if (!data_arr)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI embedding response is missing 'data' array");

    if (data_arr->size() != ai_embedding_request.inputs.size())
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "AI embedding response 'data' has {} entries but {} were requested",
            data_arr->size(), ai_embedding_request.inputs.size());

    /// Track which input slots have been filled, so a misbehaving provider that returns duplicate
    /// `index` values can't silently leave other slots empty or stack multiple embeddings into one.
    /// Combined with the cardinality check above, "no duplicates" implies every slot is filled exactly once.
    VectorWithMemoryTracking<bool> seen(ai_embedding_request.inputs.size(), false);

    for (unsigned i = 0; i < data_arr->size(); ++i)
    {
        auto item = data_arr->getObject(i);
        if (!item)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}]' is not an object", i);

        /// `index` tells us which input this embedding corresponds to. Defaults to `i` when missing (TEI).
        UInt64 idx = item->optValue<UInt64>("index", i);
        if (idx >= response.embeddings.size())
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].index' = {} is out of range (expected < {})",
                i, idx, response.embeddings.size());
        if (seen[idx])
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].index' = {} duplicates an earlier entry", i, idx);
        seen[idx] = true;

        auto embedding_arr = item->getArray("embedding");
        if (!embedding_arr)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].embedding' is missing or not an array", i);

        for (unsigned j = 0; j < embedding_arr->size(); ++j)
            response.embeddings[idx].push_back(static_cast<Float32>(embedding_arr->getElement<double>(j)));
    }
}

}
