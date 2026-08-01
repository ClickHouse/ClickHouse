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
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int MALFORMED_AI_PROVIDER_RESPONSE;
}

namespace
{
String extractProviderError(const String & response_body, int status_code)
{
    try
    {
        Poco::JSON::Parser err_parser;
        auto err_json = err_parser.parse(response_body);
        auto err_obj = err_json.extract<Poco::JSON::Object::Ptr>();
        if (err_obj && err_obj->has("error"))
        {
            auto err = err_obj->getObject("error");
            if (err)
            {
                String msg = err->optValue<String>("message", "");
                String type = err->optValue<String>("type", "");
                if (!msg.empty())
                    return fmt::format("HTTP {} [{}]: {}", status_code, type, msg);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    size_t max_len = 256;
    return fmt::format("HTTP {} (response truncated to {} chars): {}", status_code, max_len,
        response_body.substr(0, std::min(response_body.size(), max_len)));
}
}


OpenAIProvider::OpenAIProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , uri(endpoint_)
{
}

AIResponse OpenAIProvider::call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts)
{
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
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "AI provider error: {}", extractProviderError(response_body, static_cast<int>(status)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    AIResponse ai_response;

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

    ai_response.result = message->optValue<String>("content", "");
    ai_response.finish_reason = choice->optValue<String>("finish_reason", "stop");

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
        {
            ai_response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
            ai_response.output_tokens = usage->optValue<UInt64>("completion_tokens", 0);
        }
    }

    return ai_response;
}

AIEmbeddingResponse OpenAIProvider::embed(const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts)
{
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
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "AI provider error: {}", extractProviderError(response_body, static_cast<int>(status)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    AIEmbeddingResponse ai_embedding_response;
    ai_embedding_response.embeddings.resize(ai_embedding_request.inputs.size());

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
        if (idx >= ai_embedding_response.embeddings.size())
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].index' = {} is out of range (expected < {})",
                i, idx, ai_embedding_response.embeddings.size());
        if (seen[idx])
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].index' = {} duplicates an earlier entry", i, idx);
        seen[idx] = true;

        auto embedding_arr = item->getArray("embedding");
        if (!embedding_arr)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI embedding response 'data[{}].embedding' is missing or not an array", i);

        for (unsigned j = 0; j < embedding_arr->size(); ++j)
            ai_embedding_response.embeddings[idx].push_back(static_cast<Float32>(embedding_arr->getElement<double>(j)));
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
            ai_embedding_response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
    }

    return ai_embedding_response;
}

}
