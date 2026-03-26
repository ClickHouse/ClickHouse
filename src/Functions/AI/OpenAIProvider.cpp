#include <Functions/AI/OpenAIProvider.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace
{
String extractProviderError(const std::string & response_body, int status_code)
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
    catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort JSON parsing
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
    root->set("max_tokens", static_cast<Int64>(ai_request.max_tokens));

    Poco::JSON::Array::Ptr messages = new Poco::JSON::Array;

    if (!ai_request.system_prompt.empty())
    {
        Poco::JSON::Object::Ptr sys_msg = new Poco::JSON::Object;
        sys_msg->set("role", "system");
        sys_msg->set("content", sanitizeTextForAI(ai_request.system_prompt));
        messages->add(sys_msg);
    }

    Poco::JSON::Object::Ptr user_msg = new Poco::JSON::Object;
    user_msg->set("role", "user");
    user_msg->set("content", sanitizeTextForAI(ai_request.user_message));
    messages->add(user_msg);

    root->set("messages", messages);

    if (!ai_request.response_format_json.empty())
    {
        Poco::JSON::Parser fmt_parser;
        auto fmt_result = fmt_parser.parse(ai_request.response_format_json);
        root->set("response_format", fmt_result.extract<Poco::JSON::Object::Ptr>());
    }

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = body_stream.str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    http_request.set("Authorization", "Bearer " + api_key);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    std::string response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = ss.str();
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
    if (choices && choices->size() > 0)
    {
        auto choice = choices->getObject(0);
        if (choice)
        {
            auto message = choice->getObject("message");
            if (message)
                ai_response.result = message->optValue<String>("content", "");
            ai_response.finish_reason = choice->optValue<String>("finish_reason", "stop");
        }
    }

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

Poco::URI OpenAIProvider::deriveEmbeddingURI() const
{
    String path = uri.getPath();
    size_t pos = path.find("/chat/completions");
    if (pos != String::npos)
    {
        String new_path = path.substr(0, pos) + "/embeddings";
        Poco::URI embedding_uri(uri);
        embedding_uri.setPath(new_path);
        return embedding_uri;
    }
    if (path.find("/embeddings") != String::npos)
        return uri;

    Poco::URI embedding_uri(uri);
    embedding_uri.setPath("/v1/embeddings");
    return embedding_uri;
}

AIEmbeddingResponse OpenAIProvider::embed(const AIEmbeddingRequest & ai_embedding_request, const ConnectionTimeouts & timeouts)
{
    Poco::URI embedding_uri = deriveEmbeddingURI();

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;

    if (ai_embedding_request.inputs.size() == 1)
    {
        root->set("input", sanitizeTextForAI(ai_embedding_request.inputs[0]));
    }
    else
    {
        Poco::JSON::Array::Ptr input_array = new Poco::JSON::Array;
        for (const auto & text : ai_embedding_request.inputs)
            input_array->add(sanitizeTextForAI(text));
        root->set("input", input_array);
    }

    root->set("model", ai_embedding_request.model);
    if (ai_embedding_request.dimensions > 0)
        root->set("dimensions", static_cast<Int64>(ai_embedding_request.dimensions));

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = body_stream.str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, embedding_uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(
        Poco::Net::HTTPRequest::HTTP_POST,
        embedding_uri.getPathAndQuery(),
        Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty())
        http_request.set("Authorization", "Bearer " + api_key);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    std::string response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = ss.str();
    }

    auto status = http_response.getStatus();
    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "AI embedding provider error: {}", extractProviderError(response_body, static_cast<int>(status)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    AIEmbeddingResponse ai_embedding_response;
    ai_embedding_response.embeddings.resize(ai_embedding_request.inputs.size());

    auto data_arr = json_obj->getArray("data");
    if (data_arr)
    {
        for (unsigned i = 0; i < data_arr->size(); ++i)
        {
            auto item = data_arr->getObject(i);
            if (!item)
                continue;

            unsigned idx = item->optValue<unsigned>("index", i);
            if (idx >= ai_embedding_response.embeddings.size())
                continue;

            auto embedding_arr = item->getArray("embedding");
            if (embedding_arr)
            {
                ai_embedding_response.embeddings[idx].reserve(embedding_arr->size());
                for (unsigned j = 0; j < embedding_arr->size(); ++j)
                    ai_embedding_response.embeddings[idx].push_back(static_cast<Float32>(embedding_arr->getElement<double>(j)));
            }
        }
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
