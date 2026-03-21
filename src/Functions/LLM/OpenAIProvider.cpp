#include <Functions/LLM/OpenAIProvider.h>
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

OpenAIProvider::OpenAIProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_), api_key(api_key_), uri(endpoint_)
{
}

LLMResponse OpenAIProvider::call(const LLMRequest & request, const ConnectionTimeouts & timeouts)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", request.model);
    root->set("temperature", request.temperature);
    root->set("max_tokens", static_cast<Int64>(request.max_tokens));

    Poco::JSON::Array::Ptr messages = new Poco::JSON::Array;

    if (!request.system_prompt.empty())
    {
        Poco::JSON::Object::Ptr sys_msg = new Poco::JSON::Object;
        sys_msg->set("role", "system");
        sys_msg->set("content", sanitizeTextForLLM(request.system_prompt));
        messages->add(sys_msg);
    }

    Poco::JSON::Object::Ptr user_msg = new Poco::JSON::Object;
    user_msg->set("role", "user");
    user_msg->set("content", sanitizeTextForLLM(request.user_message));
    messages->add(user_msg);

    root->set("messages", messages);

    if (!request.response_format_json.empty())
    {
        Poco::JSON::Parser fmt_parser;
        auto fmt_result = fmt_parser.parse(request.response_format_json);
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
            "LLM provider returned HTTP {}: {}", static_cast<int>(status), response_body);
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    LLMResponse response;

    auto choices = json_obj->getArray("choices");
    if (choices && choices->size() > 0)
    {
        auto choice = choices->getObject(0);
        if (choice)
        {
            auto message = choice->getObject("message");
            if (message)
                response.result = message->optValue<String>("content", "");
            response.finish_reason = choice->optValue<String>("finish_reason", "stop");
        }
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
        {
            response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
            response.output_tokens = usage->optValue<UInt64>("completion_tokens", 0);
        }
    }

    return response;
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

LLMEmbeddingResponse OpenAIProvider::embed(const LLMEmbeddingRequest & request, const ConnectionTimeouts & timeouts)
{
    Poco::URI embedding_uri = deriveEmbeddingURI();

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;

    if (request.inputs.size() == 1)
    {
        root->set("input", sanitizeTextForLLM(request.inputs[0]));
    }
    else
    {
        Poco::JSON::Array::Ptr input_array = new Poco::JSON::Array;
        for (const auto & text : request.inputs)
            input_array->add(sanitizeTextForLLM(text));
        root->set("input", input_array);
    }

    root->set("model", request.model);
    if (request.dimensions > 0)
        root->set("dimensions", static_cast<Int64>(request.dimensions));

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
            "LLM embedding provider returned HTTP {}: {}", static_cast<int>(status), response_body);
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    LLMEmbeddingResponse response;
    response.embeddings.resize(request.inputs.size());

    auto data_arr = json_obj->getArray("data");
    if (data_arr)
    {
        for (unsigned i = 0; i < data_arr->size(); ++i)
        {
            auto item = data_arr->getObject(i);
            if (!item)
                continue;

            unsigned idx = item->optValue<unsigned>("index", i);
            if (idx >= response.embeddings.size())
                continue;

            auto embedding_arr = item->getArray("embedding");
            if (embedding_arr)
            {
                response.embeddings[idx].reserve(embedding_arr->size());
                for (unsigned j = 0; j < embedding_arr->size(); ++j)
                    response.embeddings[idx].push_back(static_cast<Float32>(embedding_arr->getElement<double>(j)));
            }
        }
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
            response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
    }

    return response;
}

}
