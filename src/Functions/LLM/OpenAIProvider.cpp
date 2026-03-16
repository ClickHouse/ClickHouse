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
    extern const int BAD_ARGUMENTS;
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
    auto json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    LLMResponse response;

    auto choices = json_obj->getArray("choices");
    if (choices && choices->size() > 0)
    {
        auto choice = choices->getObject(0);
        auto message = choice->getObject("message");
        response.result = message->getValue<String>("content");
        response.finish_reason = choice->getValue<String>("finish_reason");
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        response.input_tokens = usage->getValue<UInt64>("prompt_tokens");
        response.output_tokens = usage->getValue<UInt64>("completion_tokens");
    }

    return response;
}

}
