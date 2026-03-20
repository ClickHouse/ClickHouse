#include <Functions/LLM/AnthropicProvider.h>
#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

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

AnthropicProvider::AnthropicProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_), api_key(api_key_), uri(endpoint_)
{
}

LLMResponse AnthropicProvider::call(const LLMRequest & request, const ConnectionTimeouts & timeouts)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", request.model);
    root->set("max_tokens", static_cast<Int64>(request.max_tokens));
    root->set("temperature", request.temperature);

    if (!request.system_prompt.empty())
        root->set("system", sanitizeTextForLLM(request.system_prompt));

    Poco::JSON::Array::Ptr messages = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr user_msg = new Poco::JSON::Object;
    user_msg->set("role", "user");
    user_msg->set("content", sanitizeTextForLLM(request.user_message));
    messages->add(user_msg);
    root->set("messages", messages);

    if (!request.response_format_json.empty())
    {
        Poco::JSON::Parser fmt_parser;
        Poco::JSON::Array::Ptr tools_array = new Poco::JSON::Array;

        Poco::JSON::Object::Ptr tool = new Poco::JSON::Object;
        tool->set("name", "structured_output");
        tool->set("description", "Return the result in the specified format");

        auto schema_result = fmt_parser.parse(request.response_format_json);
        auto schema_obj = schema_result.extract<Poco::JSON::Object::Ptr>();
        if (schema_obj->has("json_schema"))
        {
            auto json_schema = schema_obj->getObject("json_schema");
            if (json_schema->has("schema"))
                tool->set("input_schema", json_schema->getObject("schema"));
        }

        tools_array->add(tool);
        root->set("tools", tools_array);

        Poco::JSON::Object::Ptr tool_choice = new Poco::JSON::Object;
        tool_choice->set("type", "tool");
        tool_choice->set("name", "structured_output");
        root->set("tool_choice", tool_choice);
    }

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = body_stream.str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    http_request.set("x-api-key", api_key);
    http_request.set("anthropic-version", "2023-06-01");
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
            "Anthropic provider returned HTTP {}: {}", static_cast<int>(status), response_body);
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    auto json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    LLMResponse response;

    String anthropic_stop_reason = json_obj->optValue<String>("stop_reason", "end_turn");
    if (anthropic_stop_reason == "max_tokens")
        response.finish_reason = "length";
    else if (anthropic_stop_reason == "end_turn")
        response.finish_reason = "stop";
    else
        response.finish_reason = anthropic_stop_reason;

    auto content = json_obj->getArray("content");
    if (content)
    {
        for (unsigned i = 0; i < content->size(); ++i)
        {
            auto block = content->getObject(i);
            if (!block)
                continue;
            String type = block->optValue<String>("type", "");
            if (type == "text")
            {
                response.result = block->optValue<String>("text", "");
                break;
            }
            else if (type == "tool_use")
            {
                auto input = block->getObject("input");
                if (input)
                {
                    std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
                    input->stringify(ss);
                    response.result = ss.str();
                }
                break;
            }
        }
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
        {
            response.input_tokens = usage->optValue<UInt64>("input_tokens", 0);
            response.output_tokens = usage->optValue<UInt64>("output_tokens", 0);
        }
    }

    return response;
}

}
