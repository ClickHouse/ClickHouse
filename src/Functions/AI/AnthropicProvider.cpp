#include <Functions/AI/AnthropicProvider.h>
#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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


static constexpr auto DEFAULT_ANTHROPIC_API_VERSION = "2023-06-01";

AnthropicProvider::AnthropicProvider(const String & endpoint_, const String & api_key_, const String & api_version_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , api_version(api_version_.empty() ? DEFAULT_ANTHROPIC_API_VERSION : api_version_)
    , uri(endpoint_)
{
    /// Currently support only a single API version
    if (api_version != DEFAULT_ANTHROPIC_API_VERSION)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported Anthropic API version '{}'. Supported: '{}'", api_version, DEFAULT_ANTHROPIC_API_VERSION);
}

AIResponse AnthropicProvider::call(const AIRequest & ai_request, const ConnectionTimeouts & timeouts)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("model", ai_request.model);
    root->set("temperature", ai_request.temperature);
    root->set("max_tokens", static_cast<Int64>(ai_request.max_tokens));

    if (!ai_request.system_prompt.empty())
        root->set("system", ai_request.system_prompt);

    Poco::JSON::Array::Ptr messages = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr user_msg = new Poco::JSON::Object;
    user_msg->set("role", "user");
    user_msg->set("content", ai_request.user_message);
    messages->add(user_msg);
    root->set("messages", messages);

    if (ai_request.response_format)
    {
        Poco::JSON::Array::Ptr tools_array = new Poco::JSON::Array;

        Poco::JSON::Object::Ptr tool = new Poco::JSON::Object;
        tool->set("name", "structured_output");
        tool->set("description", "Return the result in the specified format");

        if (ai_request.response_format->has("json_schema"))
        {
            auto json_schema = ai_request.response_format->getObject("json_schema");
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
    String body = std::move(body_stream).str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    http_request.set("x-api-key", api_key);
    http_request.set("anthropic-version", api_version);
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
            "Anthropic provider error: {}", extractProviderError(response_body, static_cast<int>(status)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    AIResponse ai_response;

    String anthropic_stop_reason = json_obj->optValue<String>("stop_reason", "end_turn");
    if (anthropic_stop_reason == "max_tokens")
        ai_response.finish_reason = "length";
    else if (anthropic_stop_reason == "end_turn")
        ai_response.finish_reason = "stop";
    else
        ai_response.finish_reason = anthropic_stop_reason;

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
                ai_response.result = block->optValue<String>("text", "");
                break;
            }
            else if (type == "tool_use")
            {
                auto input = block->getObject("input");
                if (input)
                {
                    std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
                    input->stringify(ss);
                    ai_response.result = ss.str();
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
            ai_response.input_tokens = usage->optValue<UInt64>("input_tokens", 0);
            ai_response.output_tokens = usage->optValue<UInt64>("output_tokens", 0);
        }
    }

    return ai_response;
}

}
