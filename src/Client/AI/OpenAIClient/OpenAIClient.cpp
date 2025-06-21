#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Dynamic/Var.h>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace openai
{

OpenAIClient::OpenAIClient(const std::string & api_key_, const std::string & base_url_)
    : api_key(api_key_)
    , base_url(base_url_)
{
    if (api_key.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "OpenAI API key cannot be empty");
}

OpenAIClient::ChatCompletionResponse OpenAIClient::createChatCompletion(const ChatCompletionRequest & request)
{
    try
    {
        Poco::URI uri(base_url + "/v1/chat/completions");
        
        /// Create JSON request body using Poco JSON
        Poco::JSON::Object::Ptr json_request = new Poco::JSON::Object;
        json_request->set("model", request.model);
        
        /// Create messages array
        Poco::JSON::Array::Ptr messages_array = new Poco::JSON::Array;
        for (const auto & message : request.messages)
        {
            Poco::JSON::Object::Ptr message_obj = new Poco::JSON::Object;
            message_obj->set("role", message.role);
            message_obj->set("content", message.content);
            
            /// Add optional name field for function messages
            if (message.name.has_value())
                message_obj->set("name", message.name.value());
            
            /// Add tool_call_id for tool response messages
            if (message.tool_call_id.has_value())
                message_obj->set("tool_call_id", message.tool_call_id.value());
            
            /// Add tool_calls if present
            if (!message.tool_calls.empty())
            {
                Poco::JSON::Array::Ptr tool_calls_array = new Poco::JSON::Array;
                for (const auto & tc : message.tool_calls)
                {
                    Poco::JSON::Object::Ptr tc_obj = new Poco::JSON::Object;
                    tc_obj->set("id", tc.id);
                    tc_obj->set("type", tc.type);
                    
                    Poco::JSON::Object::Ptr func_obj = new Poco::JSON::Object;
                    func_obj->set("name", tc.function.name);
                    func_obj->set("arguments", tc.function.arguments);
                    tc_obj->set("function", func_obj);
                    
                    tool_calls_array->add(tc_obj);
                }
                message_obj->set("tool_calls", tool_calls_array);
            }
            
            messages_array->add(message_obj);
        }
        json_request->set("messages", messages_array);
        
        /// Add tools array if present
        std::cerr << "AI: Checking tools - has_value: " << request.tools.has_value() << std::endl;
        if (request.tools.has_value())
        {
            std::cerr << "AI: Adding " << request.tools.value().size() << " tools to request" << std::endl;
            Poco::JSON::Array::Ptr tools_array = new Poco::JSON::Array;
            for (const auto & func : request.tools.value())
            {
                Poco::JSON::Object::Ptr tool_obj = new Poco::JSON::Object;
                tool_obj->set("type", "function");
                
                Poco::JSON::Object::Ptr func_obj = new Poco::JSON::Object;
                func_obj->set("name", func.name);
                func_obj->set("description", func.description);
                
                /// Create parameters object
                Poco::JSON::Object::Ptr params_obj = new Poco::JSON::Object;
                params_obj->set("type", func.parameters.type);
                
                /// Create properties object
                Poco::JSON::Object::Ptr properties_obj = new Poco::JSON::Object;
                for (const auto & [prop_name, prop_def] : func.parameters.properties)
                {
                    Poco::JSON::Object::Ptr prop_obj = new Poco::JSON::Object;
                    prop_obj->set("type", prop_def.type);
                    prop_obj->set("description", prop_def.description);
                    
                    if (prop_def.enum_values.has_value())
                    {
                        Poco::JSON::Array::Ptr enum_array = new Poco::JSON::Array;
                        for (const auto & enum_val : prop_def.enum_values.value())
                            enum_array->add(enum_val);
                        prop_obj->set("enum", enum_array);
                    }
                    
                    properties_obj->set(prop_name, prop_obj);
                }
                params_obj->set("properties", properties_obj);
                
                /// Add required array
                Poco::JSON::Array::Ptr required_array = new Poco::JSON::Array;
                for (const auto & req : func.parameters.required)
                    required_array->add(req);
                params_obj->set("required", required_array);
                params_obj->set("additionalProperties", false);
                
                func_obj->set("parameters", params_obj);
                func_obj->set("strict", true);
                
                tool_obj->set("function", func_obj);
                tools_array->add(tool_obj);
            }
            json_request->set("tools", tools_array);
        }
        
        /// Add tool_choice control if present
        if (request.tool_choice.has_value())
        {
            const std::string & tc = request.tool_choice.value();
            if (tc == "auto" || tc == "none" || tc == "required")
            {
                json_request->set("tool_choice", tc);
            }
            else
            {
                /// Parse as JSON object for specific function name
                Poco::JSON::Parser parser;
                Poco::Dynamic::Var var = parser.parse(tc);
                json_request->set("tool_choice", var);
            }
        }
        
        /// Add optional fields
        if (request.temperature.has_value())
            json_request->set("temperature", request.temperature.value());
        
        if (request.max_tokens.has_value())
            json_request->set("max_tokens", request.max_tokens.value());
        
        /// Convert to JSON string
        std::ostringstream json_stream;
        Poco::JSON::Stringifier::stringify(json_request, json_stream);
        std::string request_body = json_stream.str();
        
        /// Debug: Print request
        std::cerr << "AI: OpenAI request: " << request_body << std::endl;
        
        /// Set up HTTP connection
        ConnectionTimeouts timeouts;
        timeouts.connection_timeout = Poco::Timespan(30, 0);
        timeouts.send_timeout = Poco::Timespan(30, 0);
        timeouts.receive_timeout = Poco::Timespan(60, 0);
        
        auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts, ProxyConfiguration{});
        
        /// Create HTTP request
        Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
        http_request.setHost(uri.getHost());
        http_request.setContentType("application/json");
        http_request.setContentLength(request_body.length());
        http_request.set("Authorization", "Bearer " + api_key);
        
        LOG_TRACE(getLogger("OpenAIClient"), "Sending request to OpenAI API: {}", uri.toString());
        
        /// Send request
        auto & ostr = session->sendRequest(http_request);
        ostr << request_body;
        
        /// Get response
        Poco::Net::HTTPResponse http_response;
        auto & istr = session->receiveResponse(http_response);
        
        if (http_response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        {
            std::string error_body;
            std::getline(istr, error_body, '\0');
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "OpenAI API returned error status {}: {}", 
                           http_response.getStatus(), error_body);
        }
        
        /// Read response body
        std::string response_body;
        std::getline(istr, response_body, '\0');
        
        /// Debug: Print response
        std::cerr << "AI: OpenAI response: " << response_body << std::endl;
        
        /// Parse JSON response
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(response_body);
        const Poco::JSON::Object::Ptr & doc = json.extract<Poco::JSON::Object::Ptr>();
        
        ChatCompletionResponse response;
        response.id = doc->getValue<std::string>("id");
        response.object = doc->getValue<std::string>("object");
        response.created = doc->getValue<UInt64>("created");
        response.model = doc->getValue<std::string>("model");
        
        const Poco::JSON::Array::Ptr choices_array = doc->getArray("choices");
        for (size_t i = 0; i < choices_array->size(); ++i)
        {
            const Poco::JSON::Object::Ptr choice = choices_array->getObject(i);
            ChatCompletionResponse::Choice c;
            c.index = choice->getValue<UInt32>("index");
            
            const Poco::JSON::Object::Ptr message = choice->getObject("message");
            c.message.role = message->getValue<std::string>("role");
            
            /// Content might be null when there's a tool call
            if (message->has("content") && !message->isNull("content"))
                c.message.content = message->getValue<std::string>("content");
            
            /// Check for tool_calls in the message
            if (message->has("tool_calls") && !message->isNull("tool_calls"))
            {
                const Poco::JSON::Array::Ptr tool_calls_array = message->getArray("tool_calls");
                for (size_t j = 0; j < tool_calls_array->size(); ++j)
                {
                    const Poco::JSON::Object::Ptr tool_call_obj = tool_calls_array->getObject(j);
                    ChatCompletionResponse::Choice::Message::ToolCall tc;
                    tc.id = tool_call_obj->getValue<std::string>("id");
                    tc.type = tool_call_obj->getValue<std::string>("type");
                    
                    const Poco::JSON::Object::Ptr function_obj = tool_call_obj->getObject("function");
                    tc.function.name = function_obj->getValue<std::string>("name");
                    tc.function.arguments = function_obj->getValue<std::string>("arguments");
                    
                    c.message.tool_calls.push_back(tc);
                }
            }
            
            c.finish_reason = choice->getValue<std::string>("finish_reason");
            response.choices.push_back(c);
        }
        
        const Poco::JSON::Object::Ptr usage = doc->getObject("usage");
        response.usage.prompt_tokens = usage->getValue<UInt32>("prompt_tokens");
        response.usage.completion_tokens = usage->getValue<UInt32>("completion_tokens");
        response.usage.total_tokens = usage->getValue<UInt32>("total_tokens");
        
        return response;
    }
    catch (const Poco::Exception & e)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to parse OpenAI API response: {}", e.displayText());
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "OpenAI API request failed: {}", e.what());
    }
}

} /// namespace openai
} /// namespace DB
