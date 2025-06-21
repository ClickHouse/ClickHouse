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

OpenAIClient::OpenAIClient(const std::string & api_key_, const std::string & base_url_)
    : api_key(api_key_)
    , base_url(base_url_)
{
    if (api_key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OpenAI API key cannot be empty");
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
            messages_array->add(message_obj);
        }
        json_request->set("messages", messages_array);
        
        /// Add optional fields
        if (request.temperature.has_value())
            json_request->set("temperature", request.temperature.value());
        
        if (request.max_tokens.has_value())
            json_request->set("max_tokens", request.max_tokens.value());
        
        /// Convert to JSON string
        std::ostringstream json_stream;
        Poco::JSON::Stringifier::stringify(json_request, json_stream);
        std::string request_body = json_stream.str();
        
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
            throw Exception(ErrorCodes::NETWORK_ERROR, "OpenAI API returned error status {}: {}", 
                           http_response.getStatus(), error_body);
        }
        
        /// Read response body
        std::string response_body;
        std::getline(istr, response_body, '\0');
        
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
            c.message.content = message->getValue<std::string>("content");
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse OpenAI API response: {}", e.displayText());
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "OpenAI API request failed: {}", e.what());
    }
}

}
