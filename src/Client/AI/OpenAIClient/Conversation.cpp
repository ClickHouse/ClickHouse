#include <Client/AI/OpenAIClient/Conversation.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>

namespace DB
{
namespace openai
{

Conversation::Conversation(std::string_view system_data)
{
    setSystemData(system_data);
}

Conversation::Conversation(std::string_view system_data, std::string_view user_data)
{
    setSystemData(system_data);
    addUserData(user_data);
}

Conversation::Conversation(std::string_view system_data, std::initializer_list<std::string_view> user_data)
{
    setSystemData(system_data);
    for (const auto & data : user_data)
        addUserData(data);
}

Conversation::Conversation(std::initializer_list<std::string_view> user_data)
{
    for (const auto & data : user_data)
        addUserData(data);
}

Conversation::Conversation(const std::vector<std::string> & user_data)
{
    for (const auto & data : user_data)
        addUserData(data);
}

bool Conversation::setSystemData(std::string_view data) noexcept(false)
{
    /// Remove existing system message if any
    if (!messages.empty() && messages[0].role == "system")
        messages.erase(messages.begin());
    
    /// Insert new system message at the beginning
    OpenAIClient::ChatCompletionRequest::Message msg;
    msg.role = "system";
    msg.content = std::string(data);
    messages.insert(messages.begin(), std::move(msg));
    
    return true;
}

bool Conversation::popSystemData() noexcept(false)
{
    if (!messages.empty() && messages[0].role == "system")
    {
        messages.erase(messages.begin());
        return true;
    }
    return false;
}

bool Conversation::addUserData(std::string_view data) noexcept(false)
{
    OpenAIClient::ChatCompletionRequest::Message msg;
    msg.role = "user";
    msg.content = std::string(data);
    messages.push_back(std::move(msg));
    return true;
}

bool Conversation::addUserData(std::string_view data, std::string_view name) noexcept(false)
{
    OpenAIClient::ChatCompletionRequest::Message msg;
    msg.role = "user";
    msg.content = std::string(data);
    msg.name = std::string(name);
    messages.push_back(std::move(msg));
    return true;
}

bool Conversation::popUserData() noexcept(false)
{
    if (!messages.empty() && messages.back().role == "user")
    {
        messages.pop_back();
        return true;
    }
    return false;
}

bool Conversation::addAssistantData(std::string_view data) noexcept(false)
{
    OpenAIClient::ChatCompletionRequest::Message msg;
    msg.role = "assistant";
    msg.content = std::string(data);
    messages.push_back(std::move(msg));
    return true;
}


bool Conversation::addFunctionData(std::string_view /*name*/, std::string_view content) noexcept(false)
{
    /// Always use new tool format
    std::string tool_id = getLastToolCallId();
    if (!tool_id.empty())
    {
        return addToolData(tool_id, content);
    }
    
    /// If no tool call ID, something is wrong
    throw std::runtime_error("Cannot add function data without a tool call ID");
}

bool Conversation::addToolData(std::string_view tool_call_id, std::string_view content) noexcept(false)
{
    OpenAIClient::ChatCompletionRequest::Message msg;
    msg.role = "tool";
    msg.tool_call_id = std::string(tool_call_id);
    msg.content = std::string(content);
    messages.push_back(std::move(msg));
    return true;
}

std::string Conversation::getLastResponse() const noexcept
{
    if (!messages.empty() && messages.back().role == "assistant")
        return messages.back().content;
    return {};
}

bool Conversation::popLastResponse() noexcept(false)
{
    if (!messages.empty() && messages.back().role == "assistant")
    {
        messages.pop_back();
        return true;
    }
    return false;
}

bool Conversation::lastResponseIsFunctionCall() const noexcept
{
    if (!messages.empty() && messages.back().role == "assistant")
        return !messages.back().tool_calls.empty();
    return false;
}

std::string Conversation::getLastFunctionCallName() const noexcept(false)
{
    if (lastResponseIsFunctionCall() && !messages.back().tool_calls.empty())
        return messages.back().tool_calls[0].function.name;
    return {};
}

std::string Conversation::getLastFunctionCallArguments() const noexcept(false)
{
    if (lastResponseIsFunctionCall() && !messages.back().tool_calls.empty())
        return messages.back().tool_calls[0].function.arguments;
    return {};
}

std::string Conversation::getLastToolCallId() const noexcept(false)
{
    if (lastResponseIsFunctionCall() && !messages.back().tool_calls.empty())
        return messages.back().tool_calls[0].id;
    return {};
}

bool Conversation::update(const OpenAIClient::ChatCompletionResponse & response) noexcept(false)
{
    if (!response.choices.empty())
    {
        const auto & choice = response.choices[0];
        const auto & msg = choice.message;
        
        OpenAIClient::ChatCompletionRequest::Message new_msg;
        new_msg.role = msg.role;
        new_msg.content = msg.content;
        
        /// Handle tool_calls from response
        if (!msg.tool_calls.empty())
        {
            for (const auto & tc : msg.tool_calls)
            {
                OpenAIClient::ChatCompletionRequest::Message::ToolCall new_tc;
                new_tc.id = tc.id;
                new_tc.type = tc.type;
                new_tc.function.name = tc.function.name;
                new_tc.function.arguments = tc.function.arguments;
                new_msg.tool_calls.push_back(new_tc);
            }
        }
        
        messages.push_back(std::move(new_msg));
        return true;
    }
    return false;
}

bool Conversation::setFunctions(const std::vector<OpenAIClient::FunctionDefinition> & new_functions) noexcept(false)
{
    functions = new_functions;
    return true;
}

void Conversation::popFunctions() noexcept(false)
{
    functions.clear();
}

std::string Conversation::exportToJSON() const noexcept(false)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    
    /// Export messages
    Poco::JSON::Array::Ptr messages_array = new Poco::JSON::Array;
    for (const auto & msg : messages)
    {
        Poco::JSON::Object::Ptr msg_obj = new Poco::JSON::Object;
        msg_obj->set("role", msg.role);
        msg_obj->set("content", msg.content);
        
        if (msg.name.has_value())
            msg_obj->set("name", msg.name.value());
        
        if (!msg.tool_calls.empty())
        {
            Poco::JSON::Array::Ptr tool_calls_array = new Poco::JSON::Array;
            for (const auto & tc : msg.tool_calls)
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
            msg_obj->set("tool_calls", tool_calls_array);
        }
        
        if (msg.tool_call_id.has_value())
            msg_obj->set("tool_call_id", msg.tool_call_id.value());
        
        messages_array->add(msg_obj);
    }
    root->set("messages", messages_array);
    
    /// Export functions if any
    if (!functions.empty())
    {
        Poco::JSON::Array::Ptr functions_array = new Poco::JSON::Array;
        for (const auto & func : functions)
        {
            Poco::JSON::Object::Ptr func_obj = new Poco::JSON::Object;
            func_obj->set("name", func.name);
            func_obj->set("description", func.description);
            
            Poco::JSON::Object::Ptr params_obj = new Poco::JSON::Object;
            params_obj->set("type", func.parameters.type);
            
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
            
            Poco::JSON::Array::Ptr required_array = new Poco::JSON::Array;
            for (const auto & req : func.parameters.required)
                required_array->add(req);
            params_obj->set("required", required_array);
            
            func_obj->set("parameters", params_obj);
            functions_array->add(func_obj);
        }
        root->set("functions", functions_array);
    }
    
    
    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

bool Conversation::importFromJSON(const std::string & json) noexcept(false)
{
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(json);
        const Poco::JSON::Object::Ptr & root = result.extract<Poco::JSON::Object::Ptr>();
        
        clear();
        
        /// Import messages
        if (root->has("messages"))
        {
            const Poco::JSON::Array::Ptr messages_array = root->getArray("messages");
            for (size_t i = 0; i < messages_array->size(); ++i)
            {
                const Poco::JSON::Object::Ptr msg_obj = messages_array->getObject(i);
                OpenAIClient::ChatCompletionRequest::Message msg;
                msg.role = msg_obj->getValue<std::string>("role");
                msg.content = msg_obj->getValue<std::string>("content");
                
                if (msg_obj->has("name"))
                    msg.name = msg_obj->getValue<std::string>("name");
                
                if (msg_obj->has("tool_calls"))
                {
                    const Poco::JSON::Array::Ptr tool_calls_array = msg_obj->getArray("tool_calls");
                    for (size_t j = 0; j < tool_calls_array->size(); ++j)
                    {
                        const Poco::JSON::Object::Ptr tc_obj = tool_calls_array->getObject(j);
                        OpenAIClient::ChatCompletionRequest::Message::ToolCall tc;
                        tc.id = tc_obj->getValue<std::string>("id");
                        tc.type = tc_obj->getValue<std::string>("type");
                        
                        const Poco::JSON::Object::Ptr func_obj = tc_obj->getObject("function");
                        tc.function.name = func_obj->getValue<std::string>("name");
                        tc.function.arguments = func_obj->getValue<std::string>("arguments");
                        
                        msg.tool_calls.push_back(tc);
                    }
                }
                
                if (msg_obj->has("tool_call_id"))
                    msg.tool_call_id = msg_obj->getValue<std::string>("tool_call_id");
                
                messages.push_back(std::move(msg));
            }
        }
        
        /// Import functions if any
        if (root->has("functions"))
        {
            const Poco::JSON::Array::Ptr functions_array = root->getArray("functions");
            for (size_t i = 0; i < functions_array->size(); ++i)
            {
                const Poco::JSON::Object::Ptr func_obj = functions_array->getObject(i);
                OpenAIClient::FunctionDefinition func;
                func.name = func_obj->getValue<std::string>("name");
                func.description = func_obj->getValue<std::string>("description");
                
                const Poco::JSON::Object::Ptr params_obj = func_obj->getObject("parameters");
                func.parameters.type = params_obj->getValue<std::string>("type");
                
                const Poco::JSON::Object::Ptr properties_obj = params_obj->getObject("properties");
                std::vector<std::string> property_names;
                properties_obj->getNames(property_names);
                
                for (const auto & prop_name : property_names)
                {
                    const Poco::JSON::Object::Ptr prop_obj = properties_obj->getObject(prop_name);
                    OpenAIClient::FunctionParameter param;
                    param.type = prop_obj->getValue<std::string>("type");
                    param.description = prop_obj->getValue<std::string>("description");
                    
                    if (prop_obj->has("enum"))
                    {
                        const Poco::JSON::Array::Ptr enum_array = prop_obj->getArray("enum");
                        std::vector<std::string> enum_values;
                        for (size_t j = 0; j < enum_array->size(); ++j)
                            enum_values.push_back(enum_array->getElement<std::string>(j));
                        param.enum_values = enum_values;
                    }
                    
                    func.parameters.properties[prop_name] = param;
                }
                
                const Poco::JSON::Array::Ptr required_array = params_obj->getArray("required");
                for (size_t j = 0; j < required_array->size(); ++j)
                    func.parameters.required.push_back(required_array->getElement<std::string>(j));
                
                functions.push_back(std::move(func));
            }
        }
        
        
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void Conversation::clear() noexcept
{
    messages.clear();
    functions.clear();
}

bool Conversation::isLastMessageRole(const std::string & role) const noexcept
{
    return !messages.empty() && messages.back().role == role;
}

} /// namespace openai
} /// namespace DB
