#pragma once

#include <Client/AI/OpenAIClient/OpenAIClient.h>
#include <string>
#include <vector>
#include <optional>

namespace DB
{
namespace openai
{

/// Manages conversation state for OpenAI chat interactions
class Conversation
{
public:
    /// Constructors
    Conversation() = default;
    Conversation(const Conversation & other) = default;
    Conversation(Conversation && other) noexcept = default;
    explicit Conversation(std::string_view system_data);
    Conversation(std::string_view system_data, std::string_view user_data);
    Conversation(std::string_view system_data, std::initializer_list<std::string_view> user_data);
    Conversation(std::initializer_list<std::string_view> user_data);
    explicit Conversation(const std::vector<std::string> & user_data);
    
    /// Assignment operators
    Conversation & operator=(const Conversation & other) = default;
    Conversation & operator=(Conversation && other) noexcept = default;
    
    /// System message management
    bool setSystemData(std::string_view data) noexcept(false);
    bool popSystemData() noexcept(false);
    
    /// User message management
    bool addUserData(std::string_view data) noexcept(false);
    bool addUserData(std::string_view data, std::string_view name) noexcept(false);
    bool popUserData() noexcept(false);
    
    /// Assistant message management
    bool addAssistantData(std::string_view data) noexcept(false);
    bool addAssistantData(std::string_view data, const OpenAIClient::ChatCompletionRequest::Message::FunctionCall & function_call) noexcept(false);
    
    /// Function message management
    bool addFunctionData(std::string_view name, std::string_view content) noexcept(false);
    
    /// Response handling
    std::string getLastResponse() const noexcept;
    bool popLastResponse() noexcept(false);
    
    /// Function call handling
    bool lastResponseIsFunctionCall() const noexcept;
    std::string getLastFunctionCallName() const noexcept(false);
    std::string getLastFunctionCallArguments() const noexcept(false);
    
    /// Update conversation with response
    bool update(const OpenAIClient::ChatCompletionResponse & response) noexcept(false);
    
    /// Function definitions
    bool setFunctions(const std::vector<OpenAIClient::FunctionDefinition> & functions) noexcept(false);
    void popFunctions() noexcept(false);
    bool hasFunctions() const noexcept { return !functions.empty(); }
    const std::vector<OpenAIClient::FunctionDefinition> & getFunctions() const noexcept { return functions; }
    
    /// Function call control
    void setFunctionCallMode(const std::string & mode) noexcept(false) { function_call_mode = mode; }
    const std::optional<std::string> & getFunctionCallMode() const noexcept { return function_call_mode; }
    
    /// Export/Import
    std::string exportToJSON() const noexcept(false);
    bool importFromJSON(const std::string & json) noexcept(false);
    
    /// Get messages for API request
    const std::vector<OpenAIClient::ChatCompletionRequest::Message> & getMessages() const noexcept { return messages; }
    
    /// Clear conversation
    void clear() noexcept;
    
    /// Check if conversation is empty
    bool empty() const noexcept { return messages.empty(); }
    
    /// Get conversation size
    size_t size() const noexcept { return messages.size(); }

private:
    std::vector<OpenAIClient::ChatCompletionRequest::Message> messages;
    std::vector<OpenAIClient::FunctionDefinition> functions;
    std::optional<std::string> function_call_mode; /// "auto", "none", or specific function name
    
    /// Helper to check if last message is of specific role
    bool isLastMessageRole(const std::string & role) const noexcept;
};

} /// namespace openai
} /// namespace DB
