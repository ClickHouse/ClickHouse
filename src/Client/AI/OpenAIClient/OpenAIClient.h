#pragma once

#include <string>
#include <optional>
#include <vector>
#include <map>
#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{
namespace openai
{

class OpenAIClient
{
public:
    struct FunctionParameter
    {
        std::string type;
        std::string description;
        std::optional<std::vector<std::string>> enum_values;
    };

    struct FunctionDefinition
    {
        std::string name;
        std::string description;
        struct Parameters
        {
            std::string type = "object";
            std::map<std::string, FunctionParameter> properties;
            std::vector<std::string> required;
        } parameters;
    };

    struct ChatCompletionRequest
    {
        std::string model;
        struct Message
        {
            std::string role;
            std::string content;
            std::optional<std::string> name; /// For function messages
            std::optional<std::string> tool_call_id; /// For tool response messages
            struct ToolCall
            {
                std::string id;
                std::string type; /// Should be "function"
                struct Function
                {
                    std::string name;
                    std::string arguments; /// JSON string
                } function;
            };
            std::vector<ToolCall> tool_calls;
        };
        std::vector<Message> messages;
        std::optional<Float32> temperature;
        std::optional<UInt32> max_tokens;
        std::optional<std::vector<FunctionDefinition>> tools;
        std::optional<std::string> tool_choice; /// "auto", "none", "required", or specific function
    };

    struct ChatCompletionResponse
    {
        std::string id;
        std::string object;
        UInt64 created;
        std::string model;
        struct Choice
        {
            UInt32 index;
            struct Message
            {
                std::string role;
                std::string content;
                struct ToolCall
                {
                    std::string id;
                    std::string type; /// Should be "function"
                    struct Function
                    {
                        std::string name;
                        std::string arguments; /// JSON string
                    } function;
                };
                std::vector<ToolCall> tool_calls;
            } message;
            std::string finish_reason;
        };
        std::vector<Choice> choices;
        struct Usage
        {
            UInt32 prompt_tokens;
            UInt32 completion_tokens;
            UInt32 total_tokens;
        } usage;
    };

    explicit OpenAIClient(const std::string & api_key, const std::string & base_url = "https://api.openai.com");

    ChatCompletionResponse createChatCompletion(const ChatCompletionRequest & request);

private:
    std::string api_key;
    std::string base_url;
};

} /// namespace openai
} /// namespace DB
