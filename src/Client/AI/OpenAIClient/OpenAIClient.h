#pragma once

#include <string>
#include <optional>
#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

class OpenAIClient
{
public:
    struct ChatCompletionRequest
    {
        std::string model;
        struct Message
        {
            std::string role;
            std::string content;
        };
        std::vector<Message> messages;
        std::optional<Float32> temperature;
        std::optional<UInt32> max_tokens;
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

}
