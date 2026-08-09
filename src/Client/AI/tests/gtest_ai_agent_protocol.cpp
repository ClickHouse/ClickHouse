#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIAgentTransport.h>

using namespace DB;

TEST(AIAgentProtocol, ParsePlainText)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse("Just a plain answer.", counter);
    EXPECT_EQ(step.text, "Just a plain answer.");
    EXPECT_TRUE(step.tool_calls.empty());
    EXPECT_TRUE(step.ok());
}

TEST(AIAgentProtocol, ParseSingleToolCall)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse(
        "Let me check the schema.\n"
        "<tool_call>{\"name\": \"list_tables\", \"arguments\": {\"database\": \"default\"}}</tool_call>",
        counter);

    EXPECT_EQ(step.text, "Let me check the schema.");
    ASSERT_EQ(step.tool_calls.size(), 1u);
    EXPECT_EQ(step.tool_calls[0].tool_name, "list_tables");
    EXPECT_EQ(step.tool_calls[0].arguments.at("database").get<std::string>(), "default");
    EXPECT_EQ(step.tool_calls[0].id, "call_1");
    EXPECT_EQ(counter, 2u);
}

TEST(AIAgentProtocol, ParseMultipleToolCallsAndSurroundingText)
{
    size_t counter = 5;
    auto step = AIServerFunctionTransport::parseResponse(
        "First.\n"
        "<tool_call>{\"name\": \"a\", \"arguments\": {}}</tool_call>\n"
        "Between.\n"
        "<tool_call>{\"name\": \"b\", \"arguments\": {\"x\": 1}}</tool_call>\n"
        "After.",
        counter);

    ASSERT_EQ(step.tool_calls.size(), 2u);
    EXPECT_EQ(step.tool_calls[0].tool_name, "a");
    EXPECT_EQ(step.tool_calls[0].id, "call_5");
    EXPECT_EQ(step.tool_calls[1].tool_name, "b");
    EXPECT_EQ(step.tool_calls[1].id, "call_6");
    EXPECT_EQ(step.tool_calls[1].arguments.at("x").get<int>(), 1);
    EXPECT_EQ(step.text, "First.\n\nBetween.\n\nAfter.");
}

TEST(AIAgentProtocol, ParseArgumentsAsEncodedString)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse(
        R"(<tool_call>{"name": "t", "arguments": "{\"q\": \"SELECT 1\"}"}</tool_call>)", counter);

    ASSERT_EQ(step.tool_calls.size(), 1u);
    EXPECT_EQ(step.tool_calls[0].tool_name, "t");
    EXPECT_EQ(step.tool_calls[0].arguments.at("q").get<std::string>(), "SELECT 1");
}

TEST(AIAgentProtocol, ParseMalformedToolCall)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse("<tool_call>{not json}</tool_call>", counter);

    ASSERT_EQ(step.tool_calls.size(), 1u);
    EXPECT_EQ(step.tool_calls[0].tool_name, AIServerFunctionTransport::malformed_tool_call_name);
    EXPECT_EQ(step.tool_calls[0].arguments.at("raw").get<std::string>(), "{not json}");
}

TEST(AIAgentProtocol, ParseUnclosedToolCall)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse(
        R"(Text <tool_call>{"name": "t", "arguments": {}})", counter);

    ASSERT_EQ(step.tool_calls.size(), 1u);
    EXPECT_EQ(step.tool_calls[0].tool_name, "t");
    EXPECT_EQ(step.text, "Text");
}

TEST(AIAgentProtocol, ParseMissingName)
{
    size_t counter = 1;
    auto step = AIServerFunctionTransport::parseResponse("<tool_call>{\"arguments\": {}}</tool_call>", counter);

    ASSERT_EQ(step.tool_calls.size(), 1u);
    EXPECT_EQ(step.tool_calls[0].tool_name, AIServerFunctionTransport::malformed_tool_call_name);
}

TEST(AIAgentProtocol, RenderConversationRoundTrip)
{
    ai::Messages messages;
    messages.push_back(ai::Message::user("show tables"));
    messages.push_back(ai::Message::assistant_with_tools(
        "Checking.", {ai::ToolCallContentPart{"call_1", "list_tables", ai::JsonValue{{"database", "default"}}}}));
    messages.push_back(ai::Message::tool_results({ai::ToolResultContentPart{
        "call_1", ai::JsonValue{{"success", true}, {"result", "t1\nt2"}}, false}}));
    messages.push_back(ai::Message::assistant("Tables: t1, t2."));

    String rendered = AIServerFunctionTransport::renderConversation(messages);

    EXPECT_NE(rendered.find("User:\nshow tables"), String::npos);
    EXPECT_NE(rendered.find("<tool_call>"), String::npos);
    EXPECT_NE(rendered.find("\"name\":\"list_tables\""), String::npos);
    EXPECT_NE(rendered.find("Tool result [call_1]:\nt1\nt2"), String::npos);
    EXPECT_NE(rendered.find("Tables: t1, t2."), String::npos);
    /// The transcript must end with a cue for the model to continue as the assistant.
    EXPECT_TRUE(rendered.ends_with("Assistant:\n"));
}

TEST(AIAgentProtocol, RenderSystemPromptListsTools)
{
    ai::ToolSet tools;
    tools["my_tool"] = ai::create_tool_schema(
        "Does things.", ai::JsonValue{{"type", "object"}, {"properties", ai::JsonValue::object()}});

    String rendered = AIServerFunctionTransport::renderSystemPrompt("SYSTEM.", tools);

    EXPECT_TRUE(rendered.starts_with("SYSTEM."));
    EXPECT_NE(rendered.find("my_tool"), String::npos);
    EXPECT_NE(rendered.find("Does things."), String::npos);
    EXPECT_NE(rendered.find("<tool_call>"), String::npos);
}

#endif
