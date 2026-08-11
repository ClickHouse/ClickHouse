#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIAgent.h>

#include <sstream>

using namespace DB;

namespace
{

/// A transport returning pre-programmed steps and recording the conversations it receives.
class MockTransport : public IAIAgentTransport
{
public:
    explicit MockTransport(std::vector<AIAgentStep> steps_) : steps(std::move(steps_)) {}

    AIAgentStep step(const String &, const ai::Messages & messages, const ai::ToolSet &) override
    {
        conversations.push_back(messages);
        AIAgentStep result = steps.at(std::min(next, steps.size() - 1));
        ++next;
        return result;
    }

    String description() const override { return "mock"; }

    /// The messages passed to each model call. The transport outlives the agent in the tests
    /// (the agent owns a raw observer pointer stored by the test), so this is safe to inspect.
    std::vector<ai::Messages> conversations;

private:
    std::vector<AIAgentStep> steps;
    size_t next = 0;
};

AIAgentStep textStep(const String & text)
{
    AIAgentStep step;
    step.text = text;
    return step;
}

AIAgentStep errorStep(const String & error)
{
    AIAgentStep step;
    step.error = error;
    return step;
}

AIAgentStep toolCallStep(const String & tool_name)
{
    AIAgentStep step;
    step.tool_calls.emplace_back("call_1", tool_name, ai::JsonValue::object());
    return step;
}

struct AgentWithMock
{
    std::shared_ptr<QueryContextBuffer> buffer = std::make_shared<QueryContextBuffer>();
    MockTransport * transport = nullptr;
    std::ostringstream output;
    std::unique_ptr<AIAgent> agent;

    explicit AgentWithMock(std::vector<AIAgentStep> steps)
    {
        auto owned = std::make_unique<MockTransport>(std::move(steps));
        transport = owned.get();
        AIConfiguration config;
        config.max_steps = 4;
        /// The hooks are empty: the tools fail with an error result when called, which is fine
        /// for these tests - the agent loop treats it as an application-level tool failure.
        agent = std::make_unique<AIAgent>(config, std::move(owned), AIAgentHooks{}, buffer, output, /*use_colors=*/ false);
    }
};

String firstUserText(const ai::Messages & messages)
{
    for (const auto & message : messages)
        if (message.role == ai::kMessageRoleUser)
            return message.get_text();
    return {};
}

}

TEST(AIAgent, ResetBaselinesRecentQueries)
{
    AgentWithMock harness({textStep("ok")});

    harness.buffer->startQuery("SELECT 'before-reset'", /*from_ai=*/ false);
    harness.buffer->finishQuery(0.1, false);

    harness.agent->chat("first question");
    ASSERT_EQ(harness.transport->conversations.size(), 1u);
    EXPECT_NE(firstUserText(harness.transport->conversations[0]).find("SELECT 'before-reset'"), String::npos);

    harness.buffer->startQuery("SELECT 'pre-reset'", /*from_ai=*/ false);
    harness.buffer->finishQuery(0.1, false);

    /// `? clear` starts a fresh conversation: the buffered recent-query history is part of the
    /// conversation being cleared, so nothing recorded before the reset may be replayed.
    (*harness.agent).reset();
    harness.agent->chat("second question");

    ASSERT_EQ(harness.transport->conversations.size(), 2u);
    const auto & messages = harness.transport->conversations[1];
    ASSERT_EQ(messages.size(), 1u);
    const String text = messages[0].get_text();
    EXPECT_EQ(text.find("SELECT 'before-reset'"), String::npos);
    EXPECT_EQ(text.find("SELECT 'pre-reset'"), String::npos);
    EXPECT_NE(text.find("second question"), String::npos);

    /// Queries run after the reset are picked up again.
    harness.buffer->startQuery("SELECT 'post-reset'", /*from_ai=*/ false);
    harness.buffer->finishQuery(0.1, false);
    harness.agent->chat("third question");
    ASSERT_EQ(harness.transport->conversations.size(), 3u);
    EXPECT_NE(harness.transport->conversations[2].back().get_text().find("SELECT 'post-reset'"), String::npos);
}

TEST(AIAgent, UserTextIsNotMixedIntoToolResultsAfterFailedStep)
{
    /// First turn: the model calls a tool, then the next model call fails, leaving the history
    /// with a dangling tool-results tail. The next user question must still reach the model as
    /// a proper user message (the server-function transport renders a user message with tool
    /// results as tool results only).
    AgentWithMock harness({toolCallStep("list_databases"), errorStep("connection reset"), textStep("recovered")});

    harness.agent->chat("first question");
    harness.agent->chat("second question");

    ASSERT_EQ(harness.transport->conversations.size(), 3u);
    const auto & messages = harness.transport->conversations[2];

    ASSERT_FALSE(messages.empty());
    const auto & last = messages.back();
    EXPECT_EQ(last.role, ai::kMessageRoleUser);
    EXPECT_FALSE(last.has_tool_results());
    EXPECT_NE(last.get_text().find("second question"), String::npos);

    /// The rendering of the server-function transport keeps the question, too.
    const String rendered = AIServerFunctionTransport::renderConversation(messages);
    EXPECT_NE(rendered.find("second question"), String::npos);
}

TEST(AIAgent, FailedTurnDoesNotMergeIntoNextQuestion)
{
    /// The first model call fails outright, leaving a dangling plain-text user message.
    /// The next question must start a fresh turn instead of being appended to the stale
    /// unanswered prompt.
    AgentWithMock harness({errorStep("provider unavailable"), textStep("recovered")});

    harness.agent->chat("first question");
    harness.agent->chat("second question");

    ASSERT_EQ(harness.transport->conversations.size(), 2u);
    const auto & messages = harness.transport->conversations[1];

    ASSERT_FALSE(messages.empty());
    const auto & last = messages.back();
    EXPECT_EQ(last.role, ai::kMessageRoleUser);
    const String last_text = last.get_text();
    EXPECT_NE(last_text.find("second question"), String::npos);
    EXPECT_EQ(last_text.find("first question"), String::npos);

    /// The failed turn is closed with a synthetic assistant message to keep the roles alternating.
    ASSERT_GE(messages.size(), 3u);
    EXPECT_EQ(messages[messages.size() - 2].role, ai::kMessageRoleAssistant);
}

TEST(AIAgent, DisplaySanitizesControlCharacters)
{
    EXPECT_EQ(
        AIAgentDisplay::sanitizeForTerminal("safe\ntext\twith\x1b[31mansi\x1b]52;c;evil\x07 and \r control \x7f bytes"),
        "safe\ntext\twith[31mansi]52;c;evil and  control  bytes");
}

#endif
