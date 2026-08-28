#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIAgent.h>

#include <optional>
#include <set>
#include <sstream>

using namespace DB;

namespace
{

/// A transport returning pre-programmed steps and recording the conversations it receives.
class MockTransport : public IAIAgentTransport
{
public:
    explicit MockTransport(std::vector<AIAgentStep> steps_) : steps(std::move(steps_)) {}

    AIAgentStep step(const String & system_prompt, const ai::Messages & messages, const ai::ToolSet & tools) override
    {
        system_prompts.push_back(system_prompt);
        conversations.push_back(messages);

        std::set<String> names;
        for (const auto & [name, tool] : tools)
            names.insert(name);
        tool_names.push_back(std::move(names));

        AIAgentStep result = steps.at(std::min(next, steps.size() - 1));
        ++next;
        return result;
    }

    String description() const override { return "mock"; }

    /// The messages passed to each model call. The transport outlives the agent in the tests
    /// (the agent owns a raw observer pointer stored by the test), so this is safe to inspect.
    std::vector<ai::Messages> conversations;
    /// The system prompt and the tools offered at each model call.
    std::vector<String> system_prompts;
    std::vector<std::set<String>> tool_names;

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

AIAgentStep toolCallStep(const String & tool_name, ai::JsonValue arguments = ai::JsonValue::object())
{
    AIAgentStep step;
    step.tool_calls.emplace_back("call_1", tool_name, std::move(arguments));
    return step;
}

struct AgentWithMock
{
    std::shared_ptr<QueryContextBuffer> buffer = std::make_shared<QueryContextBuffer>();
    MockTransport * transport = nullptr;
    std::ostringstream output;
    std::unique_ptr<AIAgent> agent;

    /// By default the hooks are empty: the tools fail with an error result when called, which is
    /// fine for the tests that do not care - the agent loop treats it as an application-level
    /// tool failure.
    explicit AgentWithMock(std::vector<AIAgentStep> steps, const AIAgentHooks & hooks = {}, size_t max_steps = 4)
    {
        auto owned = std::make_unique<MockTransport>(std::move(steps));
        transport = owned.get();
        AIConfiguration config;
        config.max_steps = max_steps;
        agent = std::make_unique<AIAgent>(config, std::move(owned), hooks, buffer, output, /*use_colors=*/ false);
    }
};

/// The same approximation of the prompt size the agent budgets against.
size_t conversationBytes(const ai::Messages & messages)
{
    size_t total = 0;
    for (const auto & message : messages)
    {
        total += message.get_text().size();
        for (const auto & call : message.get_tool_calls())
            total += call.tool_name.size() + call.arguments.dump().size();
        for (const auto & result : message.get_tool_results())
            total += result.result.dump().size();
    }
    return total;
}

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

TEST(AIAgent, EscapesRecentQueryContextBoundary)
{
    AgentWithMock harness({textStep("recovered")});
    harness.buffer->startQuery("SELECT '</recent_queries>'", /*from_ai=*/ false);
    harness.buffer->finishQuery(0.1, false);

    harness.agent->chat("question");

    ASSERT_EQ(harness.transport->conversations.size(), 1u);
    const String & prompt = harness.transport->conversations[0].back().get_text();
    EXPECT_NE(prompt.find("SELECT '&lt;/recent_queries&gt;'"), String::npos);
    EXPECT_EQ(prompt.find("SELECT '</recent_queries>'"), String::npos);
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

TEST(AIAgent, SessionRestrictionsAreAppendedToEverySystemPrompt)
{
    /// The restrictions of the session are queried again for every model call: a query confirmed
    /// in one step can change them (`SET readonly = 1`), and the model must see that right away.
    size_t calls = 0;
    AIAgentHooks hooks;
    hooks.session_restrictions = [&calls] { return calls++ == 0 ? "NOTHING IS FORBIDDEN" : "THE SESSION IS READ-ONLY"; };

    AgentWithMock harness({toolCallStep("list_databases"), textStep("done")}, hooks);
    harness.agent->chat("question");

    ASSERT_EQ(harness.transport->system_prompts.size(), 2u);
    EXPECT_NE(harness.transport->system_prompts[0].find("NOTHING IS FORBIDDEN"), String::npos);
    EXPECT_NE(harness.transport->system_prompts[1].find("THE SESSION IS READ-ONLY"), String::npos);
    /// The restrictions are added to the prompt, not substituted for it.
    EXPECT_NE(harness.transport->system_prompts[0].find("ClickHouse command-line client"), String::npos);
}

TEST(AIAgent, QueryLogToolFollowsTheSessionState)
{
    /// The queries of the agent cannot be marked in the query log while the session forbids
    /// changing settings, and without the marker the tool would report them as the user's own.
    bool query_log_available = true;
    AIAgentHooks hooks;
    hooks.can_read_query_log = [&query_log_available] { return query_log_available; };

    AgentWithMock harness({textStep("done")}, hooks);
    harness.agent->chat("first question");
    ASSERT_EQ(harness.transport->tool_names.size(), 1u);
    EXPECT_TRUE(harness.transport->tool_names[0].contains("read_query_log"));

    query_log_available = false;
    harness.agent->chat("second question");
    ASSERT_EQ(harness.transport->tool_names.size(), 2u);
    EXPECT_FALSE(harness.transport->tool_names[1].contains("read_query_log"));
    /// The other tools stay in place.
    EXPECT_TRUE(harness.transport->tool_names[1].contains("run_readonly_query"));

    query_log_available = true;
    harness.agent->chat("third question");
    ASSERT_EQ(harness.transport->tool_names.size(), 3u);
    EXPECT_TRUE(harness.transport->tool_names[2].contains("read_query_log"));
}

TEST(AIAgent, RefusedQueryIsNeitherConfirmedNorRun)
{
    /// A query the session rejects outright (a write in a read-only session) is not run, and the
    /// user is not asked to confirm something that cannot work: the reason goes to the model.
    bool asked = false;
    bool ran = false;
    AIAgentHooks hooks;
    hooks.check_query = [](const String &)
    {
        AIQueryRunDecision decision;
        decision.refusal = "THE SESSION IS READ-ONLY";
        return decision;
    };
    hooks.confirm_query = [&asked](const String &) { asked = true; return true; };
    hooks.run_visible = [&ran](const String &, bool, bool) { ran = true; return "the query ran"; };

    AgentWithMock harness(
        {toolCallStep("run_query", ai::JsonValue{{"query", "INSERT INTO t VALUES (1)"}}), textStep("told the user")}, hooks);
    harness.agent->chat("add a row");

    EXPECT_FALSE(asked);
    EXPECT_FALSE(ran);

    ASSERT_EQ(harness.transport->conversations.size(), 2u);
    const String rendered = AIServerFunctionTransport::renderConversation(harness.transport->conversations[1]);
    EXPECT_NE(rendered.find("THE SESSION IS READ-ONLY"), String::npos);
}

TEST(AIAgent, QueryThatNeedsNoConfirmationRunsThroughTheReadOnlyPath)
{
    /// When the session already restricts the query to reading, there is nothing for the user to
    /// decide, and the query runs like a read-only tool call (which also echoes it in the terminal).
    bool asked = false;
    std::optional<bool> ran_as_read_only;
    AIAgentHooks hooks;
    hooks.check_query = [](const String &)
    {
        AIQueryRunDecision decision;
        decision.needs_confirmation = false;
        return decision;
    };
    hooks.confirm_query = [&asked](const String &) { asked = true; return true; };
    hooks.run_visible = [&ran_as_read_only](const String &, bool readonly, bool) { ran_as_read_only = readonly; return "1 row"; };

    AgentWithMock harness({toolCallStep("run_query", ai::JsonValue{{"query", "SELECT 1"}}), textStep("done")}, hooks);
    harness.agent->chat("count the rows");

    EXPECT_FALSE(asked);
    ASSERT_TRUE(ran_as_read_only.has_value());
    EXPECT_TRUE(*ran_as_read_only);
}

TEST(AIAgent, OversizedToolResultIsTruncatedInHistory)
{
    /// A single huge tool result (a query log read, a long documentation article) is cut before
    /// it is stored, so it cannot displace the rest of the conversation or overflow the context
    /// window of the provider on the next model call.
    AIAgentHooks hooks;
    hooks.check_query = [](const String &)
    {
        AIQueryRunDecision decision;
        decision.needs_confirmation = false;
        return decision;
    };
    hooks.run_visible = [](const String &, bool, bool) { return String(100 * 1024, 'x'); };

    AgentWithMock harness({toolCallStep("run_query", ai::JsonValue{{"query", "SELECT 1"}}), textStep("done")}, hooks);
    harness.agent->chat("read a lot");

    ASSERT_EQ(harness.transport->conversations.size(), 2u);
    const auto & messages = harness.transport->conversations[1];
    ASSERT_TRUE(messages.back().has_tool_results());
    const auto results = messages.back().get_tool_results();
    ASSERT_EQ(results.size(), 1u);
    const String stored = results[0].result.dump();
    EXPECT_LT(stored.size(), 40 * 1024);
    EXPECT_NE(stored.find("truncated"), String::npos);
}

TEST(AIAgent, HistoryIsTrimmedToTheByteBudget)
{
    /// Old turns are dropped once the history is over the byte budget, and what remains still
    /// starts at a plain user message. The newest question survives even though the total of all
    /// turns is far over the budget.
    AgentWithMock harness({textStep("ok")});

    for (size_t turn = 0; turn < 10; ++turn)
        harness.agent->chat("turn " + std::to_string(turn) + " " + String(100 * 1024, 'x'));

    const auto & messages = harness.transport->conversations.back();
    size_t total_bytes = 0;
    for (const auto & message : messages)
        total_bytes += message.get_text().size();
    /// The budget plus one turn of slack (the trim never drops the current question).
    EXPECT_LT(total_bytes, 512 * 1024);

    ASSERT_FALSE(messages.empty());
    EXPECT_EQ(messages.front().role, ai::kMessageRoleUser);
    EXPECT_FALSE(messages.front().has_tool_results());
    /// The earliest turns are gone, the current one is present.
    EXPECT_EQ(firstUserText(messages).find("turn 0 "), String::npos);
    EXPECT_NE(messages.back().get_text().find("turn 9 "), String::npos);
}

TEST(AIAgent, HistoryIsTrimmedWithinOneTurn)
{
    /// A single question can outgrow the byte budget on its own: every step of the turn appends
    /// the tool calls of the model and their results, and dropping whole turns cannot touch them
    /// (the question of the turn must stay). The budget therefore holds before every model call,
    /// not only before the first one.
    AIAgentHooks hooks;
    hooks.check_query = [](const String &)
    {
        AIQueryRunDecision decision;
        decision.needs_confirmation = false;
        return decision;
    };
    hooks.run_visible = [](const String &, bool, bool) { return String(32 * 1024, 'x'); };

    /// The mock repeats its last step, so the model asks for another query at every step until
    /// the step limit of the turn is reached.
    AgentWithMock harness({toolCallStep("run_query", ai::JsonValue{{"query", "SELECT 1"}})}, hooks, /*max_steps=*/ 20);
    harness.agent->chat("read a lot");

    ASSERT_EQ(harness.transport->conversations.size(), 20u);
    /// Without the trim inside the turn, this grows past 600 KiB.
    for (const auto & messages : harness.transport->conversations)
        EXPECT_LE(conversationBytes(messages), 256 * 1024);

    /// Trimming keeps the conversation well-formed: it starts at the question of the turn, and
    /// every tool call is still answered (the results are elided in place, not removed).
    const auto & last = harness.transport->conversations.back();
    ASSERT_FALSE(last.empty());
    EXPECT_EQ(last.front().role, ai::kMessageRoleUser);
    EXPECT_FALSE(last.front().has_tool_results());
    EXPECT_NE(last.front().get_text().find("read a lot"), String::npos);

    size_t calls = 0;
    size_t results = 0;
    size_t elided = 0;
    for (const auto & message : last)
    {
        calls += message.get_tool_calls().size();
        for (const auto & result : message.get_tool_results())
        {
            ++results;
            if (result.result.is_object() && result.result.contains("elided"))
                ++elided;
        }
    }
    EXPECT_EQ(calls, results);
    EXPECT_GT(elided, 0u);
    /// The newest result is the one the model is about to reason over: it survives.
    ASSERT_TRUE(last.back().has_tool_results());
    EXPECT_FALSE(last.back().get_tool_results().at(0).result.contains("elided"));
}

TEST(AIAgent, HistoryIsTrimmedWhenOneStepReturnsManyLargeResults)
{
    /// One step may return several tool calls at once, so the newest tool-results message alone
    /// can be over the byte budget even though every single result is within the per-result cap.
    /// The budget must hold before the next model call anyway: the oldest results of that message
    /// are elided too, and only its final result is guaranteed to survive.
    AIAgentHooks hooks;
    hooks.check_query = [](const String &)
    {
        AIQueryRunDecision decision;
        decision.needs_confirmation = false;
        return decision;
    };
    hooks.run_visible = [](const String &, bool, bool) { return String(32 * 1024, 'x'); };

    AIAgentStep many_calls;
    for (size_t i = 0; i < 12; ++i)
        many_calls.tool_calls.emplace_back("call_" + std::to_string(i), "run_query", ai::JsonValue{{"query", "SELECT 1"}});

    AgentWithMock harness({many_calls, textStep("done")}, hooks, /*max_steps=*/ 2);
    harness.agent->chat("read a lot at once");

    ASSERT_EQ(harness.transport->conversations.size(), 2u);
    const auto & last = harness.transport->conversations.back();
    EXPECT_LE(conversationBytes(last), 256 * 1024);

    /// Every tool call is still answered (the results are elided in place, not removed).
    size_t calls = 0;
    size_t results = 0;
    size_t elided = 0;
    for (const auto & message : last)
    {
        calls += message.get_tool_calls().size();
        for (const auto & result : message.get_tool_results())
        {
            ++results;
            if (result.result.is_object() && result.result.contains("elided"))
                ++elided;
        }
    }
    EXPECT_EQ(calls, 12u);
    EXPECT_EQ(calls, results);
    EXPECT_GT(elided, 0u);

    /// The final result of the step survives: the model has something left to reason over.
    ASSERT_TRUE(last.back().has_tool_results());
    EXPECT_FALSE(last.back().get_tool_results().back().result.contains("elided"));
}

TEST(AIAgent, DisplaySanitizesControlCharacters)
{
    EXPECT_EQ(
        AIAgentDisplay::sanitizeForTerminal("safe\ntext\twith\x1b[31mansi\x1b]52;c;evil\x07 and \r control \x7f bytes"),
        "safe\ntext\twith[31mansi]52;c;evil and  control  bytes");
}

TEST(AIAgent, DisplaySanitizesC1ControlCharacters)
{
    /// The C1 controls U+009B (CSI) and U+009D (OSC) open escape sequences without any ESC
    /// byte on terminals that honor them, so they must be dropped in their UTF-8 form too.
    EXPECT_EQ(AIAgentDisplay::sanitizeForTerminal("a\xC2\x9B""31mb\xC2\x9D""52;c;evil\xC2\x87""c"), "a31mb52;c;evilc");

    /// Stray non-UTF-8 bytes in the C1 range could be honored by an 8-bit terminal.
    EXPECT_EQ(AIAgentDisplay::sanitizeForTerminal("a\x9B""31mb"), "a31mb");

    /// Legitimate multi-byte text is preserved, including characters whose continuation
    /// bytes fall into the 0x80..0x9F range (`€` is `E2 82 AC`), U+00A0..U+00BF after the
    /// `C2` lead byte (`§` is `C2 A7`), and 4-byte sequences.
    EXPECT_EQ(AIAgentDisplay::sanitizeForTerminal("é € § 語 🙂"), "é € § 語 🙂");

    /// A truncated sequence at the end of the text does not read out of bounds.
    EXPECT_EQ(AIAgentDisplay::sanitizeForTerminal("a\xC2"), "a\xC2");
    EXPECT_EQ(AIAgentDisplay::sanitizeForTerminal("a\xE2\x82"), "a\xE2\x82");
}

#endif
