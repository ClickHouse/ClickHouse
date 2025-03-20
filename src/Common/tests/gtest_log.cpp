#include <exception>
#include <memory>
#include <string>
#include <vector>
#include "Common/Exception.h"
#include <Common/QuillLogger.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <gtest/gtest.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/NullChannel.h>
#include <Poco/StreamChannel.h>

#include <base/scope_guard.h>

#include <sstream>

class StdStreamSink : public quill::Sink
{
public:
    StdStreamSink() = default;

    QUILL_ATTRIBUTE_HOT void write_log(
        quill::MacroMetadata const *,
        uint64_t,
        std::string_view,
        std::string_view,
        std::string const &,
        std::string_view,
        quill::LogLevel,
        std::string_view,
        std::string_view,
        std::vector<std::pair<std::string, std::string>> const *,
        std::string_view,
        std::string_view log_statement) override
    {
        oss << log_statement;
    }

    void resetStream()
    {
        oss.str("");
    }

    std::string getString()
    {
        return oss.str();
    }

    QUILL_ATTRIBUTE_HOT void flush_sink() override { }

private:
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
};

TEST(Logger, Log)
{
    DB::startQuillBackend();
    createRootLogger({});
    LoggerPtr log = getLogger("Log");
    /// This test checks that we don't pass this string to fmtlib, because it is the only argument.
    try
    {
        LOG_INFO(log, fmt::runtime("Hello {} World"));
    }
    catch (...)
    {
        FAIL() << "Unexpected error: " << DB::getCurrentExceptionMessage(true);
    }
}

TEST(Logger, TestLog)
{
    DB::startQuillBackend();

    auto sink = DB::QuillFrontend::create_or_get_sink<StdStreamSink>("StreamSink");
    auto stream_sink = std::static_pointer_cast<StdStreamSink>(sink);
    auto log = createLogger("TestLogger", {sink});

    {
        SCOPED_TRACE("Test logs visible for test level");
        stream_sink->resetStream();
        log->setLogLevel("test");
        LOG_TEST(log, "Hello World");
        log->flushLogs();
        EXPECT_EQ(stream_sink->getString(), "Hello World\n");
    }

    {
        SCOPED_TRACE("Test logs invisible for other levels");
        for (const auto & level : {"trace", "debug", "information", "warning", "error", "fatal"})
        {
            stream_sink->resetStream();
            log->setLogLevel(level);
            LOG_TEST(log, "Hello World");
            log->flushLogs();
            EXPECT_EQ(stream_sink->getString(), "");
        }
    }
}

static size_t global_counter = 0;

static std::string getLogMessage()
{
    ++global_counter;
    return "test1 " + std::to_string(thread_local_rng());
}

static size_t getLogMessageParam()
{
    ++global_counter;
    return thread_local_rng();
}

static PreformattedMessage getPreformatted()
{
    ++global_counter;
    return PreformattedMessage::create("test3 {}", thread_local_rng());
}

static size_t getLogMessageParamOrThrow()
{
    size_t x = thread_local_rng();
    if (x % 1000 == 0)
        return x;
    throw Poco::Exception("error", 42);
}

TEST(Logger, SideEffects)
{
    DB::startQuillBackend();

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto sink = DB::QuillFrontend::create_or_get_sink<StdStreamSink>("StreamSink");
    auto stream_sink = std::static_pointer_cast<StdStreamSink>(sink);
    auto log = createLogger("Logger", {sink});
    log->setLogLevel("trace");

    stream_sink->resetStream();

    /// Ensure that parameters are evaluated only once
    global_counter = 0;
    LOG_TRACE(log, fmt::runtime(getLogMessage()));
    EXPECT_EQ(global_counter, 1);
    LOG_TRACE(log, "test2 {}", getLogMessageParam());
    EXPECT_EQ(global_counter, 2);
    LOG_TRACE(log, getPreformatted());
    EXPECT_EQ(global_counter, 3);

    auto var = PreformattedMessage::create("test4 {}", thread_local_rng());
    LOG_TRACE(log, var);
    EXPECT_EQ(var.text.starts_with("test4 "), true);
    EXPECT_EQ(var.format_string, "test4 {}");

    LOG_TRACE(log, "test no throw {}", getLogMessageParamOrThrow());
}
