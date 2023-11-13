#include <string>
#include <vector>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <gtest/gtest.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/NullChannel.h>
#include <Poco/StreamChannel.h>
#include <sstream>


TEST(Logger, Log)
{
    Poco::Logger::root().setLevel("none");
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    Poco::Logger * log = &Poco::Logger::get("Log");

    /// This test checks that we don't pass this string to fmtlib, because it is the only argument.
    EXPECT_NO_THROW(LOG_INFO(log, fmt::runtime("Hello {} World")));
}

TEST(Logger, TestLog)
{
    {   /// Test logs visible for test level

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
        auto * log = &Poco::Logger::create("TestLogger", my_channel.get());
        log->setLevel("test");
        LOG_TEST(log, "Hello World");

        EXPECT_EQ(oss.str(), "Hello World\n");
        Poco::Logger::destroy("TestLogger");
    }

    {   /// Test logs invisible for other levels
        for (const auto & level : {"trace", "debug", "information", "warning", "error", "fatal"})
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
            auto * log = &Poco::Logger::create(std::string{level} + "_Logger", my_channel.get());
            log->setLevel(level);
            LOG_TEST(log, "Hello World");

            EXPECT_EQ(oss.str(), "");

            Poco::Logger::destroy(std::string{level} + "_Logger");
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
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
    auto * log = &Poco::Logger::create("Logger", my_channel.get());
    log->setLevel("trace");

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
