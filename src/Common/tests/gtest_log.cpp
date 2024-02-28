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
#include <thread>


TEST(Logger, Log)
{
    Poco::Logger::root().setLevel("none");
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    LoggerPtr log = getLogger("Log");

    /// This test checks that we don't pass this string to fmtlib, because it is the only argument.
    EXPECT_NO_THROW(LOG_INFO(log, fmt::runtime("Hello {} World")));
}

TEST(Logger, TestLog)
{
    {   /// Test logs visible for test level

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
        auto log = createLogger("TestLogger", my_channel.get());
        log->setLevel("test");
        LOG_TEST(log, "Hello World");

        EXPECT_EQ(oss.str(), "Hello World\n");
    }

    {   /// Test logs invisible for other levels
        for (const auto & level : {"trace", "debug", "information", "warning", "error", "fatal"})
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
            auto log = createLogger(std::string{level} + "_Logger", my_channel.get());
            log->setLevel(level);
            LOG_TEST(log, "Hello World");

            EXPECT_EQ(oss.str(), "");
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
    auto log = createLogger("Logger", my_channel.get());
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

TEST(Logger, SharedRawLogger)
{
    {
        std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        auto stream_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(stream));

        auto shared_logger = getLogger("Logger_1");
        shared_logger->setChannel(stream_channel.get());
        shared_logger->setLevel("trace");

        LOG_TRACE(shared_logger, "SharedLogger1Log1");
        LOG_TRACE(getRawLogger("Logger_1"), "RawLogger1Log");
        LOG_TRACE(shared_logger, "SharedLogger1Log2");

        auto actual = stream.str();
        EXPECT_EQ(actual, "SharedLogger1Log1\nRawLogger1Log\nSharedLogger1Log2\n");
    }
    {
        std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        auto stream_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(stream));

        auto * raw_logger = getRawLogger("Logger_2");
        raw_logger->setChannel(stream_channel.get());
        raw_logger->setLevel("trace");

        LOG_TRACE(getLogger("Logger_2"), "SharedLogger2Log1");
        LOG_TRACE(raw_logger, "RawLogger2Log");
        LOG_TRACE(getLogger("Logger_2"), "SharedLogger2Log2");

        auto actual = stream.str();
        EXPECT_EQ(actual, "SharedLogger2Log1\nRawLogger2Log\nSharedLogger2Log2\n");
    }
}

TEST(Logger, SharedLoggersThreadSafety)
{
    static size_t threads_count = std::thread::hardware_concurrency();
    static constexpr size_t loggers_count = 10;
    static constexpr size_t logger_get_count = 1000;

    Poco::Logger::root();

    std::vector<std::string> names;

    Poco::Logger::names(names);
    size_t loggers_size_before = names.size();

    std::vector<std::thread> threads;

    for (size_t thread_index = 0; thread_index < threads_count; ++thread_index)
    {
        threads.emplace_back([]()
        {
            for (size_t logger_index = 0; logger_index < loggers_count; ++logger_index)
            {
                for (size_t iteration = 0; iteration < logger_get_count; ++iteration)
                {
                    getLogger("Logger_" + std::to_string(logger_index));
                }
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    Poco::Logger::names(names);
    size_t loggers_size_after = names.size();

    EXPECT_EQ(loggers_size_before, loggers_size_after);
}
