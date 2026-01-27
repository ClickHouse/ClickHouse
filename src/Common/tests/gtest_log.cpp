#include <string>
#include <vector>
#include <base/sleep.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <gtest/gtest.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/NullChannel.h>
#include <Poco/StreamChannel.h>
#include <Poco/TemporaryFile.h>
#include <filesystem>
#include <fstream>
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
    threads.reserve(threads_count);

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

TEST(Logger, ExceptionsPropagatedFromArguments)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss));
    auto log = createLogger("Logger", my_channel.get());
    log->setLevel("trace");

    std::optional<int> empty_optional;
    EXPECT_THROW(LOG_TRACE(log, "my value is {}", empty_optional.value()), std::bad_optional_access);
}

class AlwaysFailingChannel : public Poco::StreamChannel
{
public:
    explicit AlwaysFailingChannel(std::ostream & str)
        : Poco::StreamChannel(str)
    {
    }

    void log(const Poco::Message &) override { throw Poco::Exception("Exception from AlwaysFailingChannel"); }
};

TEST(Logger, ExceptionsFromPocoLoggerAreNotPropagated)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto my_channel = Poco::AutoPtr<Poco::StreamChannel>(new AlwaysFailingChannel(oss));
    auto log = createLogger("Logger", my_channel.get());
    log->setLevel("trace");

    EXPECT_NO_THROW(LOG_TRACE(log, "my value is {}", 42));

    EXPECT_EQ(oss.str(), "");
}

std::vector<std::string> getLogFileNames(const std::string & log_dir)
{
    std::vector<std::string> log_file_names;
    for (const auto & entry : std::filesystem::directory_iterator(log_dir))
        log_file_names.emplace_back(entry.path().filename());
    std::sort(log_file_names.begin(), log_file_names.end());
    return log_file_names;
}

std::string readLogFile(const std::string & path, bool skip_comments = true)
{
    std::string file_contents;
    std::ifstream input(path);
    while (input)
    {
        std::string line;
        std::getline(input, line, '\n');
        if (!line.empty() && (!skip_comments || !line.starts_with("#")))
            file_contents.append(line).append("\n");
    }
    return file_contents;
}

TEST(Logger, Rotation)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();

    auto my_channel = Poco::AutoPtr<Poco::FileChannel>(new Poco::FileChannel());
    std::filesystem::path log_dir = std::filesystem::path{temp_dir.path()};
    my_channel->setProperty(Poco::FileChannel::PROP_PATH, log_dir / "logger.log");
    my_channel->setProperty(Poco::FileChannel::PROP_ROTATION, "200, 100 milliseconds");
    auto log = createLogger("Logger", my_channel.get());
    log->setLevel("trace");

    LOG_INFO(log, "A");
    LOG_INFO(log, "B");
    LOG_INFO(log, "{}", std::string(201, 'C')); /// This should cause a rotation.

    LOG_INFO(log, "D");
    LOG_INFO(log, "E");

    sleepForMilliseconds(101); /// This should cause a rotation.

    LOG_INFO(log, "F");

    /// We expect three log files at this point: "logger.log.1", "logger.log.0", "logger.log".
    std::vector<std::string> expected_log_file_names{"logger.log", "logger.log.0", "logger.log.1"};
    EXPECT_EQ(getLogFileNames(log_dir), expected_log_file_names);
    EXPECT_EQ(readLogFile(log_dir / "logger.log.1"), "A\nB\n" + std::string(201, 'C') + "\n");
    EXPECT_EQ(readLogFile(log_dir / "logger.log.0"), "D\nE\n");
    EXPECT_EQ(readLogFile(log_dir / "logger.log"), "F\n");
}
