#include <string>
#include <vector>
#include <base/logger_useful.h>
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
