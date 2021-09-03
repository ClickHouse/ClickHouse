#include <string>
#include <vector>
#include <common/logger_useful.h>
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
    EXPECT_NO_THROW(LOG_INFO(log, "Hello {} World"));
}

TEST(Logger, TestLog)
{
    {   /// Test logs visible for test level
        Poco::Logger::root().setLevel("test");
        std::ostringstream oss;
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss)));
        Poco::Logger * log = &Poco::Logger::get("Log");
        LOG_TEST(log, "Hello World");

        EXPECT_EQ(oss.str(), "Hello World\n");
    }

    {   /// Test logs invisible for other levels
        for (const auto & level : {"trace", "debug", "information", "warning", "fatal"})
        {
            Poco::Logger::root().setLevel(level);
            std::ostringstream oss;
            Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(oss)));
            Poco::Logger * log = &Poco::Logger::get("Log");

            LOG_TEST(log, "Hello World");

            EXPECT_EQ(oss.str(), "");
        }
    }

}
