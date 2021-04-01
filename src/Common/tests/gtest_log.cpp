#include <string>
#include <vector>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/NullChannel.h>


TEST(Logger, Log)
{
    Poco::Logger::root().setLevel("none");
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    Poco::Logger * log = &Poco::Logger::get("Log");

    /// This test checks that we don't pass this string to fmtlib, because it is the only argument.
    EXPECT_NO_THROW(LOG_INFO(log, "Hello {} World"));
}
