#include <gtest/gtest.h>
#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/logger_useful.h>

#include <DataTypes/DataTypeString.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/StreamChannel.h>

#include "delta_kernel_ffi.hpp"


class DeltaKernelTest : public testing::Test
{
public:
    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);

        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            Poco::Logger::root().setLevel(test_log_level);
        else
            Poco::Logger::root().setLevel("none");
    }

    void TearDown() override {}
};


TEST_F(DeltaKernelTest, ExpressionVisitor)
{
    auto * expression = ffi::get_testing_kernel_expression();
    try
    {
        auto dag = DeltaLake::visitExpression(
            expression,
            DB::NamesAndTypesList({DB::NameAndTypePair("col", std::make_shared<DB::DataTypeString>())}));
    }
    catch (DB::Exception & e)
    {
        const std::string & message = e.message();
        if (e.code() == DB::ErrorCodes::NOT_IMPLEMENTED && message == "Method DIVIDE not implemented")
        {
            /// Implementation is not full at this moment, but
            /// there is a lot of staff before we get to IN method,
            /// so let's make sure everything before IN works.
            return;
        }
        LOG_ERROR(getLogger("Test"), "Exception: {}", message);
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(false);
}

#endif
