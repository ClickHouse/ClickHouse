#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsNonZeroUInt64 max_block_size;
}

/// A serialized query plan is data from another server. The SQL setting `max_block_size` cannot be zero, and every
/// step that reads the plan-level value uses it as a block row limit, so a zero must be rejected while the plan is
/// being read, before any step is constructed with it.
namespace
{

bool throwsBadArguments(auto && action)
{
    try
    {
        action();
        return false;
    }
    catch (const Exception & e)
    {
        return e.code() == ErrorCodes::BAD_ARGUMENTS;
    }
}

/// The stream of a single changed setting ends with its value as a string, followed by the empty name that ends the
/// stream. A value below 10 is one character, just before the terminating zero byte.
String serializeMaxBlockSize(UInt64 value)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_block_size] = value;
    WriteBufferFromOwnString out;
    settings.write(out);
    return out.str();
}

}

TEST(QueryPlanSerializationSettings, MaxBlockSizeRejectsZeroOnAssignment)
{
    QueryPlanSerializationSettings settings;
    EXPECT_TRUE(throwsBadArguments([&] { settings[QueryPlanSerializationSetting::max_block_size] = 0; }));
}

TEST(QueryPlanSerializationSettings, MaxBlockSizeRejectsZeroOnRead)
{
    String stream = serializeMaxBlockSize(1);
    ASSERT_GE(stream.size(), 2u);
    ASSERT_EQ(stream.back(), '\0');
    ASSERT_EQ(stream[stream.size() - 2], '1');

    QueryPlanSerializationSettings valid;
    ReadBufferFromString valid_in(stream);
    valid.read(valid_in);
    EXPECT_EQ(valid[QueryPlanSerializationSetting::max_block_size], 1u);

    stream[stream.size() - 2] = '0';
    QueryPlanSerializationSettings invalid;
    EXPECT_TRUE(throwsBadArguments([&]
    {
        ReadBufferFromString in(stream);
        invalid.read(in);
    }));
}
