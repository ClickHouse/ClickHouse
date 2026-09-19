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

/// The binary stream of a single changed setting ends with its value as a varint; a value below 128 is one byte.
String serializeMaxBlockSize(UInt64 value)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::max_block_size] = value;
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
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
    ASSERT_EQ(static_cast<unsigned char>(stream.back()), 1);

    QueryPlanSerializationSettings valid;
    ReadBufferFromString valid_in(stream);
    valid.readBinary(valid_in);
    EXPECT_EQ(valid[QueryPlanSerializationSetting::max_block_size], 1u);

    stream.back() = 0;
    QueryPlanSerializationSettings invalid;
    EXPECT_TRUE(throwsBadArguments([&]
    {
        ReadBufferFromString in(stream);
        invalid.readBinary(in);
    }));
}
