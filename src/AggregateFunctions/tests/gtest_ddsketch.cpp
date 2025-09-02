#include <string>
#include <vector>
#include <base/types.h>

#include <AggregateFunctions/DDSketch.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Base64.h>

#include <gtest/gtest.h>

TEST(DDSketch, MergeDifferentGammasWithoutSegfault)
{
    using namespace DB;

    DDSketchDenseLogarithmic lhs{};
    DDSketchDenseLogarithmic rhs{};

    std::string lhs_data = base64Decode("AgAAAAAAAABAAAAAAAAAAAABDAEPAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA");
    ReadBufferFromString lhs_buffer{lhs_data};
    lhs.deserialize(lhs_buffer);

    std::string rhs_data = base64Decode("As07f2aeoPY/AAAAAAAAAAABDAEjAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA");
    ReadBufferFromString rhs_buffer{rhs_data};
    rhs.deserialize(rhs_buffer);

    lhs.merge(rhs);
    std::vector<UInt8> merge_buffer;

    WriteBufferFromVector<std::vector<UInt8>> writer{merge_buffer};
    lhs.serialize(writer);

    ReadBufferFromMemory reader{merge_buffer.data(), merge_buffer.size()};
    ASSERT_NO_THROW(rhs.deserialize(reader));
}
