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

    /*
    {
      "mapping": {
        "gamma": 2.0,
        "index_offset": 0.0,
        "interpolation": 0
      },
      "positive_values": {
        "bin_counts": {},
        "contiguous_bin_counts": [
          1.0
        ],
        "contiguous_bin_index_offset": -8
      },
      "negative_values": {
        "bin_counts": {},
        "contiguous_bin_counts": [],
        "contiguous_bin_index_offset": 0
      },
      "zero_count": 0.0
    }
    */
    std::string lhs_data = base64Decode("AgAAAAAAAABAAAAAAAAAAAABDAEPAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA");
    ReadBufferFromString lhs_buffer{lhs_data};
    lhs.deserialize(lhs_buffer);

    ASSERT_DOUBLE_EQ(lhs.getCount(), 1);
    ASSERT_DOUBLE_EQ(lhs.getGamma(), 2.0);

    /*
    {
      "mapping": {
        "gamma": 1.4142135623730951,
        "index_offset": 0.0,
        "interpolation": 0
      },
      "positive_values": {
        "bin_counts": {},
        "contiguous_bin_counts": [
          1.0
        ],
        "contiguous_bin_index_offset": -18
      },
      "negative_values": {
        "bin_counts": {},
        "contiguous_bin_counts": [],
        "contiguous_bin_index_offset": 0
      },
      "zero_count": 0.0
    }
    */
    std::string rhs_data = base64Decode("As07f2aeoPY/AAAAAAAAAAABDAEjAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA");
    ReadBufferFromString rhs_buffer{rhs_data};
    rhs.deserialize(rhs_buffer);

    ASSERT_DOUBLE_EQ(rhs.getCount(), 1);
    ASSERT_DOUBLE_EQ(rhs.getGamma(), 1.4142135623730951);

    lhs.merge(rhs);
    std::vector<UInt8> merge_buffer;

    WriteBufferFromVector<std::vector<UInt8>> writer{merge_buffer};
    lhs.serialize(writer);

    ReadBufferFromMemory reader{merge_buffer.data(), merge_buffer.size()};
    ASSERT_NO_THROW(rhs.deserialize(reader));

    ASSERT_FLOAT_EQ(rhs.getCount(), 2);
    ASSERT_DOUBLE_EQ(rhs.getGamma(), 2.0);
}
