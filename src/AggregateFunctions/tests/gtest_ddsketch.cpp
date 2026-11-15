#include <string>
#include <base/types.h>

#include <AggregateFunctions/DDSketch.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>
#include <Common/VectorWithMemoryTracking.h>

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
    VectorWithMemoryTracking<UInt8> merge_buffer;

    WriteBufferFromVector<VectorWithMemoryTracking<UInt8>> writer{merge_buffer};
    lhs.serialize(writer);

    ReadBufferFromMemory reader{merge_buffer.data(), merge_buffer.size()};
    ASSERT_NO_THROW(rhs.deserialize(reader));

    ASSERT_FLOAT_EQ(rhs.getCount(), 2);
    ASSERT_DOUBLE_EQ(rhs.getGamma(), 2.0);
}

namespace
{

/// Build a DDSketch binary payload with the given gamma, offset, zero_count, and empty stores.
/// This lets us construct corrupted payloads by injecting bad values.
std::string buildDDSketchPayload(Float64 gamma, Float64 offset, Float64 zero_count)
{
    std::string result;
    DB::WriteBufferFromString buf(result);

    /// Mapping flag
    UInt8 mapping_flag = 0x02; /// FlagIndexMappingBaseLogarithmic
    writeBinary(mapping_flag, buf);
    writeBinary(gamma, buf);
    writeBinary(offset, buf);

    /// Positive store: empty contiguous encoding
    UInt8 pos_store_flag = 0x01; /// FlagTypePositiveStore
    writeBinary(pos_store_flag, buf);
    UInt8 contiguous = 0x0C; /// BinEncodingContiguousCounts
    writeBinary(contiguous, buf);
    writeVarUInt(UInt64(0), buf); /// num_bins = 0
    writeVarInt(Int64(0), buf);   /// start_key = 0
    writeVarInt(Int64(1), buf);   /// index_delta = 1

    /// Negative store: empty contiguous encoding
    UInt8 neg_store_flag = 0x03; /// FlagTypeNegativeStore
    writeBinary(neg_store_flag, buf);
    writeBinary(contiguous, buf);
    writeVarUInt(UInt64(0), buf);
    writeVarInt(Int64(0), buf);
    writeVarInt(Int64(1), buf);

    /// Zero count
    UInt8 zero_flag = 0x04; /// FlagZeroCountVarFloat
    writeBinary(zero_flag, buf);
    writeBinary(zero_count, buf);

    buf.finalize();
    return result;
}

/// Build a payload with a valid mapping but a corrupted store containing a specific bin count.
std::string buildPayloadWithBinCount(Float64 bin_count)
{
    std::string result;
    DB::WriteBufferFromString buf(result);

    Float64 gamma = 2.0;
    Float64 offset = 0.0;

    /// Mapping
    UInt8 mapping_flag = 0x02;
    writeBinary(mapping_flag, buf);
    writeBinary(gamma, buf);
    writeBinary(offset, buf);

    /// Positive store: 1 bin with the given count (sparse encoding)
    UInt8 pos_store_flag = 0x01;
    writeBinary(pos_store_flag, buf);
    UInt8 sparse = 0x04; /// BinEncodingIndexDeltasAndCounts
    writeBinary(sparse, buf);
    writeVarUInt(UInt64(1), buf);  /// num_non_empty_bins = 1
    writeVarInt(Int64(0), buf);    /// index_delta = 0
    writeFloatBinary(bin_count, buf);

    /// Negative store: empty
    UInt8 neg_store_flag = 0x03;
    writeBinary(neg_store_flag, buf);
    UInt8 contiguous = 0x0C;
    writeBinary(contiguous, buf);
    writeVarUInt(UInt64(0), buf);
    writeVarInt(Int64(0), buf);
    writeVarInt(Int64(1), buf);

    /// Zero count
    UInt8 zero_flag = 0x04;
    writeBinary(zero_flag, buf);
    Float64 zero_count = 0.0;
    writeBinary(zero_count, buf);

    buf.finalize();
    return result;
}

/// Build a payload with too many bins in the positive store.
std::string buildPayloadWithTooManyBins(UInt64 num_bins)
{
    std::string result;
    DB::WriteBufferFromString buf(result);

    Float64 gamma = 2.0;
    Float64 offset = 0.0;

    /// Mapping
    UInt8 mapping_flag = 0x02;
    writeBinary(mapping_flag, buf);
    writeBinary(gamma, buf);
    writeBinary(offset, buf);

    /// Positive store: contiguous with too many bins
    UInt8 pos_store_flag = 0x01;
    writeBinary(pos_store_flag, buf);
    UInt8 contiguous = 0x0C;
    writeBinary(contiguous, buf);
    writeVarUInt(num_bins, buf);
    writeVarInt(Int64(0), buf);
    writeVarInt(Int64(1), buf);
    /// We don't write actual bin data — the read should fail before it gets that far.

    buf.finalize();
    return result;
}

}

TEST(DDSketch, CorruptedGammaInfinity)
{
    auto data = buildDDSketchPayload(std::numeric_limits<Float64>::infinity(), 0.0, 0.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedGammaNaN)
{
    auto data = buildDDSketchPayload(std::numeric_limits<Float64>::quiet_NaN(), 0.0, 0.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedGammaLessThanOrEqualOne)
{
    auto data = buildDDSketchPayload(1.0, 0.0, 0.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedGammaNegative)
{
    auto data = buildDDSketchPayload(-1.0, 0.0, 0.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedOffsetNaN)
{
    auto data = buildDDSketchPayload(2.0, std::numeric_limits<Float64>::quiet_NaN(), 0.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedBinCountNaN)
{
    auto data = buildPayloadWithBinCount(std::numeric_limits<Float64>::quiet_NaN());
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedBinCountNegative)
{
    auto data = buildPayloadWithBinCount(-1.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, TooManyBins)
{
    auto data = buildPayloadWithTooManyBins(100000);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedZeroCountNaN)
{
    auto data = buildDDSketchPayload(2.0, 0.0, std::numeric_limits<Float64>::quiet_NaN());
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, CorruptedZeroCountNegative)
{
    auto data = buildDDSketchPayload(2.0, 0.0, -1.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_THROW(sketch.deserialize(buf), DB::Exception);
}

TEST(DDSketch, ValidPayloadStillWorks)
{
    /// A valid DDSketch with gamma=2.0, offset=0.0, zero_count=5.0, no bins
    auto data = buildDDSketchPayload(2.0, 0.0, 5.0);
    DB::ReadBufferFromString buf(data);
    DB::DDSketchDenseLogarithmic sketch;
    ASSERT_NO_THROW(sketch.deserialize(buf));
    ASSERT_DOUBLE_EQ(sketch.getCount(), 5.0);
    ASSERT_DOUBLE_EQ(sketch.getGamma(), 2.0);
}
