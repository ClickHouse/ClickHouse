#include <gtest/gtest.h>

#include <config.h>
#if USE_PARQUET

#include <Functions/FunctionFactory.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <IO/ReadBufferFromFile.h>

#include <Columns/ColumnsNumber.h>

#include <memory>

using namespace DB;

template <class T, class S>
static void testFilterPlainFixedData(int size, double positive_rate)
{
    PaddedPODArray<T> data;
    PaddedPODArray<S> src;
    PaddedPODArray<T> expected;
    RowSet set(size);
    data.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        auto value = std::rand() % 100000;
        src.push_back(value);
        (std::rand() % 100 + 1) < positive_rate * 100 ? set.set(i, true) : set.set(i, false);
        if (set.get(i))
        {
            expected.push_back(value);
        }
    }
    FilterHelper::filterPlainFixedData(src.data(), data, set, src.size());
    ASSERT_EQ(expected.size(), data.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        ASSERT_EQ(expected[i], data[i]);
    }
}

TEST(TestNativeParquetReader, TestFilterPlainFixedData)
{
    testFilterPlainFixedData<Int16, Int32>(100000, 1.0);
    testFilterPlainFixedData<Int16, Int16>(100000, 1.0);
    testFilterPlainFixedData<UInt16, Int32>(100000, 1.0);
    testFilterPlainFixedData<UInt16, UInt16>(100000, 1.0);
    testFilterPlainFixedData<Int32, Int32>(100000, 1.0);
    testFilterPlainFixedData<Int64, Int64>(100000, 1.0);
    testFilterPlainFixedData<Float32, Float32>(100000, 1.0);
    testFilterPlainFixedData<Float64, Float64>(100000, 1.0);
    testFilterPlainFixedData<DateTime64, Int64>(100000, 1.0);
    testFilterPlainFixedData<DateTime64, DateTime64>(100000, 1.0);

    testFilterPlainFixedData<Int16, Int32>(100000, 0);
    testFilterPlainFixedData<Int16, Int16>(100000, 0);
    testFilterPlainFixedData<UInt16, Int32>(100000, 0);
    testFilterPlainFixedData<UInt16, UInt16>(100000, 0);
    testFilterPlainFixedData<Int32, Int32>(100000, 0);
    testFilterPlainFixedData<Int64, Int64>(100000, 0);
    testFilterPlainFixedData<Float32, Float32>(100000, 0);
    testFilterPlainFixedData<Float64, Float64>(100000, 0);
    testFilterPlainFixedData<DateTime64, Int64>(100000, 0);
    testFilterPlainFixedData<DateTime64, DateTime64>(100000, 0);

    testFilterPlainFixedData<Int16, Int32>(100000, 0.9);
    testFilterPlainFixedData<Int16, Int16>(100000, 0.9);
    testFilterPlainFixedData<UInt16, Int32>(100000, 0.9);
    testFilterPlainFixedData<UInt16, UInt16>(100000, 0.9);
    testFilterPlainFixedData<Int32, Int32>(100000, 0.9);
    testFilterPlainFixedData<Int64, Int64>(100000, 0.9);
    testFilterPlainFixedData<Float32, Float32>(100000, 0.9);
    testFilterPlainFixedData<Float64, Float64>(100000, 0.9);
    testFilterPlainFixedData<DateTime64, Int64>(100000, 0.9);
    testFilterPlainFixedData<DateTime64, DateTime64>(100000, 0.9);
}

template <class T>
static void testGatherDictInt()
{
    int size = 10000;
    PaddedPODArray<T> data;
    PaddedPODArray<T> dict;
    std::unordered_map<T, Int32> map;
    PaddedPODArray<T> dist;
    PaddedPODArray<Int32> idx;
    for (size_t i = 0; i < size; ++i)
    {
        auto value = std::rand() % 10000;
        data.push_back(value);
        if (map.find(value) == map.end())
        {
            map[value] = static_cast<Int32>(dict.size());
            dict.push_back(value);
        }
        idx.push_back(map[value]);
    }
    dist.reserve(data.size());
    FilterHelper::gatherDictFixedValue(dict, dist, idx, data.size());
    ASSERT_EQ(data.size(), dist.size());
    for (size_t i = 0; i < data.size(); ++i)
    {
        ASSERT_EQ(data[i], dist[i]);
    }
}

TEST(TestNativeParquetReader, TestGatherDictNumberData)
{
    testGatherDictInt<Int16>();
    testGatherDictInt<Int32>();
    testGatherDictInt<Int64>();
    testGatherDictInt<Float32>();
    testGatherDictInt<Float64>();
    testGatherDictInt<DateTime64>();
}

template <class T, class S>
static void testDecodePlainData(size_t numbers, size_t exist_nums)
{
    PaddedPODArray<S> src;
    for (size_t i = 0; i < numbers; ++i)
    {
        src.push_back(std::rand() % 10000);
    }
    PaddedPODArray<T> dst;
    for (size_t i = 0; i < exist_nums; ++i)
    {
        dst.push_back(std::rand() % 10000);
    }
    dst.reserve(exist_nums + src.size());
    size_t size = src.size();
    const auto * buffer = reinterpret_cast<const uint8_t *>(src.data());
    ParquetData data{.buffer = buffer, .buffer_size = size * sizeof(S)};
    PageOffsets offsets{.remain_rows = size, .levels_offset = 0};
    PlainDecoder decoder(data, offsets);
    OptionalRowSet row_set = std::nullopt;
    decoder.decodeFixedValue<T, S>(dst, row_set, size);
    ASSERT_EQ(dst.size(), exist_nums + src.size());
    for (size_t i = 0; i < src.size(); ++i)
    {
        ASSERT_EQ(src[i], dst[exist_nums + i]);
    }
    ASSERT_EQ(offsets.levels_offset, size);
    ASSERT_EQ(offsets.remain_rows, 0);
}

TEST(TestNativeParquetReader, testDecodeNoFilter)
{
    testDecodePlainData<DateTime64, Int64>(1000, 100);
    testDecodePlainData<DateTime64, Int64>(1000, 0);
    testDecodePlainData<Int64, Int64>(1000, 100);
    testDecodePlainData<Int64, Int64>(1000, 0);
    testDecodePlainData<Int32, Int32>(1000, 100);
    testDecodePlainData<Int32, Int32>(1000, 0);
    testDecodePlainData<Int16, Int32>(1000, 100);
    testDecodePlainData<Int16, Int32>(1000, 0);
}

TEST(TestNativeParquetReader, TestRowSet)
{
    RowSet rowSet(10000);
    rowSet.setAllFalse();
    rowSet.set(100, true);
    rowSet.set(1234, true);
    ASSERT_EQ(2, rowSet.count());
    ASSERT_FALSE(rowSet.none());
    ASSERT_TRUE(rowSet.any());
    ASSERT_FALSE(rowSet.all());
}

TEST(TestNativeParquetReader, TestColumnIntFilter)
{
    BigIntRangeFilter filter(100, 200, false);
    BigIntRangeFilter filter_allow_null(100, 200, true);
    BigIntRangeFilter filter2(200, 200, false);

    ASSERT_TRUE(!filter.testInt16(99));
    ASSERT_TRUE(filter.testInt16(100));
    ASSERT_TRUE(filter.testInt16(150));
    ASSERT_TRUE(filter.testInt16(200));
    ASSERT_TRUE(filter2.testInt16(200));
    ASSERT_TRUE(!filter.testInt16(210));
    ASSERT_TRUE(!filter2.testInt16(210));

    ASSERT_TRUE(!filter.testInt32(99));
    ASSERT_TRUE(filter.testInt32(100));
    ASSERT_TRUE(filter.testInt32(150));
    ASSERT_TRUE(filter.testInt32(200));
    ASSERT_TRUE(filter2.testInt32(200));
    ASSERT_TRUE(!filter.testInt32(210));
    ASSERT_TRUE(!filter2.testInt32(210));

    ASSERT_TRUE(!filter.testInt64(99));
    ASSERT_TRUE(filter.testInt64(100));
    ASSERT_TRUE(filter.testInt64(150));
    ASSERT_TRUE(filter.testInt64(200));
    ASSERT_TRUE(filter2.testInt64(200));
    ASSERT_TRUE(!filter.testInt64(210));
    ASSERT_TRUE(!filter2.testInt64(210));

    ASSERT_TRUE(filter.testInt64Range(90, 100, true));
    ASSERT_TRUE(filter.testInt64Range(110, 200, true));
    ASSERT_TRUE(filter.testInt64Range(200, 210, true));
    ASSERT_TRUE(filter_allow_null.testInt64Range(90, 100, true));
    ASSERT_FALSE(filter.testInt64Range(90, 99, true));
    ASSERT_FALSE(filter.testInt64Range(201, 210, true));
    ASSERT_TRUE(filter_allow_null.testInt64Range(201, 210, true));
    ASSERT_FALSE(filter_allow_null.testInt64Range(201, 210, false));

    PaddedPODArray<Int16> int16_values = {99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28,
                                          99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28};
    RowSet set1(int16_values.size());
    filter.testInt16Values(set1, int16_values.size(), int16_values.data());
    ASSERT_EQ(set1.count(), 22);
    set1.setAllTrue();
    filter2.testInt16Values(set1, int16_values.size(), int16_values.data());
    ASSERT_EQ(set1.count(), 2);

    PaddedPODArray<Int32> int32_values = {99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28,
                                          99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28};
    RowSet set2(int32_values.size());
    filter.testInt32Values(set2, int32_values.size(), int32_values.data());
    ASSERT_EQ(set2.count(), 22);
    set2.setAllTrue();
    filter2.testInt32Values(set2, int32_values.size(), int32_values.data());
    ASSERT_EQ(set2.count(), 2);

    PaddedPODArray<Int64> int64_values = {99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28,
                                          99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 150, 200, 210, 211, 231, 24, 25, 26, 27, 28};
    RowSet set3(int64_values.size());
    filter.testInt64Values(set3, int64_values.size(), int64_values.data());
    ASSERT_EQ(set3.count(), 22);
    set3.setAllTrue();
    filter2.testInt64Values(set3, int64_values.size(), int64_values.data());
    ASSERT_EQ(set3.count(), 2);


    NegatedBigIntRangeFilter negated_filter(200, 200, false);
    ASSERT_FALSE(negated_filter.testInt16(200));
    ASSERT_FALSE(negated_filter.testInt32(200));
    ASSERT_FALSE(negated_filter.testInt64(200));
    RowSet row_set4 = RowSet(int16_values.size());
    negated_filter.testInt16Values(row_set4, int16_values.size(), int16_values.data());
    ASSERT_EQ(38, row_set4.count());
    RowSet row_set5 = RowSet(int32_values.size());
    negated_filter.testInt32Values(row_set5, int32_values.size(), int32_values.data());
    ASSERT_EQ(38, row_set5.count());
    RowSet row_set6 = RowSet(int64_values.size());
    negated_filter.testInt64Values(row_set6, int64_values.size(), int64_values.data());
    ASSERT_EQ(38, row_set6.count());
}

TEST(TestNativeParquetReader, TestIsNullFilter)
{
    IsNullFilter filter;
    ASSERT_TRUE(filter.testNull());
    ASSERT_FALSE(filter.testNotNull());
    ASSERT_FALSE(filter.testInt64(100));
    ASSERT_FALSE(filter.testInt32(100));
    ASSERT_FALSE(filter.testInt16(100));
    ASSERT_FALSE(filter.testInt64Range(100, 101, false));
    ASSERT_FALSE(filter.testString("test"));
    ASSERT_FALSE(filter.testFloat32(0.3f));
    ASSERT_FALSE(filter.testFloat64(0.3));
}

#endif
