#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressionCodecQuantized.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

constexpr size_t DIMENSIONS = 8;
constexpr size_t GRANULE_ROWS = 4;
constexpr size_t NUM_GRANULES = 3;

DataTypePtr vectorType()
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
}

SerializationPtr quantizedSerialization()
{
    QuantizedCodecParams params{.method = "int8", .dimensions = DIMENSIONS, .bits = 0, .m = 0};
    return std::make_shared<SerializationQuantizedVector>(vectorType()->getDefaultSerialization(), params);
}

ColumnPtr makeVectors(size_t num_rows, size_t first_row)
{
    auto column = vectorType()->createColumn();
    auto & array = typeid_cast<ColumnArray &>(*column);
    auto & data = typeid_cast<ColumnFloat32 &>(array.getData()).getData();
    auto & offsets = array.getOffsets();
    for (size_t row = 0; row < num_rows; ++row)
    {
        for (size_t i = 0; i < DIMENSIONS; ++i)
            data.push_back(static_cast<Float32>((first_row + row) * DIMENSIONS + i));
        offsets.push_back(data.size());
    }
    return column;
}

/// Write `NUM_GRANULES` granules the way a Compact part does: one full serialization round (array streams then codes)
/// per granule, all substreams appended to one shared buffer.
String writeCompactGranules()
{
    auto serialization = quantizedSerialization();
    String buffer;
    WriteBufferFromString ostr(buffer);

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath &) -> WriteBuffer * { return &ostr; };
    settings.data_part_type = MergeTreeDataPartType::Compact;
    settings.position_independent_encoding = true;
    settings.use_specialized_prefixes_and_suffixes_substreams = true;

    for (size_t granule = 0; granule < NUM_GRANULES; ++granule)
    {
        auto column = makeVectors(GRANULE_ROWS, granule * GRANULE_ROWS);
        ISerialization::SerializeBinaryBulkStatePtr state;
        serialization->serializeBinaryBulkStatePrefix(*column, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, column->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }

    ostr.finalize();
    return buffer;
}

}

/// Reading granule after granule from one shared, never-repositioned stream is what a Compact part's MultiBuffer
/// reader does inside a stripe. Without the codes being accounted for, the second granule's array sizes are read
/// from the middle of the first granule's codes (issue clickhouse-private#67517).
TEST(QuantizedVectorSerialization, CompactGranulesReadSequentiallyFromOneStream)
{
    const String data = writeCompactGranules();
    auto serialization = quantizedSerialization();

    ReadBufferFromString istr(data);
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath &) -> ReadBuffer * { return &istr; };
    settings.data_part_type = MergeTreeDataPartType::Compact;
    settings.position_independent_encoding = true;
    settings.use_specialized_prefixes_and_suffixes_substreams = true;

    auto column = vectorType()->createColumn();
    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    for (size_t granule = 0; granule < NUM_GRANULES; ++granule)
        serialization->deserializeBinaryBulkWithMultipleStreams(*column, GRANULE_ROWS, settings, state, nullptr);

    ASSERT_EQ(column->size(), NUM_GRANULES * GRANULE_ROWS);
    const auto & array = typeid_cast<const ColumnArray &>(*column);
    const auto & data_values = typeid_cast<const ColumnFloat32 &>(array.getData()).getData();
    ASSERT_EQ(data_values.size(), NUM_GRANULES * GRANULE_ROWS * DIMENSIONS);
    for (size_t i = 0; i < data_values.size(); ++i)
        ASSERT_FLOAT_EQ(data_values[i], static_cast<Float32>(i)) << "at element " << i;
}
