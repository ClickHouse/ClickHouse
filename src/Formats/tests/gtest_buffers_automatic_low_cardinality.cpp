#include <gtest/gtest.h>

#include <Columns/ColumnMaterializationUtils.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/BuffersReader.h>
#include <Formats/BuffersWriter.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// The Buffers format writes every column with the default serialization of its declared type, so a
/// `String` column that is a non-native `ColumnLowCardinality` (automatic `LowCardinality`
/// serialization) must be materialized before it reaches `SerializationString`.
TEST(BuffersFormat, AutomaticLowCardinalityStringColumn)
{
    constexpr size_t n = 10;

    auto type = DataTypeFactory::instance().get("String");

    Block header;
    header.insert(ColumnWithTypeAndName(type->createColumn(), type, "s"));

    auto full = type->createColumn();
    for (size_t i = 0; i < n; ++i)
        full->insert(Field("val_" + std::to_string(i % 3)));

    auto encoded = convertToSerialization(ColumnPtr(std::move(full)), *type, /*low_cardinality=*/true);
    ASSERT_TRUE(encoded->lowCardinality());

    Block block;
    block.insert(ColumnWithTypeAndName(encoded, type, "s"));

    FormatSettings format_settings;

    WriteBufferFromOwnString out;
    BuffersWriter writer(out, std::make_shared<const Block>(header), format_settings);
    writer.write(block);
    out.finalize();

    ReadBufferFromString in(out.str());
    BuffersReader reader(in, header, format_settings);
    Block result = reader.read();

    ASSERT_EQ(result.rows(), n);
    const auto & column = *result.getByPosition(0).column;
    EXPECT_FALSE(column.lowCardinality());
    for (size_t i = 0; i < n; ++i)
    {
        Field got;
        column.get(i, got);
        ASSERT_EQ(got, Field("val_" + std::to_string(i % 3))) << "row " << i;
    }
}
