#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/tests/gtest_global_register.h>

#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

using namespace DB;

/// A server that still has a `Nullable(SimpleAggregateFunction(...))` column sends it over Arrow Flight as a
/// nullable opaque field tagged with the full `SimpleAggregateFunction` name.
TEST(ArrowColumnToCHColumn, NullableOpaqueSimpleAggregateFunction)
{
    tryRegisterAggregateFunctions();
    const auto type = DataTypeFactory::instance().get("SimpleAggregateFunction(any, QBit(Float32, 2))");
    const auto serialization = type->getDefaultSerialization();
    const FormatSettings format_settings;

    auto value = type->createColumn();
    ReadBufferFromString text(std::string_view("[1,2]"));
    serialization->deserializeWholeText(*value, text, format_settings);
    WriteBufferFromOwnString bytes;
    serialization->serializeBinary(*value, 0, bytes, format_settings);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(bytes.str()).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    const auto metadata = arrow::key_value_metadata(
        {"ARROW:extension:name", "ARROW:extension:metadata"},
        {std::string(FormatSettings::ARROW_OPAQUE_EXTENSION_NAME), type->getName()});
    const auto table = arrow::Table::Make(arrow::schema({arrow::field("q", arrow::binary(), /*nullable=*/ true, metadata)}), {array});

    const Block header({ColumnWithTypeAndName(type->createColumn(), type, "q")});
    ArrowColumnToCHColumn converter(
        header, "ArrowFlight", format_settings, std::nullopt, std::nullopt,
        /*allow_missing_columns_=*/ false, /*null_as_default_=*/ true,
        FormatSettings::DateTimeOverflowBehavior::Ignore, /*allow_geoparquet_parser_=*/ false);
    const Chunk chunk = converter.arrowTableToCHChunk(table, 2, nullptr);

    ASSERT_EQ(chunk.getNumRows(), 2);
    WriteBufferFromOwnString out;
    serialization->serializeText(*chunk.getColumns()[0], 0, out, format_settings);
    writeChar(',', out);
    serialization->serializeText(*chunk.getColumns()[0], 1, out, format_settings);
    out.finalize();
    EXPECT_EQ(out.str(), "[1,2],[0,0]");
}

#endif
