#include <gtest/gtest.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/BuffersReader.h>
#include <Formats/BuffersWriter.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// The Buffers format carries no per-column serialization kind, so a ColumnSparse reaching the
/// writer must be densified before the type-level serializer (which only accepts a dense column).
/// Build a sparse String column explicitly and round-trip it through Buffers.
TEST(BuffersFormat, SparseStringColumn)
{
    constexpr size_t n = 10;

    auto type = DataTypeFactory::instance().get("String");

    /// The schema/header is a dense String column; only the data block carries the sparse column.
    Block header;
    header.insert(ColumnWithTypeAndName(type->createColumn(), type, "s"));

    auto make_sparse_block = [&]
    {
        /// ColumnSparse layout: values[0] is the default, values[1..] are the non-default values,
        /// offsets holds their row positions.
        auto values = ColumnString::create();
        values->insert(Field(String("")));
        values->insert(Field(String("rare1")));
        values->insert(Field(String("rare2")));
        auto offsets = ColumnUInt64::create();
        offsets->insert(Field(UInt64(3)));
        offsets->insert(Field(UInt64(7)));
        auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), n);
        EXPECT_TRUE(sparse->isSparse());

        Block block;
        block.insert(ColumnWithTypeAndName(std::move(sparse), type, "s"));
        return block;
    };

    FormatSettings format_settings;

    WriteBufferFromOwnString out;
    BuffersWriter writer(out, std::make_shared<const Block>(header), format_settings);
    writer.write(make_sparse_block());
    out.finalize();

    ReadBufferFromString in(out.str());
    BuffersReader reader(in, header, format_settings);
    Block result = reader.read();

    ASSERT_EQ(result.rows(), n);
    const auto & column = *result.getByPosition(0).column;
    for (size_t i = 0; i < n; ++i)
    {
        Field got;
        column.get(i, got);
        Field expected = Field(String(i == 3 ? "rare1" : i == 7 ? "rare2" : ""));
        ASSERT_EQ(got, expected) << "row " << i;
    }
}
