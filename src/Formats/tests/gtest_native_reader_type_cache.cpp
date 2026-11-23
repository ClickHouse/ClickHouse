#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/BlockMissingValues.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// With `null_as_default` the reader gives the column with the type of the header, but the map keeps
/// the type as it was read, so that the next block is read with the type of its own data.
TEST(NativeReaderTypeCache, KeepsTheTypeAsReadWhenTheHeaderConverts)
{
    auto nullable_type = DataTypeFactory::instance().get("Nullable(UInt64)");
    auto column = nullable_type->createColumn();
    column->insert(Field(UInt64(5)));
    column->insert(Field());
    Block block{ColumnWithTypeAndName(std::move(column), nullable_type, "x")};

    WriteBufferFromOwnString out;
    NativeWriter writer(out, DBMS_TCP_PROTOCOL_VERSION, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
    writer.write(block);
    out.finalize();

    auto number_type = DataTypeFactory::instance().get("UInt64");
    Block header{ColumnWithTypeAndName(number_type->createColumn(), number_type, "x")};
    FormatSettings settings;
    settings.null_as_default = true;

    NativeReaderTypeCache type_cache;
    ReadBufferFromString in(out.str());
    for (size_t read = 0; read < 2; ++read)
    {
        SCOPED_TRACE(read);
        BlockMissingValues missing_values(1);
        NativeReader reader(in, header, DBMS_TCP_PROTOCOL_VERSION, settings, &missing_values, &type_cache);
        Block result = reader.read();

        ASSERT_EQ(result.rows(), 2u);
        EXPECT_EQ(result.getByName("x").type->getName(), "UInt64");
        const auto & values = assert_cast<const ColumnUInt64 &>(*result.getByName("x").column).getData();
        EXPECT_EQ(values[0], 5u);
        EXPECT_EQ(values[1], 0u);
        EXPECT_TRUE(missing_values.hasDefaultBits(0));

        ASSERT_EQ(type_cache.size(), 1u);
        EXPECT_EQ(type_cache.at("Nullable(UInt64)")->getName(), "Nullable(UInt64)");
    }
}
