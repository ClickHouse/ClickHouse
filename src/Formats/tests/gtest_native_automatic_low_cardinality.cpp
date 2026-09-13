#include <gtest/gtest.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMaterializationUtils.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

/// A `String` column that is dictionary-encoded in memory by automatic `LowCardinality`
/// serialization: the declared type stays `String`, the column is a non-native
/// `ColumnLowCardinality`.
ColumnWithTypeAndName makeAutomaticallyEncodedColumn(size_t num_rows)
{
    auto type = DataTypeFactory::instance().get("String");

    auto full = type->createColumn();
    for (size_t i = 0; i < num_rows; ++i)
        full->insert(Field("val_" + std::to_string(i % 4)));

    auto column = convertToSerialization(ColumnPtr(std::move(full)), *type, /*low_cardinality=*/true);
    EXPECT_TRUE(column->lowCardinality());

    return ColumnWithTypeAndName(column, type, "s");
}

}

TEST(NativeAutomaticLowCardinality, CurrentRevisionKeepsTheEncoding)
{
    auto column = makeAutomaticallyEncodedColumn(100);

    auto [serialization, info, result_column]
        = NativeWriter::getSerializationAndColumn(DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION, column);

    ASSERT_NE(info, nullptr);
    EXPECT_TRUE(ISerialization::hasKind(info->getKindStack(), ISerialization::Kind::LOW_CARDINALITY));
    EXPECT_TRUE(result_column->lowCardinality());
    EXPECT_TRUE(ISerialization::hasKind(serialization->getKindStack(), ISerialization::Kind::LOW_CARDINALITY));
}

TEST(NativeAutomaticLowCardinality, OlderRevisionMaterializes)
{
    /// Every peer that does not know the `LOW_CARDINALITY` kind - both one that predates custom
    /// serializations entirely and one that knows them but not this kind - must get a full column
    /// written with the default serialization of the declared type.
    for (UInt64 client_revision :
         {static_cast<UInt64>(0),
          static_cast<UInt64>(DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION),
          static_cast<UInt64>(DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION - 1)})
    {
        auto column = makeAutomaticallyEncodedColumn(100);

        auto [serialization, info, result_column] = NativeWriter::getSerializationAndColumn(client_revision, column);

        /// A peer that predates custom serializations is not told about them at all; a newer one
        /// still expects the serialization-kind byte, just without the `LOW_CARDINALITY` kind.
        if (client_revision < DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
        {
            EXPECT_EQ(info, nullptr) << "revision " << client_revision;
        }
        else
        {
            ASSERT_NE(info, nullptr) << "revision " << client_revision;
            EXPECT_FALSE(ISerialization::hasKind(info->getKindStack(), ISerialization::Kind::LOW_CARDINALITY))
                << "revision " << client_revision;
        }
        EXPECT_FALSE(result_column->lowCardinality()) << "revision " << client_revision;
        EXPECT_FALSE(ISerialization::hasKind(serialization->getKindStack(), ISerialization::Kind::LOW_CARDINALITY))
            << "revision " << client_revision;
        EXPECT_EQ(result_column->size(), 100u) << "revision " << client_revision;
    }
}

TEST(NativeAutomaticLowCardinality, RoundTripThroughOlderRevision)
{
    static constexpr UInt64 old_revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION - 1;
    static constexpr size_t num_rows = 1000;

    Block block;
    block.insert(makeAutomaticallyEncodedColumn(num_rows));

    /// The same data without the encoding: an old peer must see exactly these bytes.
    Block plain_block;
    {
        auto type = DataTypeFactory::instance().get("String");
        auto full = type->createColumn();
        for (size_t i = 0; i < num_rows; ++i)
            full->insert(Field("val_" + std::to_string(i % 4)));
        plain_block.insert(ColumnWithTypeAndName(std::move(full), type, "s"));
    }

    auto serialize = [](const Block & to_write)
    {
        String result;
        WriteBufferFromString out(result);
        NativeWriter writer(out, old_revision, std::make_shared<const Block>(to_write.cloneEmpty()));
        writer.write(to_write);
        writer.flush();
        return result;
    };

    const String encoded = serialize(block);
    EXPECT_EQ(encoded, serialize(plain_block));

    ReadBufferFromString in(encoded);
    NativeReader reader(in, old_revision);
    auto read_block = reader.read();

    ASSERT_EQ(read_block.rows(), num_rows);
    const auto & read_column = *read_block.getByName("s").column;
    EXPECT_FALSE(read_column.lowCardinality());
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_EQ(String(read_column.getDataAt(i)), "val_" + std::to_string(i % 4)) << "row " << i;
}
