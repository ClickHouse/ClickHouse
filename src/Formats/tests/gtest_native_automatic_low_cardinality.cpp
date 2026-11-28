#include <gtest/gtest.h>

#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMaterializationUtils.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Formats/FormatSettings.h>
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

TEST(NativeAutomaticLowCardinality, RoundTripThroughCurrentRevision)
{
    static constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION;
    static constexpr size_t num_rows = 1000;

    Block block;
    block.insert(makeAutomaticallyEncodedColumn(num_rows));

    String encoded;
    {
        WriteBufferFromString out(encoded);
        NativeWriter writer(out, revision, std::make_shared<const Block>(block.cloneEmpty()));
        writer.write(block);
        writer.flush();
    }

    /// Both a peer reading the result of a query and the server reading inserted data must accept
    /// the `{DEFAULT, LOW_CARDINALITY}` kind stack.
    for (auto allowed_kinds : {ISerialization::KindSet::all(), NativeReader::default_allowed_kinds})
    {
        ReadBufferFromString in(encoded);
        NativeReader reader(in, revision, std::nullopt, allowed_kinds);
        auto read_block = reader.read();

        ASSERT_EQ(read_block.rows(), num_rows);
        const auto & read = read_block.getByName("s");
        EXPECT_EQ(read.type->getName(), "String");
        auto full = recursiveRemoveNonNativeLowCardinality(read.column);
        EXPECT_FALSE(full->lowCardinality());
        for (size_t i = 0; i < num_rows; ++i)
            ASSERT_EQ(String(full->getDataAt(i)), "val_" + std::to_string(i % 4)) << "row " << i;
    }
}

TEST(NativeAutomaticLowCardinality, TupleElementThroughOlderRevision)
{
    /// The `LOW_CARDINALITY` kind of an encoded `Tuple` element is only in the info of the element,
    /// not in the kind stack of the outer column, but it must be materialized for an old peer as well.
    static constexpr UInt64 old_revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION - 1;
    static constexpr size_t num_rows = 1000;

    auto tuple_type = DataTypeFactory::instance().get("Tuple(String)");

    Block block;
    {
        auto element = makeAutomaticallyEncodedColumn(num_rows);
        block.insert(ColumnWithTypeAndName(ColumnTuple::create(Columns{element.column}), tuple_type, "t"));
    }

    Block plain_block;
    {
        auto full = DataTypeFactory::instance().get("String")->createColumn();
        for (size_t i = 0; i < num_rows; ++i)
            full->insert(Field("val_" + std::to_string(i % 4)));
        plain_block.insert(ColumnWithTypeAndName(ColumnTuple::create(Columns{std::move(full)}), tuple_type, "t"));
    }

    auto [serialization, info, result_column] = NativeWriter::getSerializationAndColumn(old_revision, block.getByName("t"));
    ASSERT_NE(info, nullptr);
    EXPECT_FALSE(info->hasCustomSerialization());
    EXPECT_FALSE(typeid_cast<const ColumnTuple &>(*result_column).getColumn(0).lowCardinality());

    auto serialize = [](const Block & to_write)
    {
        String result;
        WriteBufferFromString out(result);
        NativeWriter writer(out, old_revision, std::make_shared<const Block>(to_write.cloneEmpty()));
        writer.write(to_write);
        writer.flush();
        return result;
    };

    EXPECT_EQ(serialize(block), serialize(plain_block));
}

TEST(NativeAutomaticLowCardinality, BLOBThroughOlderRevision)
{
    /// Parallel block marshalling wraps the column into a `ColumnBLOB` before `NativeWriter` sees it.
    /// The kinds announced for the blob are computed from its wrapped column, so they must describe
    /// the materialized payload rather than the encoded column for a peer without the `LOW_CARDINALITY` kind.
    static constexpr UInt64 old_revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION - 1;
    static_assert(old_revision >= DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING);
    static constexpr size_t num_rows = 1000;

    auto column = makeAutomaticallyEncodedColumn(num_rows);
    column.column = ColumnBLOB::create(
        column, CompressionCodecFactory::instance().getDefaultCodec(), old_revision, FormatSettings{});

    EXPECT_EQ(
        ISerialization::getKindStack(*column.column),
        (ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED}));

    Block block;
    block.insert(column);

    String encoded;
    {
        WriteBufferFromString out(encoded);
        NativeWriter writer(out, old_revision, std::make_shared<const Block>(block.cloneEmpty()));
        writer.write(block);
        writer.flush();
    }

    ReadBufferFromString in(encoded);
    NativeReader reader(in, old_revision, std::nullopt, ISerialization::KindSet::all());
    auto read_block = reader.read();

    ASSERT_EQ(read_block.rows(), num_rows);
    const auto & read = read_block.getByName("s");
    EXPECT_EQ(read.type->getName(), "String");
    auto full = read.column->convertToFullColumnIfDetached();
    EXPECT_FALSE(full->lowCardinality());
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_EQ(String(full->getDataAt(i)), "val_" + std::to_string(i % 4)) << "row " << i;
}

TEST(NativeAutomaticLowCardinality, TupleElementThroughCurrentRevision)
{
    /// A peer of the current revision keeps the encoding of a `Tuple` element. The type of the read column is
    /// still `Tuple(String)`, so only a recursive check of the column notices the encoded element.
    static constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION;
    static constexpr size_t num_rows = 1000;

    auto tuple_type = DataTypeFactory::instance().get("Tuple(String)");

    Block block;
    {
        auto element = makeAutomaticallyEncodedColumn(num_rows);
        block.insert(ColumnWithTypeAndName(ColumnTuple::create(Columns{element.column}), tuple_type, "t"));
    }

    String encoded;
    {
        WriteBufferFromString out(encoded);
        NativeWriter writer(out, revision, std::make_shared<const Block>(block.cloneEmpty()));
        writer.write(block);
        writer.flush();
    }

    ReadBufferFromString in(encoded);
    NativeReader reader(in, revision);
    auto read_block = reader.read();

    ASSERT_EQ(read_block.rows(), num_rows);
    const auto & read = read_block.getByName("t");
    EXPECT_EQ(read.type->getName(), "Tuple(String)");
    EXPECT_FALSE(read.column->lowCardinality());
    EXPECT_TRUE(hasNonNativeLowCardinality(*read.column));

    auto full = recursiveRemoveNonNativeLowCardinality(read.column);
    EXPECT_FALSE(hasNonNativeLowCardinality(*full));
    const auto & element = typeid_cast<const ColumnTuple &>(*full).getColumn(0);
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_EQ(String(element.getDataAt(i)), "val_" + std::to_string(i % 4)) << "row " << i;
}

TEST(NativeAutomaticLowCardinality, LowCardinalityHeaderWithoutTypesConversion)
{
    /// The reader converts an encoded `String` column to a `LowCardinality(String)` header with the conversion
    /// for old clients, which expects a full column of the dictionary type.
    static constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_AUTOMATIC_LOW_CARDINALITY_SERIALIZATION;
    static constexpr size_t num_rows = 1000;

    Block block;
    block.insert(makeAutomaticallyEncodedColumn(num_rows));

    String encoded;
    {
        WriteBufferFromString out(encoded);
        NativeWriter writer(out, revision, std::make_shared<const Block>(block.cloneEmpty()));
        writer.write(block);
        writer.flush();
    }

    auto low_cardinality_type = DataTypeFactory::instance().get("LowCardinality(String)");
    Block header;
    header.insert(ColumnWithTypeAndName(low_cardinality_type->createColumn(), low_cardinality_type, "s"));

    FormatSettings format_settings;
    format_settings.native.allow_types_conversion = false;

    ReadBufferFromString in(encoded);
    NativeReader reader(in, header, revision, format_settings);
    auto read_block = reader.read();

    ASSERT_EQ(read_block.rows(), num_rows);
    const auto & read = read_block.getByName("s");
    EXPECT_EQ(read.type->getName(), "LowCardinality(String)");
    ASSERT_TRUE(read.column->lowCardinality());
    EXPECT_TRUE(read.column->isNativeLowCardinality());
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_EQ(String(read.column->getDataAt(i)), "val_" + std::to_string(i % 4)) << "row " << i;
}
