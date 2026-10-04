#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>

#if USE_AVRO

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

using namespace DB;

namespace
{

/// Single-column schema `c0 String`, partitioned by `identity(c0)` unless another transform is asked for.
struct PartitionerFixture
{
    Poco::JSON::Array::Ptr schema = new Poco::JSON::Array;
    Poco::JSON::Array::Ptr spec = new Poco::JSON::Array;
    SharedHeader header;
    ContextPtr context;

    explicit PartitionerFixture(const String & transform = "identity", bool nullable_source = false)
    {
        tryRegisterFunctions();
        context = getContext().context;

        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set("id", 1);
        field->set("name", "c0");
        schema->add(field);

        Poco::JSON::Object::Ptr spec_field = new Poco::JSON::Object;
        spec_field->set("transform", transform);
        spec_field->set("source-id", 1);
        spec->add(spec_field);

        DataTypePtr source_type = std::make_shared<DataTypeString>();
        if (nullable_source)
            source_type = makeNullable(source_type);
        header = std::make_shared<const Block>(Block{{source_type->createColumn(), source_type, "c0"}});
    }

    /// A chunk with columns but zero rows - what a block whose rows were all position-deleted
    /// looks like. Note `Chunk::empty()` is false for it: it requires no-columns as well.
    static Chunk rowlessChunk()
    {
        return Chunk(Columns{ColumnString::create()}, 0);
    }

    static Chunk chunkWithValues(const std::vector<String> & values)
    {
        auto col = ColumnString::create();
        for (const auto & value : values)
            col->insertData(value.data(), value.size());
        size_t num_rows = values.size();
        Columns columns;
        columns.push_back(std::move(col));
        return Chunk(std::move(columns), num_rows);
    }

    /// A `std::nullopt` element becomes a NULL row.
    static Chunk nullableChunkWithValues(const std::vector<std::optional<String>> & values)
    {
        auto nested = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        for (const auto & value : values)
        {
            if (value)
                nested->insertData(value->data(), value->size());
            else
                nested->insertDefault();
            null_map->insertValue(value ? 0 : 1);
        }
        size_t num_rows = values.size();
        Columns columns;
        columns.push_back(ColumnNullable::create(std::move(nested), std::move(null_map)));
        return Chunk(std::move(columns), num_rows);
    }
};

TEST(IcebergChunkPartitioner, RowlessChunkYieldsNoPartitions)
{
    PartitionerFixture fx;
    ChunkPartitioner partitioner(fx.spec, fx.schema, fx.context, fx.header);

    auto rowless = PartitionerFixture::rowlessChunk();
    ASSERT_FALSE(rowless.empty());
    EXPECT_TRUE(partitioner.partitionChunk(rowless).empty());

    Chunk no_columns_at_all;
    EXPECT_TRUE(partitioner.partitionChunk(no_columns_at_all).empty());
}

TEST(IcebergChunkPartitioner, PartitionChunkScattersByKey)
{
    PartitionerFixture fx;
    ChunkPartitioner partitioner(fx.spec, fx.schema, fx.context, fx.header);

    auto chunk = PartitionerFixture::chunkWithValues({"a", "b", "a"});
    auto result = partitioner.partitionChunk(chunk);
    ASSERT_EQ(result.size(), 2u);

    size_t total_rows = 0;
    for (const auto & [key, part_chunk] : result)
        total_rows += part_chunk.getNumRows();
    EXPECT_EQ(total_rows, 3u);
}

TEST(IcebergChunkPartitioner, CalculatorSkipsRowlessChunks)
{
    PartitionerFixture fx;
    IcebergPartitionCalculator calculator(fx.spec, fx.schema, fx.context, fx.header);

    /// The delete-file compaction pipeline feeds the calculator chunks that position deletes
    /// may have filtered down to zero rows. It must skip them, not abort or throw.
    auto rowless = PartitionerFixture::rowlessChunk();
    EXPECT_NO_THROW(calculator.transform(rowless));
    EXPECT_TRUE(calculator.getPartitionValue().empty());

    /// The partition value is latched from the first non-empty chunk instead.
    auto data = PartitionerFixture::chunkWithValues({"a", "a"});
    EXPECT_NO_THROW(calculator.transform(data));
    ASSERT_EQ(calculator.getPartitionValue().size(), 1u);
    EXPECT_EQ(calculator.getPartitionValue()[0].safeGet<String>(), "a");
}

TEST(IcebergChunkPartitioner, BucketKeyIsPublishedAsIcebergInt)
{
    PartitionerFixture fx("bucket[16]");
    ChunkPartitioner partitioner(fx.spec, fx.schema, fx.context, fx.header);

    ASSERT_EQ(partitioner.getResultTypes().size(), 1u);
    EXPECT_EQ(partitioner.getResultTypes()[0]->getTypeId(), TypeIndex::Int32);

    auto chunk = PartitionerFixture::chunkWithValues({"eu", "us"});
    auto result = partitioner.partitionChunk(chunk);
    ASSERT_EQ(result.size(), 2u);

    for (const auto & [key, _] : result)
    {
        ASSERT_EQ(key.size(), 1u);
        /// The keys carry the published type, not the transform function's `UInt32`. Both hold the
        /// same number, so only the Field's own tag distinguishes them.
        EXPECT_EQ(key[0].getType(), Field::Types::Int64);
        EXPECT_GE(key[0].safeGet<Int64>(), 0);
        EXPECT_LT(key[0].safeGet<Int64>(), 16);
    }
}

TEST(IcebergChunkPartitioner, NullableBucketKeyIsPublishedAsNullableIcebergInt)
{
    PartitionerFixture fx("bucket[16]", /*nullable_source=*/true);
    ChunkPartitioner partitioner(fx.spec, fx.schema, fx.context, fx.header);

    ASSERT_EQ(partitioner.getResultTypes().size(), 1u);
    EXPECT_TRUE(partitioner.getResultTypes()[0]->isNullable());
    EXPECT_EQ(removeNullable(partitioner.getResultTypes()[0])->getTypeId(), TypeIndex::Int32);

    auto chunk = PartitionerFixture::nullableChunkWithValues({"eu", std::nullopt});
    auto result = partitioner.partitionChunk(chunk);
    ASSERT_EQ(result.size(), 2u);

    size_t null_keys = 0;
    for (const auto & [key, _] : result)
    {
        ASSERT_EQ(key.size(), 1u);
        if (key[0].getType() == Field::Types::Null)
            ++null_keys;
        else
            EXPECT_EQ(key[0].getType(), Field::Types::Int64);
    }
    EXPECT_EQ(null_keys, 1u);
}

}

#endif
