#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/StorageInMemoryMetadata.h>

using namespace DB;

namespace
{

/// A metadata snapshot carrying nothing but a partition key, which is all serializeText reads.
StorageMetadataPtr metadataWithPartitionKey(const NamesAndTypesList & key_columns)
{
    ColumnsWithTypeAndName sample;
    for (const auto & column : key_columns)
        sample.emplace_back(column.type->createColumn(), column.type, column.name);

    KeyDescription key;
    key.sample_block = Block(sample);

    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->partition_key = std::move(key);
    return metadata;
}

}

/// MergeTreePartition::load sizes `value` and then fills it, so a throw part way through --
/// a failed read of partition.dat, for instance -- leaves default-constructed Fields, which
/// are Null. Serializing such a partition happens on paths that only mean to log the part,
/// so it must not throw; before the fix, inserting a Null into a non-nullable partition key
/// column raised LOGICAL_ERROR and aborted the server.
TEST(MergeTreePartitionSerializeNull, LowCardinalityStringKey)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto metadata = metadataWithPartitionKey({{"dc_name", type}});

    MergeTreePartition partition;
    partition.value = Row{Field()};

    String serialized;
    /// "ColumnUnique can't contain null values" without the fix.
    ASSERT_NO_THROW(serialized = partition.serializeToString(metadata));
    EXPECT_EQ(serialized, "ᴺᵁᴸᴸ");
}

TEST(MergeTreePartitionSerializeNull, PlainStringKey)
{
    auto metadata = metadataWithPartitionKey({{"dc_name", std::make_shared<DataTypeString>()}});

    MergeTreePartition partition;
    partition.value = Row{Field()};

    String serialized;
    ASSERT_NO_THROW(serialized = partition.serializeToString(metadata));
    EXPECT_EQ(serialized, "ᴺᵁᴸᴸ");
}

/// The tuple branch takes a different code path and needs the same guard, including the case
/// where only some of the values were deserialized before the throw. Tuple elements go through
/// serializeTextQuoted, which renders a null as NULL rather than the pretty ᴺᵁᴸᴸ above.
TEST(MergeTreePartitionSerializeNull, CompositeKeyPartiallyLoaded)
{
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto metadata = metadataWithPartitionKey({{"dc_name", lc_string}, {"shard", std::make_shared<DataTypeUInt64>()}});

    MergeTreePartition partition;
    partition.value = Row{Field("dc1"), Field()};

    String serialized;
    ASSERT_NO_THROW(serialized = partition.serializeToString(metadata));
    EXPECT_EQ(serialized, "('dc1',NULL)");
}

/// Non-null values must serialize exactly as before; the fix only widens the type for a Null.
TEST(MergeTreePartitionSerializeNull, NonNullValuesAreUnchanged)
{
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    /// A single key is written with serializeText, so the string is not quoted.
    MergeTreePartition single;
    single.value = Row{Field("dc1")};
    EXPECT_EQ(single.serializeToString(metadataWithPartitionKey({{"dc_name", lc_string}})), "dc1");

    MergeTreePartition composite;
    composite.value = Row{Field("dc1"), Field(UInt64(7))};
    EXPECT_EQ(
        composite.serializeToString(
            metadataWithPartitionKey({{"dc_name", lc_string}, {"shard", std::make_shared<DataTypeUInt64>()}})),
        "('dc1',7)");
}

/// An empty partition key still renders as tuple(), unaffected by the change.
TEST(MergeTreePartitionSerializeNull, EmptyValue)
{
    auto metadata = metadataWithPartitionKey({{"dc_name", std::make_shared<DataTypeString>()}});

    MergeTreePartition partition;
    EXPECT_EQ(partition.serializeToString(metadata), "tuple()");
}
