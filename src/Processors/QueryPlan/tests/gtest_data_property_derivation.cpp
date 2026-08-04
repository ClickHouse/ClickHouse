#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/DataPropertyDerivation.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <array>

using namespace DB;
using namespace DB::QueryPlanOptimizations;
using DataPropertyColumnSet = DB::QueryPlanOptimizations::ColumnSet;

namespace
{

void addColumn(Block & header, const String & name, const DataTypePtr & type)
{
    header.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
}

}

TEST(DataPropertyDerivation, DeclaredUniqueKeyMapsToOutputPositions)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(header, "tenant", std::make_shared<DataTypeUInt32>());

    StorageInMemoryMetadata metadata;
    metadata.unique_key.column_names = {"tenant", "id"};

    auto properties = deriveDataPropertiesForStorageRead(header, &metadata);

    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "id"}, {1, "tenant"}}));
    EXPECT_EQ(properties.uniqueKeys().front().provenance, DataPropertyProvenance::storageDeclaration());
    EXPECT_EQ(properties.uniqueKeys().front().equality_mode, DataPropertyEqualityMode::NonNullOrdinaryEquality);
}

TEST(DataPropertyDerivation, MissingOrAmbiguousUniqueKeyIsDropped)
{
    StorageInMemoryMetadata metadata;
    metadata.unique_key.column_names = {"id"};

    Block missing_header;
    addColumn(missing_header, "value", std::make_shared<DataTypeUInt64>());
    EXPECT_TRUE(deriveDataPropertiesForStorageRead(missing_header, &metadata).uniqueKeys().empty());

    Block ambiguous_header;
    addColumn(ambiguous_header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(ambiguous_header, "id", std::make_shared<DataTypeUInt64>());
    EXPECT_TRUE(deriveDataPropertiesForStorageRead(ambiguous_header, &metadata).uniqueKeys().empty());
}

TEST(DataPropertyDerivation, PrimarySortingAndPartitionKeysAreNotUnique)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());

    StorageInMemoryMetadata metadata;
    metadata.primary_key.column_names = {"id"};
    metadata.sorting_key.column_names = {"id"};
    metadata.partition_key.column_names = {"id"};

    auto properties = deriveDataPropertiesForStorageRead(header, &metadata);
    EXPECT_TRUE(properties.uniqueKeys().empty());
}

TEST(DataPropertyDerivation, NonNullFactsHandleLowCardinalityNullable)
{
    Block header;
    addColumn(header, "plain", std::make_shared<DataTypeUInt64>());
    addColumn(header, "nullable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()));
    addColumn(header, "low_cardinality", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    addColumn(
        header,
        "low_cardinality_nullable",
        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())));
    addColumn(header, "dynamic", std::make_shared<DataTypeDynamic>());
    addColumn(
        header,
        "variant",
        std::make_shared<DataTypeVariant>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>()}));

    auto properties = deriveDataPropertiesForStorageRead(header, nullptr);

    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "plain"}, {2, "low_cardinality"}}));
}

TEST(DataPropertyDerivation, NonLeafStepDoesNotRunLeafDerivation)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);
    LimitStep limit(shared_header, 10, 0);

    DataPropertySet child;
    child.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{0, "id"}}));
    const std::array children{child};

    auto properties = deriveDataProperties(limit, children);
    EXPECT_TRUE(properties.empty());
}
