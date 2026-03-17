#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/PhysicalNameMapping.h>


using namespace DB;

namespace
{

DataTypePtr uint64Type()
{
    static const auto type = std::make_shared<DataTypeUInt64>();
    return type;
}

NamesAndTypesList makeColumns(std::initializer_list<String> names)
{
    NamesAndTypesList columns;
    for (const auto & name : names)
        columns.emplace_back(name, uint64Type());
    return columns;
}

}

TEST(PhysicalNameMapping, DropReAddSameName)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"a"}));

    EXPECT_EQ(mapping.getPhysicalName("a"), "a");

    mapping.removeColumn("a");
    auto new_physical_name = mapping.allocatePhysicalName();
    mapping.addColumn("a", new_physical_name);

    EXPECT_EQ(mapping.getPhysicalName("a"), "1");
    EXPECT_EQ(new_physical_name, "1");
    EXPECT_NE(new_physical_name, "a");
}

TEST(PhysicalNameMapping, CounterWithNumericColumnNames)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"2", "a", "10"}));

    EXPECT_EQ(mapping.allocatePhysicalName(), "11");
}

TEST(PhysicalNameMapping, RenamePreservesPhysical)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"a", "b"}));

    mapping.renameColumn("a", "c");

    EXPECT_FALSE(mapping.hasLogicalName("a"));
    EXPECT_TRUE(mapping.hasLogicalName("c"));
    EXPECT_EQ(mapping.getPhysicalName("c"), "a");
    EXPECT_EQ(mapping.getLogicalName("a"), "c");
}

TEST(PhysicalNameMapping, SerializeDeserializeRoundTrip)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"10", "a"}));
    auto new_physical_name = mapping.allocatePhysicalName();
    mapping.addColumn("c", new_physical_name);
    mapping.renameColumn("a", "b");

    auto restored = PhysicalNameMapping::fromString(mapping.toString());

    EXPECT_TRUE(restored.isActive());
    EXPECT_EQ(restored.getPhysicalName("10"), "10");
    EXPECT_EQ(restored.getPhysicalName("b"), "a");
    EXPECT_EQ(restored.getPhysicalName("c"), "11");
    EXPECT_EQ(restored.getLogicalName("a"), "b");
    EXPECT_EQ(restored.allocatePhysicalName(), "12");
}

TEST(PhysicalNameMapping, UnmappedColumnsPassthrough)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"a"}));

    EXPECT_EQ(mapping.getPhysicalNameOrDefault("_row_exists"), "_row_exists");

    auto columns = makeColumns({"a", "_row_exists"});
    populatePhysicalNames(columns, mapping);

    auto a = columns.tryGetByName("a");
    auto row_exists = columns.tryGetByName("_row_exists");

    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(row_exists.has_value());
    EXPECT_EQ(a->getPhysicalNameInStorage(), "a");
    EXPECT_EQ(row_exists->getPhysicalNameInStorage(), "_row_exists");
}

TEST(PhysicalNameMapping, ConcurrentDropAddCycle)
{
    auto mapping = PhysicalNameMapping::createForExistingTable(makeColumns({"a"}));

    mapping.removeColumn("a");
    auto b_physical_name = mapping.allocatePhysicalName();
    mapping.addColumn("b", b_physical_name);

    mapping.removeColumn("b");
    auto a_physical_name = mapping.allocatePhysicalName();
    mapping.addColumn("a", a_physical_name);

    EXPECT_EQ(b_physical_name, "1");
    EXPECT_EQ(a_physical_name, "2");
    EXPECT_EQ(mapping.getPhysicalName("a"), "2");
    EXPECT_NE(a_physical_name, b_physical_name);
}
