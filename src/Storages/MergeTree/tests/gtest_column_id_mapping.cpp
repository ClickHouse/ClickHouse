#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/ColumnIdMapping.h>


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

TEST(ColumnIdMapping, DropReAddSameName)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a"}));

    EXPECT_EQ(mapping.getColumnId("a"), "a");

    mapping.removeColumn("a");
    auto new_column_id = mapping.allocateColumnId();
    mapping.addColumn("a", new_column_id);

    EXPECT_EQ(mapping.getColumnId("a"), "1");
    EXPECT_EQ(new_column_id, "1");
    EXPECT_NE(new_column_id, "a");
}

TEST(ColumnIdMapping, CounterWithNumericColumnNames)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"2", "a", "10"}));

    EXPECT_EQ(mapping.allocateColumnId(), "11");
}

TEST(ColumnIdMapping, RenamePreservesPhysical)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a", "b"}));

    mapping.renameColumn("a", "c");

    EXPECT_FALSE(mapping.hasLogicalName("a"));
    EXPECT_TRUE(mapping.hasLogicalName("c"));
    EXPECT_EQ(mapping.getColumnId("c"), "a");
    EXPECT_EQ(mapping.getLogicalName("a"), "c");
}

TEST(ColumnIdMapping, SerializeDeserializeRoundTrip)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"10", "a"}));
    auto new_column_id = mapping.allocateColumnId();
    mapping.addColumn("c", new_column_id);
    mapping.renameColumn("a", "b");

    auto restored = ColumnIdMapping::fromString(mapping.toString());

    EXPECT_TRUE(restored.isActive());
    EXPECT_EQ(restored.getColumnId("10"), "10");
    EXPECT_EQ(restored.getColumnId("b"), "a");
    EXPECT_EQ(restored.getColumnId("c"), "11");
    EXPECT_EQ(restored.getLogicalName("a"), "b");
    EXPECT_EQ(restored.allocateColumnId(), "12");
}

TEST(ColumnIdMapping, DeserializeClampsNextColumnIdToExistingIds)
{
    auto restored = ColumnIdMapping::fromString(R"({
        "active": true,
        "next_column_id": 2,
        "mapping": {
            "a": "10",
            "b": "name"
        }
    })");

    EXPECT_EQ(restored.allocateColumnId(), "11");
}

TEST(ColumnIdMapping, UnmappedColumnsPassthrough)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a"}));

    EXPECT_EQ(mapping.getColumnIdOrDefault("_row_exists"), "_row_exists");

    auto columns = makeColumns({"a", "_row_exists"});
    populateColumnIds(columns, mapping);

    auto a = columns.tryGetByName("a");
    auto row_exists = columns.tryGetByName("_row_exists");

    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(row_exists.has_value());
    EXPECT_EQ(a->getColumnIdInStorage(), "a");
    EXPECT_EQ(row_exists->getColumnIdInStorage(), "_row_exists");
}

TEST(ColumnIdMapping, TwoPhaseRenameNormal)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a", "b"}));

    mapping.beginRename("a", "c");

    EXPECT_TRUE(mapping.hasLogicalName("a"));
    EXPECT_TRUE(mapping.hasLogicalName("c"));
    EXPECT_EQ(mapping.getColumnId("a"), "a");
    EXPECT_EQ(mapping.getColumnId("c"), "a");

    mapping.finishRename("a");

    EXPECT_FALSE(mapping.hasLogicalName("a"));
    EXPECT_TRUE(mapping.hasLogicalName("c"));
    EXPECT_EQ(mapping.getColumnId("c"), "a");
    EXPECT_EQ(mapping.getLogicalName("a"), "c");
}

TEST(ColumnIdMapping, TwoPhaseRenameCrashRecovery)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a", "b"}));

    mapping.beginRename("a", "c");

    auto serialized = mapping.toString();
    auto restored = ColumnIdMapping::fromString(serialized);

    EXPECT_TRUE(restored.hasLogicalName("a"));
    EXPECT_TRUE(restored.hasLogicalName("c"));
    /// Both "a" and "c" map to physical "a"; reverse map must be deterministic
    /// (lexicographically smallest logical name wins).
    EXPECT_EQ(restored.getLogicalName("a"), "a");

    restored.removeColumn("c");

    EXPECT_TRUE(restored.hasLogicalName("a"));
    EXPECT_FALSE(restored.hasLogicalName("c"));
    EXPECT_EQ(restored.getColumnId("a"), "a");
    EXPECT_EQ(restored.getLogicalName("a"), "a");
}

TEST(ColumnIdMapping, TwoPhaseRenameRemoveOldPreservesPhysical)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a", "b"}));

    auto phys = mapping.allocateColumnId();
    mapping.addColumn("c", phys);

    mapping.beginRename("c", "d");

    mapping.removeColumn("c");

    EXPECT_TRUE(mapping.hasLogicalName("d"));
    EXPECT_EQ(mapping.getColumnId("d"), phys);
    EXPECT_TRUE(mapping.hasColumnId(phys));
}

TEST(ColumnIdMapping, ConcurrentDropAddCycle)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a"}));

    mapping.removeColumn("a");
    auto b_column_id = mapping.allocateColumnId();
    mapping.addColumn("b", b_column_id);

    mapping.removeColumn("b");
    auto a_column_id = mapping.allocateColumnId();
    mapping.addColumn("a", a_column_id);

    EXPECT_EQ(b_column_id, "1");
    EXPECT_EQ(a_column_id, "2");
    EXPECT_EQ(mapping.getColumnId("a"), "2");
    EXPECT_NE(a_column_id, b_column_id);
}

TEST(ColumnIdMapping, RenameToExistingColumnIdIsRejected)
{
    auto mapping = ColumnIdMapping::createForExistingTable(makeColumns({"a", "b"}));
    auto id = mapping.allocateColumnId();
    mapping.addColumn("c", id);

    /// Renaming "a" to "1" collides with column c's ID "1".
    EXPECT_THROW(mapping.renameColumn("a", id), Exception);
    EXPECT_THROW(mapping.beginRename("a", id), Exception);

    /// Self-case: renaming "c" to its own ID "1" is allowed.
    EXPECT_NO_THROW(mapping.renameColumn("c", id));
    EXPECT_EQ(mapping.getColumnId(id), id);
}
