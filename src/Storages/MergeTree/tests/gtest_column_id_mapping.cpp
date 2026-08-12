#include <gtest/gtest.h>

#include <base/defines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Common/Exception.h>


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
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));

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
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"2", "a", "10"}));

    EXPECT_EQ(mapping.allocateColumnId(), "11");
}

TEST(ColumnIdMapping, CounterCoversBothHalvesOfACompoundId)
{
    /// A flattened Nested child id spends a counter value on each half, so both halves have to
    /// push the counter up -- whether they come from a column name at activation or from a
    /// mapping an earlier ALTER wrote.
    auto identity = ColumnIdMapping::createIdentity(makeColumns({"n.3", "a"}));
    EXPECT_EQ(identity.allocateColumnId(), "4");

    auto restored = ColumnIdMapping::fromString(R"({
        "active": true,
        "next_column_id": 2,
        "mapping": {
            "n.x": "1.7",
            "n.y": "1.5"
        }
    })");

    EXPECT_EQ(restored.allocateColumnId(), "8");
}

TEST(ColumnIdMapping, RenamePreservesColumnId)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));

    mapping.beginRename("a", "c");
    mapping.finishRename("a");

    EXPECT_FALSE(mapping.hasLogicalName("a"));
    EXPECT_TRUE(mapping.hasLogicalName("c"));
    EXPECT_EQ(mapping.getColumnId("c"), "a");
    EXPECT_EQ(mapping.getLogicalName("a"), "c");
}

TEST(ColumnIdMapping, SerializeDeserializeRoundTrip)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"10", "a"}));
    auto new_column_id = mapping.allocateColumnId();
    mapping.addColumn("c", new_column_id);
    mapping.beginRename("a", "b");
    mapping.finishRename("a");

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
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));

    EXPECT_EQ(mapping.getColumnIdOrDefault("_row_exists"), "_row_exists");

    /// A virtual column is left UNSTAMPED; `getColumnId()` falls back to the name,
    /// so empty is equivalent to the old write behavior of stamping it to its own name.
    auto columns = makeColumns({"a", "_row_exists"});
    mapping.stampColumnIds(columns);

    auto a = columns.tryGetByName("a");
    auto row_exists = columns.tryGetByName("_row_exists");

    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(row_exists.has_value());
    EXPECT_EQ(a->getColumnId().value(), "a");
    EXPECT_TRUE(row_exists->column_id.empty());
    EXPECT_EQ(row_exists->getColumnId().value(), "_row_exists");
}

TEST(ColumnIdMapping, TwoPhaseRenameNormal)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));

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
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));

    mapping.beginRename("a", "c");

    auto serialized = mapping.toString();
    auto restored = ColumnIdMapping::fromString(serialized);

    EXPECT_TRUE(restored.hasLogicalName("a"));
    EXPECT_TRUE(restored.hasLogicalName("c"));
    /// Both "a" and "c" map to column ID "a"; reverse map must be deterministic
    /// (lexicographically smallest logical name wins).
    EXPECT_EQ(restored.getLogicalName("a"), "a");

    restored.removeColumn("c");

    EXPECT_TRUE(restored.hasLogicalName("a"));
    EXPECT_FALSE(restored.hasLogicalName("c"));
    EXPECT_EQ(restored.getColumnId("a"), "a");
    EXPECT_EQ(restored.getLogicalName("a"), "a");
}

TEST(ColumnIdMapping, TwoPhaseRenameRemoveOldPreservesColumnId)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));

    auto column_id = mapping.allocateColumnId();
    mapping.addColumn("c", column_id);

    mapping.beginRename("c", "d");

    mapping.removeColumn("c");

    EXPECT_TRUE(mapping.hasLogicalName("d"));
    EXPECT_EQ(mapping.getColumnId("d"), column_id);
    EXPECT_TRUE(mapping.hasColumnId(column_id));
}

TEST(ColumnIdMapping, ConcurrentDropAddCycle)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));

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
    /// Renaming a column to a name equal to another active column's id is rejected: on-disk
    /// artifacts are keyed by the column id, so such a logical name makes name-vs-id resolution
    /// ambiguous (reachable via a mutation that then reads/writes the wrong streams). The
    /// two-phase rotation a->x; b->a is a special case of this and is likewise rejected.
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));
    auto id = mapping.allocateColumnId();
    mapping.addColumn("c", id);
    ASSERT_EQ(id, "1");

    /// Renaming "a" to "1" collides with column c's id "1".
    EXPECT_THROW(mapping.beginRename("a", id), Exception);

    /// Two-phase rotation: after a->x, "x" holds id "a", so renaming "b" to "a" is rejected.
    auto rot = ColumnIdMapping::createIdentity(makeColumns({"a", "b"}));
    rot.beginRename("a", "x");
    rot.finishRename("a");
    EXPECT_THROW(rot.beginRename("b", "a"), Exception);

    /// Self-case: renaming "c" to its own id "1" is allowed.
    EXPECT_NO_THROW(mapping.beginRename("c", id));
    EXPECT_NO_THROW(mapping.finishRename("c"));
    EXPECT_EQ(mapping.getColumnId(id), id);
}

/// stampColumnIds must NOT clobber a column that already carries a part-local id.
/// Callers such as MergeTreeDataPartWide::getListOfStreamsForColumn (subcolumn sizes) feed
/// an already-stamped part column into the reader factory, which then re-stamps off the
/// *live* metadata mapping. After a DROP + re-ADD reuses the logical name at a fresh id,
/// re-stamping would rewrite the old part's real id to the live one and mis-resolve its
/// streams. Columns that arrive id-less (the main read path's query columns) must still
/// get stamped.
TEST(ColumnIdMapping, StampForReadPreservesExistingId)
{
    /// Live mapping after DROP COLUMN a; ADD COLUMN a ...: name "a" now resolves to "1",
    /// while old parts still store it under the original id "a".
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));
    mapping.removeColumn("a");
    auto fresh_id = mapping.allocateColumnId();
    mapping.addColumn("a", fresh_id);
    ASSERT_EQ(fresh_id, "1");
    ASSERT_EQ(mapping.getColumnId("a"), "1");

    /// A pre-stamped old-part column keeps its own id.
    NamesAndTypesList prestamped = makeColumns({"a"});
    prestamped.front().setColumnId(ColumnId{"a"});
    mapping.stampColumnIds(prestamped);
    EXPECT_EQ(prestamped.front().getColumnId().value(), "a");

    /// An id-less query column (the main read path) still gets stamped to the live id.
    NamesAndTypesList idless = makeColumns({"a"});
    mapping.stampColumnIds(idless);
    EXPECT_EQ(idless.front().getColumnId().value(), "1");
}

/// Change-detector tied to the `addPersistent(...)` set in MergeTreeData::createVirtuals.
/// `isPersistentVirtualColumn` hardcodes that set; if a new persistent virtual column is
/// added there, BOTH `isPersistentVirtualColumn` and this test must be updated. Persistent
/// virtuals are stored in parts and must NOT be remapped by the column ID mapping;
/// misclassifying one would corrupt its stream resolution.
TEST(ColumnIdMapping, IsPersistentVirtualColumnMatchesAddPersistentSet)
{
    /// Exactly the three columns added via addPersistent(...).
    EXPECT_TRUE(isPersistentVirtualColumn(RowExistsColumn::name));
    EXPECT_TRUE(isPersistentVirtualColumn(BlockNumberColumn::name));
    EXPECT_TRUE(isPersistentVirtualColumn(BlockOffsetColumn::name));

    /// A sample of ephemeral (read-computed) virtuals must be excluded.
    EXPECT_FALSE(isPersistentVirtualColumn(PartDataVersionColumn::name));
    EXPECT_FALSE(isPersistentVirtualColumn("_part"));
    EXPECT_FALSE(isPersistentVirtualColumn("_part_offset"));
}

/// Drift guard: `isVirtualColumn` and `MergeTreeData::createVirtuals` now share one registry
/// (`getMergeTreeVirtuals`), so they cannot drift. This asserts the invariant end-to-end
/// (were the single source ever re-forked, this catches it): the predicate must cover every
/// virtual `createVirtuals` registers, plus `_partition_value` (added only when a partition key
/// is present, so absent from `createVirtuals(nullptr)`). With strict stamping, a miss means the
/// stamp treats a virtual as a real stored column and throws.
TEST(ColumnIdMapping, IsVirtualColumnCoversCreateVirtuals)
{
    const auto virtuals = MergeTreeData::createVirtuals(nullptr);
    for (const auto & column : virtuals)
        EXPECT_TRUE(isVirtualColumn(column.name)) << "createVirtuals registers '" << column.name
            << "' but isVirtualColumn does not cover it";

    EXPECT_TRUE(isVirtualColumn(PartitionValueColumn::name));

    /// A real user column is not virtual.
    EXPECT_FALSE(isVirtualColumn("a"));
}

/// Unified stamp (write == read): a mapped column takes its id, a virtual is left UNSTAMPED
/// (empty ≡ name-keyed on disk), and an already-stamped part-local id is preserved (not
/// clobbered by the live mapping after a DROP + re-ADD name reuse).
TEST(ColumnIdMapping, StampColumnIdsStampsMappedLeavesVirtualEmpty)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));
    mapping.removeColumn("a");
    auto fresh_id = mapping.allocateColumnId();
    mapping.addColumn("a", fresh_id);
    ASSERT_EQ(fresh_id, "1");

    auto columns = makeColumns({"a", BlockNumberColumn::name});
    mapping.stampColumnIds(columns);
    EXPECT_EQ(columns.tryGetByName("a")->getColumnId().value(), "1");
    /// Virtual left unstamped; name fallback yields its own name (the old write behavior).
    EXPECT_TRUE(columns.tryGetByName(BlockNumberColumn::name)->column_id.empty());
    EXPECT_EQ(columns.tryGetByName(BlockNumberColumn::name)->getColumnId().value(), BlockNumberColumn::name);

    NamesAndTypesList prestamped = makeColumns({"a"});
    prestamped.front().setColumnId(ColumnId{"42"});
    mapping.stampColumnIds(prestamped);
    EXPECT_EQ(prestamped.front().getColumnId().value(), "42");
}

/// Lenient stamp: mapped columns get their id, columns outside the mapping (synthetic
/// projection aggregates, not-yet-applied ALTER columns, a projection part's parent-mapped
/// columns during loadColumns) are left UNSTAMPED, and it never throws.
TEST(ColumnIdMapping, StampColumnIdsLenientLeavesUnmappedEmpty)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a", "c"}));

    auto columns = makeColumns({"a", "sum(c)"});
    EXPECT_NO_THROW(mapping.stampColumnIdsLenient(columns));
    EXPECT_EQ(columns.tryGetByName("a")->getColumnId().value(), "a");
    EXPECT_TRUE(columns.tryGetByName("sum(c)")->column_id.empty());
}

/// Discriminating: a real stored column absent from an active mapping is a schema/mapping
/// desync. The unified stamp must fail loud (LOGICAL_ERROR -> abort under debug/sanitizer, a
/// throw otherwise) instead of silently defaulting the id to the name / leaving it empty.
/// With the old lenient code it would not fail, so this is RED without the fix.
TEST(ColumnIdMappingDeathTest, StampColumnIdsRejectsUnmappedColumn)
{
    auto mapping = ColumnIdMapping::createIdentity(makeColumns({"a"}));
    auto columns = makeColumns({"a", "ghost"});
#ifdef DEBUG_OR_SANITIZER_BUILD
    EXPECT_DEATH({ mapping.stampColumnIds(columns); }, "desynced");
#else
    EXPECT_THROW(mapping.stampColumnIds(columns), DB::Exception);
#endif
}
