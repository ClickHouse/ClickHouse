#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{
namespace
{

UpdateAffectedColumns getAffectedColumns(const String & update)
{
    MutationCommands commands;
    auto & command = commands.emplace_back();
    command.type = MutationCommand::UPDATE;
    command.ast_text = update;
    return getUpdateAffectedColumns(commands, getContext().context);
}

void expectConflict(const UpdateAffectedColumns & reader, const UpdateAffectedColumns & writer)
{
    EXPECT_TRUE(reader.hasConflict(writer));
    EXPECT_TRUE(writer.hasConflict(reader));

    /// Plain MergeTree uses counters and must detect the conflict in either arrival order.
    UpdateAffectedColumnsWithCounters in_progress;
    in_progress.add(reader);
    in_progress.add(reader);
    EXPECT_TRUE(in_progress.hasConflict(writer));
    in_progress.remove(reader);
    EXPECT_TRUE(in_progress.hasConflict(writer));
    in_progress.remove(reader);
    EXPECT_FALSE(in_progress.hasConflict(writer));

    in_progress.add(writer);
    EXPECT_TRUE(in_progress.hasConflict(reader));
    in_progress.remove(writer);
    EXPECT_FALSE(in_progress.hasConflict(reader));

    /// ReplicatedMergeTree reads the same sets back from Keeper.
    UpdateAffectedColumns restored;
    restored.fromString(reader.toString());
    EXPECT_EQ(restored.used, reader.used);
    EXPECT_EQ(restored.updated, reader.updated);
    EXPECT_TRUE(restored.hasConflict(writer));
    EXPECT_TRUE(writer.hasConflict(restored));
}

}

TEST(UpdateAffectedColumns, CorrelatedAssignmentTracksQualifiedColumns)
{
    const auto writer = getAffectedColumns("UPDATE lookup_key = 42 WHERE 1");

    for (const auto * key : {
        "lookup_key",
        "target.lookup_key",
        "db.target.lookup_key",
        "`target.with.dot`.lookup_key",
        "`db.with.dot`.`target.with.dot`.lookup_key"})
    {
        SCOPED_TRACE(key);
        const auto reader = getAffectedColumns(
            String("UPDATE value = (SELECT s.result FROM source AS s WHERE s.id = ") + key + ") WHERE 1");

        EXPECT_TRUE(reader.used.contains("lookup_key"));
        EXPECT_EQ(reader.updated, (NameSet{"value"}));
        expectConflict(reader, writer);
    }
}

TEST(UpdateAffectedColumns, QualifiedPredicateTracksUsedColumns)
{
    const auto reader = getAffectedColumns("UPDATE value = 0 WHERE db.target.lookup_key = 1");
    EXPECT_TRUE(reader.used.contains("db.target.lookup_key"));
    EXPECT_TRUE(reader.used.contains("target.lookup_key"));
    EXPECT_TRUE(reader.used.contains("lookup_key"));
    expectConflict(reader, getAffectedColumns("UPDATE lookup_key = 42 WHERE 1"));
}

TEST(UpdateAffectedColumns, QuotedColumnNamesKeepEmbeddedDots)
{
    const auto writer = getAffectedColumns("UPDATE `lookup.key` = 42 WHERE 1");

    for (const auto * key : {"`lookup.key`", "target.`lookup.key`", "db.target.`lookup.key`"})
    {
        SCOPED_TRACE(key);
        const auto reader = getAffectedColumns(
            String("UPDATE value = (SELECT s.result FROM source AS s WHERE s.id = ") + key + ") WHERE 1");

        EXPECT_TRUE(reader.used.contains("lookup.key"));
        EXPECT_FALSE(reader.used.contains("key"));
        expectConflict(reader, writer);
    }
}

TEST(UpdateAffectedColumns, QualifiedDottedColumnsKeepIntermediateSuffixes)
{
    const auto reader = getAffectedColumns(
        "UPDATE value = (SELECT s.result FROM source AS s WHERE s.id = db.target.nested.key) WHERE 1");

    EXPECT_TRUE(reader.used.contains("db.target.nested.key"));
    EXPECT_TRUE(reader.used.contains("target.nested.key"));
    EXPECT_TRUE(reader.used.contains("nested.key"));
    expectConflict(reader, getAffectedColumns("UPDATE `nested.key` = 42 WHERE 1"));
}

TEST(UpdateAffectedColumns, UnrelatedAndWriteOnlyUpdatesDoNotConflict)
{
    const auto reader = getAffectedColumns(
        "UPDATE value = (SELECT s.result FROM source AS s WHERE s.id = target.lookup_key) WHERE 1");
    const auto unrelated = getAffectedColumns("UPDATE untouched = 1 WHERE 1");
    EXPECT_FALSE(reader.hasConflict(unrelated));
    EXPECT_FALSE(unrelated.hasConflict(reader));
    EXPECT_FALSE(reader.hasConflict(reader));

    const auto first = getAffectedColumns("UPDATE value = 1 WHERE 1");
    const auto second = getAffectedColumns("UPDATE value = 2 WHERE 1");
    EXPECT_TRUE(first.used.empty());
    EXPECT_TRUE(second.used.empty());
    EXPECT_FALSE(first.hasConflict(second));
    EXPECT_FALSE(second.hasConflict(first));
}

TEST(UpdateAffectedColumns, MultipleAssignmentsKeepReadAndWriteSetsSeparate)
{
    const auto reader = getAffectedColumns(
        "UPDATE value = (SELECT s.result FROM source AS s WHERE s.id = target.lookup_key), "
        "other = target.region WHERE target.active = 1");

    EXPECT_EQ(reader.updated, (NameSet{"value", "other"}));
    for (const auto * column : {"lookup_key", "region", "active"})
    {
        SCOPED_TRACE(column);
        EXPECT_TRUE(reader.used.contains(column));
        expectConflict(reader, getAffectedColumns(String("UPDATE ") + column + " = 42 WHERE 1"));
    }
}

}
