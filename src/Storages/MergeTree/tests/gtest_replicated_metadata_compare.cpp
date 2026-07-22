#include <gtest/gtest.h>

#include <Common/tests/gtest_global_register.h>

#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>

using namespace DB;

/// The serialized expression fields of the replicated table metadata are compared as ASTs
/// (`sameAST`, i.e. by `getTreeHash`), not as text. Metadata written by a version affected by
/// #92340 keeps the redundant parentheses the user wrote (`sorting key: (b)`), while older
/// versions store the canonical form without them; formatting may also change for unrelated
/// aesthetic reasons. Equal definitions must compare equal in any stored form, and genuinely
/// different definitions must still differ — including differences that live outside the AST
/// children (index granularity, constraint type, TTL destination, projection clause roles).

namespace
{

struct MetadataFields
{
    String sampling = "b";
    String sorting_key = "b, c";
    String ttl = "d + toIntervalYear(10)";
    String indices = "ix b * c TYPE minmax GRANULARITY 1";
    String projections = "pr (SELECT b ORDER BY c)";
    String constraints = "cc CHECK a > 0";
    String primary_key = "b";
    String partition_key = "a";
};

ReplicatedMergeTreeTableMetadata makeMetadata(const MetadataFields & fields)
{
    String s = "metadata format version: 1\n"
               "date column: \n"
               "sampling expression: " + fields.sampling + "\n"
               "index granularity: 8192\n"
               "mode: 0\n"
               "sign column: \n"
               "primary key: " + fields.primary_key + "\n"
               "data format version: 1\n"
               "partition key: " + fields.partition_key + "\n";
    if (!fields.sorting_key.empty())
        s += "sorting key: " + fields.sorting_key + "\n";
    if (!fields.ttl.empty())
        s += "ttl: " + fields.ttl + "\n";
    if (!fields.indices.empty())
        s += "indices: " + fields.indices + "\n";
    if (!fields.projections.empty())
        s += "projections: " + fields.projections + "\n";
    if (!fields.constraints.empty())
        s += "constraints: " + fields.constraints + "\n";
    s += "merge parameters format version: 2\n";

    return ReplicatedMergeTreeTableMetadata::parseRaw(s);
}

ReplicatedMergeTreeTableMetadata::Diff diffOf(const MetadataFields & local_fields, const MetadataFields & zk_fields)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    return makeMetadata(local_fields).checkAndFindDiff(makeMetadata(zk_fields), "test_table");
}

}

TEST(ReplicatedMergeTreeTableMetadataCompare, ParenthesizedFormsCompareEqual)
{
    /// The local metadata is canonical, the Keeper metadata kept the redundant parentheses
    /// that #92340 preserves. The definitions are equal, so nothing must be reported changed.
    MetadataFields local;
    MetadataFields from_zk;
    from_zk.sampling = "(b)";
    from_zk.sorting_key = "(b), (c)";
    from_zk.ttl = "(d + toIntervalYear(10))";
    from_zk.indices = "ix (b * c) TYPE minmax GRANULARITY 1";
    from_zk.projections = "pr (SELECT (b) ORDER BY (c))";
    from_zk.constraints = "cc CHECK (a > 0)";
    from_zk.primary_key = "(b)";
    from_zk.partition_key = "(a)";

    auto diff = diffOf(local, from_zk);
    EXPECT_TRUE(diff.empty());
}

TEST(ReplicatedMergeTreeTableMetadataCompare, TupleWrappedKeysCompareEqual)
{
    /// A key stored as `tuple(...)` is the same key as its unwrapped form.
    MetadataFields local;
    MetadataFields from_zk;
    from_zk.sorting_key = "tuple(b, c)";
    from_zk.primary_key = "tuple(b)";

    auto diff = diffOf(local, from_zk);
    EXPECT_TRUE(diff.empty());
}

TEST(ReplicatedMergeTreeTableMetadataCompare, GenuineChangesAreStillDetected)
{
    /// Genuinely different definitions, stored with the redundant parentheses: the change must
    /// still be detected, and the new value recorded in the diff is the stored string as is.
    MetadataFields local;
    MetadataFields from_zk;
    from_zk.sampling = "(c)";
    from_zk.sorting_key = "(b), (d)";
    from_zk.ttl = "(d + toIntervalYear(20))";
    from_zk.constraints = "cc CHECK (a > 1)";

    auto diff = diffOf(local, from_zk);
    EXPECT_TRUE(diff.sampling_expression_changed);
    EXPECT_EQ(diff.new_sampling_expression, "(c)");
    EXPECT_TRUE(diff.sorting_key_changed);
    EXPECT_EQ(diff.new_sorting_key, "(b), (d)");
    EXPECT_TRUE(diff.ttl_table_changed);
    EXPECT_EQ(diff.new_ttl_table, "(d + toIntervalYear(20))");
    EXPECT_TRUE(diff.constraints_changed);
    EXPECT_EQ(diff.new_constraints, "cc CHECK (a > 1)");
    EXPECT_FALSE(diff.skip_indices_changed);
    EXPECT_FALSE(diff.projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, SortDirectionIsSignificant)
{
    /// `b DESC` and `b` describe different layouts; unwrapping `tuple(...)` and ignoring the
    /// parentheses must not drop the sort direction.
    MetadataFields local;
    MetadataFields from_zk;
    from_zk.sorting_key = "b DESC, c";

    auto diff = diffOf(local, from_zk);
    EXPECT_TRUE(diff.sorting_key_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, ImmutableKeyMismatchThrows)
{
    MetadataFields local;

    MetadataFields other_primary;
    other_primary.primary_key = "c";
    EXPECT_ANY_THROW(diffOf(local, other_primary));

    MetadataFields reverse_primary;
    reverse_primary.primary_key = "b DESC";
    EXPECT_ANY_THROW(diffOf(local, reverse_primary));

    MetadataFields other_partition;
    other_partition.partition_key = "b";
    EXPECT_ANY_THROW(diffOf(local, other_partition));
}

TEST(ReplicatedMergeTreeTableMetadataCompare, TTLSemanticsOutsideExpressionAreSignificant)
{
    /// Parts of a TTL element that are not AST children: mode, destination, GROUP BY keys and
    /// assignments, recompression codec.
    MetadataFields local;

    MetadataFields to_disk;
    to_disk.ttl = "d + toIntervalYear(10) TO DISK 'd1'";
    EXPECT_TRUE(diffOf(local, to_disk).ttl_table_changed);

    MetadataFields disk1;
    disk1.ttl = "d + toIntervalYear(10) TO DISK 'd1'";
    MetadataFields disk2;
    disk2.ttl = "d + toIntervalYear(10) TO DISK 'd2'";
    EXPECT_TRUE(diffOf(disk1, disk2).ttl_table_changed);
    EXPECT_FALSE(diffOf(disk1, disk1).ttl_table_changed);

    MetadataFields volume;
    volume.ttl = "d + toIntervalYear(10) TO VOLUME 'd1'";
    EXPECT_TRUE(diffOf(disk1, volume).ttl_table_changed);

    MetadataFields group_by_max;
    group_by_max.ttl = "d + toIntervalYear(10) GROUP BY b SET c = max(c)";
    MetadataFields group_by_min;
    group_by_min.ttl = "d + toIntervalYear(10) GROUP BY b SET c = min(c)";
    EXPECT_TRUE(diffOf(group_by_max, group_by_min).ttl_table_changed);

    MetadataFields group_by_max_parens;
    group_by_max_parens.ttl = "(d + toIntervalYear(10)) GROUP BY b SET c = max((c))";
    EXPECT_FALSE(diffOf(group_by_max, group_by_max_parens).ttl_table_changed);

    MetadataFields recompress_zstd;
    recompress_zstd.ttl = "d + toIntervalYear(10) RECOMPRESS CODEC(ZSTD(1))";
    MetadataFields recompress_lz4;
    recompress_lz4.ttl = "d + toIntervalYear(10) RECOMPRESS CODEC(LZ4)";
    EXPECT_TRUE(diffOf(recompress_zstd, recompress_lz4).ttl_table_changed);
    EXPECT_FALSE(diffOf(recompress_zstd, recompress_zstd).ttl_table_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, DeclarationIdentityIsSignificant)
{
    /// Parts of index/projection/constraint declarations that are not AST children:
    /// names, index granularity, constraint type, projection clause roles.
    MetadataFields local;

    MetadataFields renamed_index;
    renamed_index.indices = "ix2 b * c TYPE minmax GRANULARITY 1";
    EXPECT_TRUE(diffOf(local, renamed_index).skip_indices_changed);

    MetadataFields other_granularity;
    other_granularity.indices = "ix b * c TYPE minmax GRANULARITY 2";
    EXPECT_TRUE(diffOf(local, other_granularity).skip_indices_changed);

    MetadataFields renamed_projection;
    renamed_projection.projections = "pr2 (SELECT b ORDER BY c)";
    EXPECT_TRUE(diffOf(local, renamed_projection).projections_changed);

    MetadataFields group_by_projection;
    group_by_projection.projections = "pr (SELECT b GROUP BY c)";
    EXPECT_TRUE(diffOf(local, group_by_projection).projections_changed);

    MetadataFields assume_constraint;
    assume_constraint.constraints = "cc ASSUME a > 0";
    EXPECT_TRUE(diffOf(local, assume_constraint).constraints_changed);

    MetadataFields renamed_constraint;
    renamed_constraint.constraints = "cc2 CHECK a > 0";
    EXPECT_TRUE(diffOf(local, renamed_constraint).constraints_changed);
}
