#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>

using namespace DB;

/// Equal definitions must compare equal in any stored form, and different ones must still differ,
/// including differences that live outside the AST children.

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

/// The bugfix validation compiles this test against the merge-base sources, where
/// `checkAndFindDiff` still takes a column set and a context to resolve the parsed
/// expressions against. Dispatch on the available signature (from a template, so the
/// discarded branch is not instantiated), so the test builds against both sources
/// and demonstrates the bug at runtime instead of breaking the "before" build.
template <typename Metadata>
ReplicatedMergeTreeTableMetadata::Diff callCheckAndFindDiff(const Metadata & local, const Metadata & from_zk)
{
    if constexpr (requires { local.checkAndFindDiff(from_zk, "test_table"); })
        return local.checkAndFindDiff(from_zk, "test_table");
    else
        return local.checkAndFindDiff(
            from_zk, ColumnsDescription{}, VirtualColumnsDescription{}, "test_table", getContext().context);
}

ReplicatedMergeTreeTableMetadata::Diff diffOf(const MetadataFields & local_fields, const MetadataFields & zk_fields)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    return callCheckAndFindDiff(makeMetadata(local_fields), makeMetadata(zk_fields));
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

TEST(ReplicatedMergeTreeTableMetadataCompare, NormalizeImplicitIndicesUsesAutomaticIndexSettings)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    ColumnsDescription columns;
    columns.add(ColumnDescription("x", std::make_shared<DataTypeUInt64>()));

    MetadataFields fields;
    /// The canonical implicit definition as an older replica would have written it: the index type
    /// is built with `makeASTFunction`, so it carries an (empty) argument list and formats as `minmax()`.
    fields.indices = "auto_minmax_index_x x TYPE minmax() GRANULARITY 1";
    const auto serialized = makeMetadata(fields).toString();

    /// While the automatic-index setting for the column's category is enabled, an explicitly
    /// declared index with the reserved prefix is rejected at CREATE/ALTER time, so the entry can
    /// only be legacy pre-25.12 implicit metadata and is stripped.
    auto implicit_metadata = ReplicatedMergeTreeTableMetadata::parseAndNormalize(
        serialized, columns,
        /* add_minmax_index_for_numeric_columns */ true, /* add_minmax_index_for_string_columns */ false,
        getContext().context);
    EXPECT_TRUE(implicit_metadata.skip_indices.empty());

    /// The reserved prefix is legal for an explicitly declared index exactly while both settings
    /// are disabled. Nothing is stripped then, so an explicit index stays visible to the metadata
    /// comparison even when the Keeper entry is newer than the local snapshot.
    auto explicit_metadata = ReplicatedMergeTreeTableMetadata::parseAndNormalize(
        serialized, columns,
        /* add_minmax_index_for_numeric_columns */ false, /* add_minmax_index_for_string_columns */ false,
        getContext().context);
    EXPECT_EQ(explicit_metadata.skip_indices, fields.indices);

    /// Only the setting matching the column's category strips the entry: a numeric column's index
    /// is untouched when only the string setting is enabled.
    auto other_category_metadata = ReplicatedMergeTreeTableMetadata::parseAndNormalize(
        serialized, columns,
        /* add_minmax_index_for_numeric_columns */ false, /* add_minmax_index_for_string_columns */ true,
        getContext().context);
    EXPECT_EQ(other_category_metadata.skip_indices, fields.indices);
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

TEST(ReplicatedMergeTreeTableMetadataCompare, TTLElementCloneIsIsolated)
{
    /// `formatDefinition` clones the AST before canonicalizing it, so the clone must not share any
    /// node with the metadata snapshot it was given. `recompression_codec` is not a child, so the
    /// copy constructor leaves it shared unless `clone` handles it explicitly.
    ParserTTLExpressionList parser;
    ASTPtr original = parseQuery(
        parser, "d + toIntervalYear(10) RECOMPRESS CODEC(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    const String original_text = original->formatWithSecretsOneLine();

    ASTPtr copy = original->clone();
    auto & copied_element = copy->children.at(0)->as<ASTTTLElement &>();
    ASSERT_TRUE(copied_element.recompression_codec);
    const auto & element = original->children.at(0)->as<const ASTTTLElement &>();
    EXPECT_NE(copied_element.recompression_codec.get(), element.recompression_codec.get());

    /// Mutating the clone the way the canonicalization does must not reach the original.
    copied_element.recompression_codec->setParenthesized(true);
    for (const auto & child : copied_element.recompression_codec->children)
        child->setParenthesized(true);
    EXPECT_EQ(original->formatWithSecretsOneLine(), original_text);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, ReverseSortingKeyDiffIsApplicable)
{
    /// The apply path must be symmetric with the comparison path: a sorting key stored with an
    /// explicit direction (`b DESC`) must be reparsed by `Diff::getNewMetadata` instead of being
    /// rejected by a plain expression parser (which would leave the `ALTER_METADATA` log entry
    /// retrying forever on the replica).
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto context = getContext().context;
    VirtualColumnsDescription virtuals;

    ColumnsDescription columns;
    columns.add(ColumnDescription("b", std::make_shared<DataTypeUInt64>()));
    columns.add(ColumnDescription("c", std::make_shared<DataTypeUInt64>()));

    StorageInMemoryMetadata old_metadata;
    old_metadata.columns = columns;
    old_metadata.sorting_key = KeyDescription::parse("b, c", columns, virtuals, context, /*allow_order=*/ true);
    old_metadata.primary_key = KeyDescription::parse("b", columns, virtuals, context, /*allow_order=*/ true);

    ReplicatedMergeTreeTableMetadata::Diff diff;
    diff.sorting_key_changed = true;
    diff.new_sorting_key = "b DESC, c";

    auto new_metadata = diff.getNewMetadata(columns, virtuals, context, old_metadata);
    ASSERT_EQ(new_metadata.sorting_key.column_names.size(), 2);
    EXPECT_EQ(new_metadata.sorting_key.column_names[0], "b");
    EXPECT_EQ(new_metadata.sorting_key.column_names[1], "c");
    ASSERT_EQ(new_metadata.sorting_key.reverse_flags.size(), 2);
    EXPECT_TRUE(new_metadata.sorting_key.reverse_flags[0]);
    EXPECT_FALSE(new_metadata.sorting_key.reverse_flags[1]);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, ParenthesizedKeyFromOldLeaderIsAppliedCanonically)
{
    /// A leader running a version that keeps redundant parentheses publishes a real key change as
    /// `sorting key: (b) DESC, c` / `sampling expression: (b)`. Applying that entry must store the
    /// same local metadata as the canonical text does, otherwise `SHOW CREATE` on this replica
    /// would report `ORDER BY tuple((b) DESC, c)` while the leader reports `ORDER BY (b DESC, c)`.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto context = getContext().context;
    VirtualColumnsDescription virtuals;

    ColumnsDescription columns;
    columns.add(ColumnDescription("b", std::make_shared<DataTypeUInt64>()));
    columns.add(ColumnDescription("c", std::make_shared<DataTypeUInt64>()));

    StorageInMemoryMetadata old_metadata;
    old_metadata.columns = columns;
    old_metadata.sorting_key = KeyDescription::parse("b, c", columns, virtuals, context, /*allow_order=*/ true);
    old_metadata.primary_key = KeyDescription::parse("b", columns, virtuals, context, /*allow_order=*/ true);
    old_metadata.sampling_key = KeyDescription::parse("c", columns, virtuals, context, /*allow_order=*/ false);

    auto apply = [&] (const String & sorting_key, const String & sampling_expression)
    {
        ReplicatedMergeTreeTableMetadata::Diff diff;
        diff.sorting_key_changed = true;
        diff.new_sorting_key = sorting_key;
        diff.sampling_expression_changed = true;
        diff.new_sampling_expression = sampling_expression;
        return diff.getNewMetadata(columns, virtuals, context, old_metadata);
    };

    auto canonical = apply("b DESC, c", "b");
    auto parenthesized = apply("(b) DESC, c", "(b)");

    /// What `applyMetadataChangesToCreateQuery` writes into the local `CREATE` statement.
    EXPECT_EQ(
        parenthesized.sorting_key.definition_ast->formatWithSecretsOneLine(),
        canonical.sorting_key.definition_ast->formatWithSecretsOneLine());
    EXPECT_EQ(
        parenthesized.sampling_key.definition_ast->formatWithSecretsOneLine(),
        canonical.sampling_key.definition_ast->formatWithSecretsOneLine());

    /// And the key itself is still the intended one.
    ASSERT_EQ(parenthesized.sorting_key.column_names.size(), 2);
    EXPECT_EQ(parenthesized.sorting_key.column_names[0], "b");
    EXPECT_EQ(parenthesized.sorting_key.column_names[1], "c");
    ASSERT_EQ(parenthesized.sorting_key.reverse_flags.size(), 2);
    EXPECT_TRUE(parenthesized.sorting_key.reverse_flags[0]);
    EXPECT_FALSE(parenthesized.sorting_key.reverse_flags[1]);
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

TEST(ReplicatedMergeTreeTableMetadataCompare, ApplyTransformerParametersAreSignificant)
{
    /// The `parameters` and `lambda` of a projection's `APPLY` transformer are not children, so
    /// reaching only the node itself would make `APPLY quantile(0.5)` and `APPLY quantile(0.9)`
    /// compare equal.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields quantile_median;
    quantile_median.projections = "pr (SELECT COLUMNS('b|c') APPLY quantile(0.5) GROUP BY a)";
    MetadataFields quantile_high;
    quantile_high.projections = "pr (SELECT COLUMNS('b|c') APPLY quantile(0.9) GROUP BY a)";
    EXPECT_TRUE(diffOf(quantile_median, quantile_high).projections_changed);
    EXPECT_FALSE(diffOf(quantile_median, quantile_median).projections_changed);

    MetadataFields lambda_median;
    lambda_median.projections = "pr (SELECT COLUMNS('b|c') APPLY (x -> quantile(0.5)(x)) GROUP BY a)";
    MetadataFields lambda_high;
    lambda_high.projections = "pr (SELECT COLUMNS('b|c') APPLY (x -> quantile(0.9)(x)) GROUP BY a)";
    EXPECT_TRUE(diffOf(lambda_median, lambda_high).projections_changed);
    EXPECT_FALSE(diffOf(lambda_median, lambda_median).projections_changed);

    /// The same declarations written with redundant parentheses are still equal, so the comparison
    /// has to reach through those same members.
    MetadataFields quantile_median_parens;
    quantile_median_parens.projections = "pr (SELECT COLUMNS('b|c') APPLY quantile((0.5)) GROUP BY (a))";
    EXPECT_FALSE(diffOf(quantile_median, quantile_median_parens).projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, WindowFrameIsSignificant)
{
    /// The frame of a window definition is not stored in `children` either: only the frame offsets
    /// are. Two projections whose windows aggregate over different frames are different.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields cumulative;
    cumulative.projections = "pr (SELECT a, sum(b) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) GROUP BY a, b)";
    MetadataFields current_row;
    current_row.projections = "pr (SELECT a, sum(b) OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND CURRENT ROW) GROUP BY a, b)";
    EXPECT_TRUE(diffOf(cumulative, current_row).projections_changed);
    EXPECT_FALSE(diffOf(cumulative, cumulative).projections_changed);

    MetadataFields range_frame;
    range_frame.projections = "pr (SELECT a, sum(b) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) GROUP BY a, b)";
    EXPECT_TRUE(diffOf(cumulative, range_frame).projections_changed);

    MetadataFields following;
    following.projections = "pr (SELECT a, sum(b) OVER (PARTITION BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) GROUP BY a, b)";
    MetadataFields preceding;
    preceding.projections = "pr (SELECT a, sum(b) OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) GROUP BY a, b)";
    EXPECT_TRUE(diffOf(following, preceding).projections_changed);

    /// Redundant parentheses inside the window definition still compare equal.
    MetadataFields cumulative_parens;
    cumulative_parens.projections = "pr (SELECT (a), sum((b)) OVER (PARTITION BY (a) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) GROUP BY a, b)";
    EXPECT_FALSE(diffOf(cumulative, cumulative_parens).projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, ProjectionWithSettingsIsSignificant)
{
    /// A projection's `WITH SETTINGS` clause is an `ASTSetQuery`, whose `default_settings` (the
    /// `x = DEFAULT` spelling) and `query_parameters` (the `param_x = ...` spelling) are not
    /// children: reaching only `changes` would make these declarations compare equal.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields granularity_default;
    granularity_default.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (index_granularity = DEFAULT)";
    MetadataFields block_size_default;
    block_size_default.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (max_compress_block_size = DEFAULT)";
    EXPECT_TRUE(diffOf(granularity_default, block_size_default).projections_changed);
    EXPECT_FALSE(diffOf(granularity_default, granularity_default).projections_changed);

    MetadataFields granularity_explicit;
    granularity_explicit.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (index_granularity = 42)";
    EXPECT_TRUE(diffOf(granularity_default, granularity_explicit).projections_changed);

    MetadataFields no_settings;
    EXPECT_TRUE(diffOf(granularity_default, no_settings).projections_changed);

    MetadataFields param_one;
    param_one.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (param_x = 1)";
    MetadataFields param_two;
    param_two.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (param_x = 2)";
    EXPECT_TRUE(diffOf(param_one, param_two).projections_changed);

    MetadataFields param_renamed;
    param_renamed.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (param_y = 1)";
    EXPECT_TRUE(diffOf(param_one, param_renamed).projections_changed);

    /// A setting whose value spells out the bytes of a query parameter of the same name. The
    /// parser strips the `param_` prefix, so the two entries only stay distinct because every
    /// carrier is length-prefixed and each list contributes its size.
    MetadataFields value_spelling_parameter;
    value_spelling_parameter.projections
        = "pr (SELECT b ORDER BY c) WITH SETTINGS (max_compress_block_size = 3458764513820540928)";
    MetadataFields parameter_spelling_value;
    parameter_spelling_value.projections = "pr (SELECT b ORDER BY c) WITH SETTINGS (param_max_compress_block_size = 0)";
    EXPECT_TRUE(diffOf(value_spelling_parameter, parameter_spelling_value).projections_changed);

    /// Two reset settings whose names concatenate to the name of a single reset setting.
    MetadataFields two_resets;
    two_resets.projections
        = "pr (SELECT b ORDER BY c) WITH SETTINGS (index_granularity = DEFAULT, max_compress_block_size = DEFAULT)";
    MetadataFields one_concatenated_reset;
    one_concatenated_reset.projections
        = "pr (SELECT b ORDER BY c) WITH SETTINGS (index_granularitymax_compress_block_size = DEFAULT)";
    EXPECT_TRUE(diffOf(two_resets, one_concatenated_reset).projections_changed);

    /// Declarations that differ only in formatting are still equal.
    MetadataFields granularity_default_parens;
    granularity_default_parens.projections = "pr (SELECT (b) ORDER BY (c)) WITH SETTINGS (index_granularity = DEFAULT)";
    EXPECT_FALSE(diffOf(granularity_default, granularity_default_parens).projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, AliasIsFramedInTheHash)
{
    /// An alias is hashed right before what `getID` writes, so without a length prefix the bytes of
    /// `fooIdentifier_bar` and of `bar AS Identifier_foo` are the same stream and two projections
    /// with different output columns would compare equal.
    MetadataFields unaliased;
    unaliased.projections = "pr (SELECT fooIdentifier_bar GROUP BY a, fooIdentifier_bar, bar)";
    MetadataFields aliased;
    aliased.projections = "pr (SELECT bar AS Identifier_foo GROUP BY a, fooIdentifier_bar, bar)";
    EXPECT_TRUE(diffOf(unaliased, aliased).projections_changed);
    EXPECT_FALSE(diffOf(aliased, aliased).projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, NestedQueryShapeIsSignificant)
{
    /// A projection expression can contain a nested query. The clause roles of an `ASTSelectQuery`
    /// live in its `positions` map and the set operation of a union lives in its modes, neither of
    /// which is a child, so those shapes have to be hashed explicitly.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields nested_where;
    nested_where.projections = "pr (SELECT b WHERE b IN (SELECT number FROM numbers(10) WHERE number > 1) ORDER BY c)";
    MetadataFields nested_having;
    nested_having.projections = "pr (SELECT b WHERE b IN (SELECT number FROM numbers(10) HAVING number > 1) ORDER BY c)";
    EXPECT_TRUE(diffOf(nested_where, nested_having).projections_changed);
    EXPECT_FALSE(diffOf(nested_where, nested_where).projections_changed);

    MetadataFields union_all;
    union_all.projections = "pr (SELECT b WHERE b IN (SELECT 1 UNION ALL SELECT 2) ORDER BY c)";
    MetadataFields union_distinct;
    union_distinct.projections = "pr (SELECT b WHERE b IN (SELECT 1 UNION DISTINCT SELECT 2) ORDER BY c)";
    EXPECT_TRUE(diffOf(union_all, union_distinct).projections_changed);

    MetadataFields except_query;
    except_query.projections = "pr (SELECT b WHERE b IN (SELECT 1 EXCEPT SELECT 2) ORDER BY c)";
    MetadataFields intersect_query;
    intersect_query.projections = "pr (SELECT b WHERE b IN (SELECT 1 INTERSECT SELECT 2) ORDER BY c)";
    EXPECT_TRUE(diffOf(except_query, intersect_query).projections_changed);

    /// Redundant parentheses inside the nested query still compare equal.
    MetadataFields nested_where_parens;
    nested_where_parens.projections = "pr (SELECT b WHERE b IN (SELECT number FROM numbers(10) WHERE (number) > 1) ORDER BY (c))";
    EXPECT_FALSE(diffOf(nested_where, nested_where_parens).projections_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, OrderByElementChildRolesAreSignificant)
{
    /// The roles of the children live in `positions`, not in the children themselves.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields fill_to;
    fill_to.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM 1 TO 2)";
    MetadataFields fill_step;
    fill_step.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM 1 STEP 2)";
    EXPECT_TRUE(diffOf(fill_to, fill_step).constraints_changed);
    EXPECT_FALSE(diffOf(fill_to, fill_to).constraints_changed);

    /// Redundant parentheses around the bounds still compare equal.
    MetadataFields fill_to_parens;
    fill_to_parens.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM (1) TO (2))";
    EXPECT_FALSE(diffOf(fill_to, fill_to_parens).constraints_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, ExplicitDefaultNullsOrderIsNotSignificant)
{
    /// Only the effective nulls direction is significant, not whether `NULLS` was written.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    MetadataFields ascending;
    ascending.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number ASC)";
    MetadataFields ascending_nulls_last;
    ascending_nulls_last.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number ASC NULLS LAST)";
    EXPECT_FALSE(diffOf(ascending, ascending_nulls_last).constraints_changed);

    MetadataFields descending;
    descending.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number DESC)";
    MetadataFields descending_nulls_last;
    descending_nulls_last.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number DESC NULLS LAST)";
    EXPECT_FALSE(diffOf(descending, descending_nulls_last).constraints_changed);

    /// A non-default modifier still moves the NULLs, so it stays significant.
    MetadataFields ascending_nulls_first;
    ascending_nulls_first.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number ASC NULLS FIRST)";
    EXPECT_TRUE(diffOf(ascending, ascending_nulls_first).constraints_changed);

    MetadataFields descending_nulls_first;
    descending_nulls_first.constraints = "cc CHECK a IN (SELECT number FROM numbers(3) ORDER BY number DESC NULLS FIRST)";
    EXPECT_TRUE(diffOf(descending, descending_nulls_first).constraints_changed);

    /// These two share `nulls_direction` and differ only in `direction`.
    EXPECT_TRUE(diffOf(ascending_nulls_first, descending).constraints_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, CommonTableExpressionIdentityIsSignificant)
{
    /// The CTE name, `MATERIALIZED` and the column aliases are not children.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// Same `FROM x`, same subqueries in the same order: only the binding differs.
    MetadataFields x_is_one;
    x_is_one.constraints = "cc CHECK a IN (WITH x AS (SELECT 1 AS q), y AS (SELECT 2 AS q) SELECT q FROM x)";
    MetadataFields x_is_two;
    x_is_two.constraints = "cc CHECK a IN (WITH y AS (SELECT 1 AS q), x AS (SELECT 2 AS q) SELECT q FROM x)";
    EXPECT_TRUE(diffOf(x_is_one, x_is_two).constraints_changed);
    EXPECT_FALSE(diffOf(x_is_one, x_is_one).constraints_changed);

    /// `MATERIALIZED` and the column aliases live outside the children as well.
    MetadataFields plain;
    plain.constraints = "cc CHECK a IN (WITH x AS (SELECT 1 AS q) SELECT q FROM x)";
    MetadataFields materialized;
    materialized.constraints = "cc CHECK a IN (WITH x AS MATERIALIZED (SELECT 1 AS q) SELECT q FROM x)";
    EXPECT_TRUE(diffOf(plain, materialized).constraints_changed);

    MetadataFields aliased;
    aliased.constraints = "cc CHECK a IN (WITH x (r) AS (SELECT 1 AS q) SELECT r FROM x)";
    EXPECT_TRUE(diffOf(plain, aliased).constraints_changed);
}

TEST(ReplicatedMergeTreeTableMetadataCompare, WindowNameIsSignificant)
{
    /// The window name is not a child.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// Same `OVER w`, same frames in the same order: only the binding differs, so `OVER w` sums
    /// ascending in the first and descending in the second.
    MetadataFields w_is_ascending;
    w_is_ascending.constraints
        = "cc CHECK a IN (SELECT sum(number) OVER w FROM numbers(4) WINDOW w AS (ORDER BY number), v AS (ORDER BY number DESC))";
    MetadataFields w_is_descending;
    w_is_descending.constraints
        = "cc CHECK a IN (SELECT sum(number) OVER w FROM numbers(4) WINDOW v AS (ORDER BY number), w AS (ORDER BY number DESC))";
    EXPECT_TRUE(diffOf(w_is_ascending, w_is_descending).constraints_changed);
    EXPECT_FALSE(diffOf(w_is_ascending, w_is_ascending).constraints_changed);
}

