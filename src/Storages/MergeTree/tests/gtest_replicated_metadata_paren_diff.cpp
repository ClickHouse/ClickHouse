#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/VirtualColumnsDescription.h>

using namespace DB;

/// Regression coverage for `ReplicatedMergeTreeTableMetadata::checkAndFindDiff` on metadata
/// stored by a version affected by #92340: such a version kept the redundant parentheses the
/// user wrote (`ORDER BY (b)` -> `sorting key: (b)`), while the canonical form has none.
/// The diff must compare the fields in the backward-compatible canonical form, otherwise a
/// replica that joined old parenthesized Keeper metadata reports the field as changed on every
/// unrelated replicated `ALTER` and re-imports the noncanonical AST into the local metadata.

namespace
{

ColumnsDescription makeColumns()
{
    return ColumnsDescription{NamesAndTypesList{
        {"a", std::make_shared<DataTypeUInt32>()},
        {"b", std::make_shared<DataTypeUInt32>()},
        {"c", std::make_shared<DataTypeUInt32>()},
        {"d", std::make_shared<DataTypeDateTime>()},
    }};
}

String makeMetadataString(const String & sampling, const String & sorting_key, const String & ttl, const String & constraints)
{
    return "metadata format version: 1\n"
           "date column: \n"
           "sampling expression: " + sampling + "\n"
           "index granularity: 8192\n"
           "mode: 0\n"
           "sign column: \n"
           "primary key: b\n"
           "data format version: 1\n"
           "partition key: a\n"
           "sorting key: " + sorting_key + "\n"
           "ttl: " + ttl + "\n"
           "constraints: " + constraints + "\n"
           "merge parameters format version: 2\n";
}

ReplicatedMergeTreeTableMetadata parseMetadata(const String & s, const ColumnsDescription & columns, ContextPtr context)
{
    return ReplicatedMergeTreeTableMetadata::parseAndNormalize(
        s, columns, /* add_minmax_index_for_numeric_columns = */ false, /* add_minmax_index_for_string_columns = */ false, context);
}

}

TEST(ReplicatedMergeTreeTableMetadataDiff, ParenthesizedFieldsCompareEqual)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto context = getContext().context;
    auto columns = makeColumns();
    VirtualColumnsDescription virtuals;

    /// The local metadata is canonical (built by the current version), the Keeper metadata kept
    /// the redundant parentheses that #92340 preserved. The definitions are equal.
    auto local = parseMetadata(
        makeMetadataString("b", "b, c", "d + toIntervalYear(10)", "cc CHECK a > 0"), columns, context);
    auto from_zk = parseMetadata(
        makeMetadataString("(b)", "(b), (c)", "(d + toIntervalYear(10))", "cc CHECK (a > 0)"), columns, context);

    auto diff = local.checkAndFindDiff(from_zk, columns, virtuals, "test_table", context);
    EXPECT_FALSE(diff.sorting_key_changed);
    EXPECT_FALSE(diff.sampling_expression_changed);
    EXPECT_FALSE(diff.ttl_table_changed);
    EXPECT_FALSE(diff.constraints_changed);
    EXPECT_TRUE(diff.empty());
}

TEST(ReplicatedMergeTreeTableMetadataDiff, GenuineChangesAreStillDetectedCanonicalized)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto context = getContext().context;
    auto columns = makeColumns();
    VirtualColumnsDescription virtuals;

    auto local = parseMetadata(
        makeMetadataString("b", "b, c", "d + toIntervalYear(10)", "cc CHECK a > 0"), columns, context);
    /// Genuinely different definitions, stored with the redundant parentheses: the change must
    /// still be detected, and the new value recorded in the diff must be the canonical form so
    /// the parenthesized text does not leak into the local metadata.
    auto from_zk = parseMetadata(
        makeMetadataString("(b)", "(b), (d)", "(d + toIntervalYear(20))", "cc CHECK (a > 1)"), columns, context);

    auto diff = local.checkAndFindDiff(from_zk, columns, virtuals, "test_table", context);
    EXPECT_TRUE(diff.sorting_key_changed);
    EXPECT_EQ(diff.new_sorting_key, "b, d");
    EXPECT_FALSE(diff.sampling_expression_changed);
    EXPECT_TRUE(diff.ttl_table_changed);
    EXPECT_EQ(diff.new_ttl_table, "d + toIntervalYear(20)");
    EXPECT_TRUE(diff.constraints_changed);
    EXPECT_EQ(diff.new_constraints, "cc CHECK a > 1");
}
