#include <gtest/gtest.h>

#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;

/// Regression test for a SIGSEGV that occurred in production when
/// applyPatchesToBlock was called with a patch part whose column type
/// had diverged from the result block type due to schema evolution.
///
/// Without the type->equals() guard the patch was applied blindly,
/// causing insertFrom to reinterpret ColumnString memory as
/// ColumnVector (or vice-versa) → SIGSEGV at a garbage pointer.
///
/// With the guard the mismatched patch is skipped and the result
/// block retains its original data for that column.
TEST(ApplyPatches, TypeMismatchSkipsPatch)
{
    /// ---------- result block (current schema: String column) ----------
    auto result_col = ColumnString::create();
    result_col->insert("aaa");
    result_col->insert("bbb");
    result_col->insert("ccc");

    auto version_col = ColumnUInt64::create();
    version_col->insert(1u);
    version_col->insert(1u);
    version_col->insert(1u);

    Block result_block;
    result_block.insert({result_col->getPtr(), std::make_shared<DataTypeString>(), "value"});
    result_block.insert({version_col->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    /// ---------- patch block (old schema: UInt64 column) ----------
    auto patch_value_col = ColumnUInt64::create();
    patch_value_col->insert(42u);

    auto patch_version_col = ColumnUInt64::create();
    patch_version_col->insert(2u);     /// higher version → patch wins if applied

    Block patch_block;
    patch_block.insert({patch_value_col->getPtr(), std::make_shared<DataTypeUInt64>(), "value"});
    patch_block.insert({patch_version_col->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    /// ---------- build PatchToApply ----------
    auto patch = std::make_shared<PatchToApply>();
    patch->patch_blocks.push_back(std::move(patch_block));
    patch->result_row_indices.push_back(1);  /// update row 1
    patch->patch_row_indices.push_back(0);   /// from patch row 0

    PatchesToApply patches{std::move(patch)};
    Block versions_block;

    /// ---------- apply ----------
    applyPatchesToBlock(result_block, versions_block, patches, {"value"}, /*source_data_version=*/ 1);

    /// ---------- verify ----------
    /// The patch has UInt64 type while the result has String type.
    /// The guard must skip the patch, so row 1 keeps its original value.
    const auto & col = result_block.getByName("value").column;
    ASSERT_EQ(col->size(), 3u);
    EXPECT_EQ((*col)[0].safeGet<String>(), "aaa");
    EXPECT_EQ((*col)[1].safeGet<String>(), "bbb");   /// NOT "42" or garbled
    EXPECT_EQ((*col)[2].safeGet<String>(), "ccc");
}

/// Sanity check: when types match the patch IS applied normally.
TEST(ApplyPatches, SameTypeAppliesPatch)
{
    auto result_col = ColumnUInt64::create();
    result_col->insert(100u);
    result_col->insert(200u);
    result_col->insert(300u);

    auto version_col = ColumnUInt64::create();
    version_col->insert(1u);
    version_col->insert(1u);
    version_col->insert(1u);

    Block result_block;
    result_block.insert({result_col->getPtr(), std::make_shared<DataTypeUInt64>(), "value"});
    result_block.insert({version_col->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    auto patch_value_col = ColumnUInt64::create();
    patch_value_col->insert(999u);

    auto patch_version_col = ColumnUInt64::create();
    patch_version_col->insert(2u);

    Block patch_block;
    patch_block.insert({patch_value_col->getPtr(), std::make_shared<DataTypeUInt64>(), "value"});
    patch_block.insert({patch_version_col->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    auto patch = std::make_shared<PatchToApply>();
    patch->patch_blocks.push_back(std::move(patch_block));
    patch->result_row_indices.push_back(1);
    patch->patch_row_indices.push_back(0);

    PatchesToApply patches{std::move(patch)};
    Block versions_block;

    applyPatchesToBlock(result_block, versions_block, patches, {"value"}, /*source_data_version=*/ 1);

    const auto & col = result_block.getByName("value").column;
    ASSERT_EQ(col->size(), 3u);
    EXPECT_EQ((*col)[0].safeGet<UInt64>(), 100u);
    EXPECT_EQ((*col)[1].safeGet<UInt64>(), 999u);   /// patch applied
    EXPECT_EQ((*col)[2].safeGet<UInt64>(), 300u);
}

/// When multiple patch sources exist and only some have mismatched types,
/// the compatible sources must still be applied (per-source filtering).
TEST(ApplyPatches, MixedTypeSourcesAppliesCompatibleOnes)
{
    /// Result block: String column with 4 rows.
    auto result_col = ColumnString::create();
    result_col->insert("a");
    result_col->insert("b");
    result_col->insert("c");
    result_col->insert("d");

    auto version_col = ColumnUInt64::create();
    version_col->insert(1u);
    version_col->insert(1u);
    version_col->insert(1u);
    version_col->insert(1u);

    Block result_block;
    result_block.insert({result_col->getPtr(), std::make_shared<DataTypeString>(), "value"});
    result_block.insert({version_col->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    /// Patch 1: INCOMPATIBLE — UInt64 type, wants to update row 1.
    auto p1_value = ColumnUInt64::create();
    p1_value->insert(42u);
    auto p1_version = ColumnUInt64::create();
    p1_version->insert(2u);

    Block p1_block;
    p1_block.insert({p1_value->getPtr(), std::make_shared<DataTypeUInt64>(), "value"});
    p1_block.insert({p1_version->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    auto patch1 = std::make_shared<PatchToApply>();
    patch1->patch_blocks.push_back(std::move(p1_block));
    patch1->result_row_indices.push_back(1);
    patch1->patch_row_indices.push_back(0);

    /// Patch 2: COMPATIBLE — String type, wants to update row 2.
    auto p2_value = ColumnString::create();
    p2_value->insert("updated");
    auto p2_version = ColumnUInt64::create();
    p2_version->insert(3u);

    Block p2_block;
    p2_block.insert({p2_value->getPtr(), std::make_shared<DataTypeString>(), "value"});
    p2_block.insert({p2_version->getPtr(), std::make_shared<DataTypeUInt64>(), PartDataVersionColumn::name});

    auto patch2 = std::make_shared<PatchToApply>();
    patch2->patch_blocks.push_back(std::move(p2_block));
    patch2->result_row_indices.push_back(2);
    patch2->patch_row_indices.push_back(0);

    PatchesToApply patches{std::move(patch1), std::move(patch2)};
    Block versions_block;

    applyPatchesToBlock(result_block, versions_block, patches, {"value"}, /*source_data_version=*/ 1);

    const auto & col = result_block.getByName("value").column;
    ASSERT_EQ(col->size(), 4u);
    EXPECT_EQ((*col)[0].safeGet<String>(), "a");
    EXPECT_EQ((*col)[1].safeGet<String>(), "b");        /// incompatible patch skipped
    EXPECT_EQ((*col)[2].safeGet<String>(), "updated");   /// compatible patch applied
    EXPECT_EQ((*col)[3].safeGet<String>(), "d");
}
