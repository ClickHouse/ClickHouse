#pragma once

#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Core/Block.h>

#include <deque>
#include <optional>
#include <set>
#include <vector>

namespace DB
{

struct ColumnForPatch
{
    enum class Order
    {
        /// Apply patch before converting the column to actual type.
        BeforeConversions,
        /// Apply patch after converting the column to actual type.
        AfterConversions,
        /// Apply patch after evaluating missing defaults for the column.
        AfterEvaluatingDefaults,
    };

    ColumnForPatch(const String & column_name_, Order order_) : column_name(column_name_), order(order_) {}

    String column_name;
    Order order;
};

using ColumnsForPatch = std::vector<ColumnForPatch>;
using ColumnsForPatches = std::vector<ColumnsForPatch>;

/// Determines which columns from `header` each patch reader updates,
/// and at which stage (BeforeConversions / AfterConversions / AfterEvaluatingDefaults).
ColumnsForPatches getColumnsForPatches(
    const Block & header,
    const Columns & columns,
    const MergeTreePatchReaders & patch_readers);

/// Copies patch system columns (_part, _part_offset, _block_number, _block_offset, _data_version)
/// from `from` to `to` if they are missing in `to`.
void addPatchVirtualColumns(Block & to, const Block & from);

/// Applies patches from `patch_readers` to `result_columns` for columns matching
/// `suitable_orders` within the [min_version, max_version) version window.
void applyPatchesToColumns(
    const MergeTreePatchReaders & patch_readers,
    const std::vector<std::deque<PatchReadResultPtr>> & patches_results,
    const Block & result_header,
    Columns & result_columns,
    Block & versions_block,
    std::optional<UInt64> min_version,
    std::optional<UInt64> max_version,
    const ColumnsForPatches & columns_for_patches,
    const std::set<ColumnForPatch::Order> & suitable_orders,
    const Block & additional_columns);

}
