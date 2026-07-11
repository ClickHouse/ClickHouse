#include <Core/CompareHelper.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/KeyDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <absl/container/flat_hash_map.h>
#include <base/types.h>
#include <optional>
#include <shared_mutex>

namespace ProfileEvents
{
    extern const Event ApplyPatchesMicroseconds;
    extern const Event BuildPatchesJoinMicroseconds;
    extern const Event BuildPatchesMergeMicroseconds;
    extern const Event ApplyPatchMergeOnKeyMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(column_name).column).getData();
}

PaddedPODArray<UInt64> & getColumnUInt64Data(Block & block, const String & column_name)
{
    return assert_cast<ColumnUInt64 &>(block.getByName(column_name).column->assumeMutableRef()).getData();
}

bool canApplyPatchInplace(const IColumn & column)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return nullable->getNestedColumn().isFixedAndContiguous();

    return column.isFixedAndContiguous();
}

IColumn::Versions & addDataVersionForColumn(Block & block, const String & column_name, UInt64 num_rows, UInt64 data_version)
{
    String data_version_name = PartDataVersionColumn::name + "_" + column_name;
    if (block.has(data_version_name))
        return getColumnUInt64Data(block, data_version_name);

    ColumnWithTypeAndName column;
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = ColumnUInt64::create(num_rows, data_version);
    column.name = data_version_name;

    block.insert(std::move(column));
    return getColumnUInt64Data(block, data_version_name);
}

/// Builds patch sources for a column from all patch blocks.
/// @p converted_columns_storage keeps cast results alive while the returned sources reference them.
VectorWithMemoryTracking<IColumn::Patch::Source> createPatchSources(
    const Blocks & patch_blocks,
    const ColumnWithTypeAndName & result_column,
    Columns & converted_columns_storage)
{
    VectorWithMemoryTracking<IColumn::Patch::Source> sources;
    sources.reserve(patch_blocks.size());

    for (const auto & patch_block : patch_blocks)
    {
        const auto & patch_col_with_type = patch_block.getByName(result_column.name);
        if (!patch_col_with_type.column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} has null data in patch block", result_column.name);

        const IColumn * source_col = patch_col_with_type.column.get();

        /// Patch column may have a different on-disk type when it predates
        /// an ALTER MODIFY COLUMN that hasn't been materialized yet.
        if (!result_column.column->structureEquals(*source_col))
        {
            converted_columns_storage.push_back(castColumn(patch_col_with_type, result_column.type));
            source_col = converted_columns_storage.back().get();
        }

        IColumn::Patch::Source source =
        {
            .column = *source_col,
            .versions = getColumnUInt64Data(patch_block, PartDataVersionColumn::name),
        };

        sources.push_back(std::move(source));
    }

    return sources;
}

struct CombinedPatchBuilder
{
public:
    explicit CombinedPatchBuilder(const PatchesToApply & patches_) : patches(patches_)
    {
        build();
    }

    /// @p converted_columns_storage keeps cast results alive while the returned Patch references them.
    IColumn::Patch createPatchForColumn(
        const ColumnWithTypeAndName & result_column,
        IColumn::Versions & dst_versions, Columns & converted_columns_storage);

private:
    void build();

    ALWAYS_INLINE UInt64 getResultRowIndex(UInt64 patch_idx, UInt64 row_idx) const
    {
        return patches[patch_idx]->result_row_indices[row_idx];
    }

    ALWAYS_INLINE UInt64 getPatchRowIndex(UInt64 patch_idx, UInt64 row_idx) const
    {
        return patches[patch_idx]->patch_row_indices[row_idx];
    }

    ALWAYS_INLINE UInt64 getPatchBlockIndex(UInt64 patch_idx, UInt64 row_idx) const
    {
        return patches[patch_idx]->getNumSources() == 1 ? 0 : patches[patch_idx]->patch_block_indices[row_idx];
    }

    PatchesToApply patches;
    /// Flattened blocks from all patches.
    Blocks all_patch_blocks;
    /// Index of block in the flattened patch blocks.
    IColumn::Offsets src_block_indices;
    /// Index of row in the patch block.
    IColumn::Offsets src_row_indices;
    /// Index of row in the result block.
    IColumn::Offsets dst_row_indices;

};

void CombinedPatchBuilder::build()
{
    /// A mapping (patch_idx, patch_block_idx) -> flattened_block_idx.
    std::vector<std::vector<size_t>> flattened_block_indices(patches.size());

    /// Each patch may have multiple blocks.
    /// Here we flatten all blocks into one vector.
    for (size_t i = 0; i < patches.size(); ++i)
    {
        size_t num_sources = patches[i]->getNumSources();
        flattened_block_indices[i].resize(num_sources);

        for (size_t j = 0; j < num_sources; ++j)
        {
            flattened_block_indices[i][j] = all_patch_blocks.size();
            all_patch_blocks.push_back(patches[i]->patch_blocks[j]);
        }
    }

    std::vector<UInt64> heap;
    std::vector<UInt64> cursors(patches.size());
    std::vector<const IColumn::Versions *> versions(all_patch_blocks.size());

    for (size_t i = 0; i < patches.size(); ++i)
    {
        if (patches[i]->getNumRows() > 0)
            heap.push_back(i);
    }

    for (size_t i = 0; i < all_patch_blocks.size(); ++i)
        versions[i] = &getColumnUInt64Data(all_patch_blocks[i], PartDataVersionColumn::name);

    enum class RowOp
    {
        Skip,
        Add,
        Update,
    };

    auto get_row_op = [&](UInt64 patch_idx, UInt64 row_idx)
    {
        chassert(src_block_indices.size() == dst_row_indices.size());
        chassert(src_row_indices.size() == dst_row_indices.size());

        if (dst_row_indices.empty())
            return RowOp::Add;

        UInt64 last_result_row = dst_row_indices.back();
        UInt64 current_result_row = getResultRowIndex(patch_idx, row_idx);

        /// Patches must be sorted by row index in the result block.
        chassert(current_result_row >= last_result_row);

        /// We found a new updated row in the result block.
        if (current_result_row != last_result_row)
            return RowOp::Add;

        /// The updated row in result block is the same.
        /// Keep the row with the highest version in patch.

        UInt64 last_flattened_block = src_block_indices.back();
        UInt64 last_patch_row = src_row_indices.back();

        UInt64 current_patch_block = getPatchBlockIndex(patch_idx, row_idx);
        UInt64 current_patch_row = getPatchRowIndex(patch_idx, row_idx);
        UInt64 current_flattened_block = flattened_block_indices[patch_idx][current_patch_block];

        UInt64 last_version = (*versions[last_flattened_block])[last_patch_row];
        UInt64 current_version = (*versions[current_flattened_block])[current_patch_row];

        return current_version > last_version ? RowOp::Update : RowOp::Skip;
    };

    auto greater = [&](UInt64 lhs, UInt64 rhs)
    {
        return getResultRowIndex(lhs, cursors[lhs]) > getResultRowIndex(rhs, cursors[rhs]);
    };

    std::make_heap(heap.begin(), heap.end(), greater);

    /// Here we merge all patches into one patch.
    /// We use a simple merging sorted algorithm with heap,
    /// using the fact that patches are sorted by row index in the result block.

    while (!heap.empty())
    {
        UInt64 patch_idx = heap.front();
        UInt64 row_idx = cursors[patch_idx];

        std::pop_heap(heap.begin(), heap.end(), greater);
        heap.pop_back();

        auto row_op = get_row_op(patch_idx, row_idx);

        if (row_op != RowOp::Skip)
        {
            UInt64 patch_block_idx = getPatchBlockIndex(patch_idx, row_idx);
            UInt64 patch_row_idx = getPatchRowIndex(patch_idx, row_idx);
            UInt64 result_row_idx = getResultRowIndex(patch_idx, row_idx);
            UInt64 flattened_block_idx = flattened_block_indices[patch_idx][patch_block_idx];

            if (row_op == RowOp::Update)
            {
                src_block_indices.back() = flattened_block_idx;
                src_row_indices.back() = patch_row_idx;
                dst_row_indices.back() = result_row_idx;
            }
            else
            {
                src_block_indices.push_back(flattened_block_idx);
                src_row_indices.push_back(patch_row_idx);
                dst_row_indices.push_back(result_row_idx);
            }
        }

        ++cursors[patch_idx];
        if (cursors[patch_idx] < patches[patch_idx]->getNumRows())
        {
            heap.push_back(patch_idx);
            std::push_heap(heap.begin(), heap.end(), greater);
        }
    }
}

IColumn::Patch CombinedPatchBuilder::createPatchForColumn(
    const ColumnWithTypeAndName & result_column,
    IColumn::Versions & dst_versions, Columns & converted_columns_storage)
{
    return IColumn::Patch
    {
        .sources = createPatchSources(all_patch_blocks, result_column, converted_columns_storage),
        .src_col_indices = &src_block_indices,
        .src_row_indices = src_row_indices,
        .dst_row_indices = dst_row_indices,
        .dst_versions = dst_versions,
    };
}

/// Builds a patch for a column directly from an already combined PatchToApply
/// (row indices are version-resolved and result rows are unique and ascending).
IColumn::Patch createPatchForColumnFromCombined(
    const PatchToApply & patch_to_apply,
    const ColumnWithTypeAndName & result_column,
    IColumn::Versions & dst_versions,
    Columns & converted_columns_storage)
{
    return IColumn::Patch
    {
        .sources = createPatchSources(patch_to_apply.patch_blocks, result_column, converted_columns_storage),
        .src_col_indices = patch_to_apply.getNumSources() > 1 ? &patch_to_apply.patch_block_indices : nullptr,
        .src_row_indices = patch_to_apply.patch_row_indices,
        .dst_row_indices = patch_to_apply.result_row_indices,
        .dst_versions = dst_versions,
    };
}

Block getUpdatedHeader(const PatchesToApply & patches, const NameSet & updated_columns)
{
    Blocks headers;

    for (const auto & patch : patches)
    {
        if (patch->patch_blocks.empty())
            continue;

        /// All blocks in one patch must have the same structure.
        for (size_t i = 1; i < patch->patch_blocks.size(); ++i)
            assertCompatibleHeader(patch->patch_blocks[i], patch->patch_blocks[0], "patch parts");

        Block header = patch->patch_blocks[0].cloneEmpty();

        for (const auto & column : patch->patch_blocks[0])
        {
            /// Ignore columns that are not updated or have no data.
            if (!updated_columns.contains(column.name) || !column.column)
                header.erase(column.name);
        }

        /// Sort columns by name so that assertCompatibleHeader below compares
        /// matching columns at the same positions. Patch blocks may arrive with
        /// different column orderings because addPatchPartsColumns collects names
        /// from a NameSet (unordered_set) whose iteration order is non-deterministic.
        /// Downstream consumers use name-based lookups, so order does not matter
        /// for correctness — only for this positional compatibility check.
        headers.push_back(header.sortColumns());
    }

    if (headers.empty())
        return {};

    /// Schema evolution may cause type mismatches across patch headers.
    /// Skip assertion in that case — castColumn in apply handles conversion.
    for (size_t i = 1; i < headers.size(); ++i)
        if (!isCompatibleHeader(headers[i], headers[0]))
            return headers.front();

    for (size_t i = 1; i < headers.size(); ++i)
        assertCompatibleHeader(headers[i], headers[0], "patch parts");

    return headers.front();
}

bool canApplyPatchesRaw(const PatchesToApply & patches)
{
    for (const auto & patch : patches)
    {
        if (patch->getNumSources() != 1)
        {
            return false;
        }

        if (patches.size() > 1)
        {
            for (const auto & column : patch->patch_blocks.front())
            {
                if (!isPatchPartSystemColumn(column.name) && column.column && !canApplyPatchInplace(*column.column))
                    return false;
            }
        }
    }

    return true;
}

void applyPatchesToBlockRaw(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Block & updated_header,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    for (auto & result_column : result_block)
    {
        if (!updated_header.has(result_column.name))
            continue;

        auto & result_versions = addDataVersionForColumn(versions_block, result_column.name, result_block.rows(), source_data_version);
        result_column.column = removeSpecialRepresentations(result_column.column);

        for (const auto & patch_to_apply : patches)
        {
            chassert(patch_to_apply->patch_blocks.size() == 1);
            const auto & patch_block = patch_to_apply->patch_blocks.front();

            if (!patch_block.has(result_column.name))
                continue;

            auto patch_col_with_type = patch_block.getByName(result_column.name);
            if (!patch_col_with_type.column)
                continue;

            /// Patch column may have a different on-disk type when it predates
            /// an ALTER MODIFY COLUMN that hasn't been materialized yet.
            ColumnPtr converted_col;
            if (!result_column.column->structureEquals(*patch_col_with_type.column))
                converted_col = castColumn(patch_col_with_type, result_column.type);

            IColumn::Patch::Source source =
            {
                .column = converted_col ? *converted_col : *patch_col_with_type.column,
                .versions = getColumnUInt64Data(patch_block, PartDataVersionColumn::name),
            };

            IColumn::Patch patch =
            {
                .sources = {std::move(source)},
                .src_col_indices = nullptr,
                .src_row_indices = patch_to_apply->patch_row_indices,
                .dst_row_indices = patch_to_apply->result_row_indices,
                .dst_versions = result_versions,
            };

            if (canApplyPatchInplace(*result_column.column))
            {
                /// COW-safe in-place update: clone when the column is shared instead of mutating
                /// a column still referenced by another owner via `assumeMutableRef`.
                auto mutable_column = IColumn::mutate(std::move(result_column.column));
                mutable_column->updateInplaceFrom(patch);
                result_column.column = std::move(mutable_column);
            }
            else
                result_column.column = result_column.column->updateFrom(patch);
        }
    }
}

void applyPatchesToBlockCombined(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Block & updated_header,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    /// A single PatchToApply is already combined (e.g. built by applyPatchesMergeOnKey):
    /// its row indices can be applied directly, without rebuilding them in CombinedPatchBuilder.
    std::optional<CombinedPatchBuilder> builder;
    if (patches.size() > 1)
        builder.emplace(patches);

    for (auto & result_column : result_block)
    {
        if (!updated_header.has(result_column.name))
            continue;

        auto & result_versions = addDataVersionForColumn(versions_block, result_column.name, result_block.rows(), source_data_version);
        result_column.column = removeSpecialRepresentations(result_column.column);

        /// Local storage so cast results are released after each column update.
        Columns converted_columns;
        auto multi_patch = builder
            ? builder->createPatchForColumn(result_column, result_versions, converted_columns)
            : createPatchForColumnFromCombined(*patches.front(), result_column, result_versions, converted_columns);

        if (canApplyPatchInplace(*result_column.column))
        {
            /// COW-safe in-place update: clone when the column is shared instead of mutating
            /// a column still referenced by another owner via `assumeMutableRef`.
            auto mutable_column = IColumn::mutate(std::move(result_column.column));
            mutable_column->updateInplaceFrom(multi_patch);
            result_column.column = std::move(mutable_column);
        }
        else
            result_column.column = result_column.column->updateFrom(multi_patch);
    }
}

}

PatchToApplyPtr applyPatchMerge(const Block & result_block, const Block & patch_block, const PatchPartInfoForReader & patch)
{
    if (patch.source_parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Applying patch parts with mode {} requires only one part, got: {}", patch.mode, patch.source_parts.size());

    auto patch_to_apply = std::make_shared<PatchToApply>();

    size_t num_rows = result_block.rows();
    size_t patch_rows = patch_block.rows();

    if (num_rows == 0 || patch_rows == 0)
        return patch_to_apply;

    patch_to_apply->patch_blocks.emplace_back(patch_block);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesMergeMicroseconds);

    const auto & patch_name_column = assert_cast<const ColumnLowCardinality &>(*patch_block.getByName("_part").column);
    const auto & patch_offset_data = getColumnUInt64Data(patch_block, "_part_offset");
    const auto & result_offset_data = getColumnUInt64Data(result_block, "_part_offset");

    UInt64 first_result_offset = result_offset_data[0];
    UInt64 last_result_offset = result_offset_data[num_rows - 1];

    auto [patch_begin, patch_end] = getPartNameOffsetRange(
        patch_name_column,
        patch_offset_data,
        patch.source_parts.front(),
        first_result_offset,
        last_result_offset);

    size_t size_to_reserve = std::min(static_cast<size_t>(patch_end - patch_begin), num_rows);

    patch_to_apply->result_row_indices.reserve(size_to_reserve);
    patch_to_apply->patch_row_indices.reserve(size_to_reserve);

    /// Optimize in case when _part_offset has all consecutive rows.
    if (last_result_offset - first_result_offset + 1 == num_rows)
    {
        for (size_t patch_row = patch_begin; patch_row < patch_end; ++patch_row)
        {
            chassert(patch_offset_data[patch_row] >= first_result_offset);
            size_t result_row = patch_offset_data[patch_row] - first_result_offset;

            patch_to_apply->patch_row_indices.push_back(patch_row);
            patch_to_apply->result_row_indices.push_back(result_row);
        }
    }
    else
    {
        /// It may be in case when _part_offset is filtered.
        /// TODO: apply filter to indices in MergeTreeReadersChain.
        size_t result_it = 0;
        size_t result_end = num_rows;
        size_t patch_it = patch_begin;

        while (patch_it < patch_end && result_it < result_end)
        {
            if (patch_offset_data[patch_it] > result_offset_data[result_it])
            {
                ++result_it;
            }
            else if (patch_offset_data[patch_it] < result_offset_data[result_it])
            {
                ++patch_it;
            }
            else
            {
                patch_to_apply->patch_row_indices.push_back(patch_it++);
                patch_to_apply->result_row_indices.push_back(result_it++);
            }
        }
    }

    return patch_to_apply;
}

namespace
{

ColumnRawPtrs extractSortingKeyColumns(const Block & block, const Names & sorting_key_column_names)
{
    ColumnRawPtrs out;
    out.reserve(sorting_key_column_names.size());

    for (const auto & name : sorting_key_column_names)
        out.push_back(block.getByName(name).column.get());

    return out;
}

/// Compares sort-key tuples at two (block, row) positions, honouring DESC flags.
/// Returns <0, =0, or >0 using the same convention as `IColumn::compareAt`.
ALWAYS_INLINE int compareSortKeyRows(
    const ColumnRawPtrs & lhs_columns,
    size_t lhs_row,
    const ColumnRawPtrs & rhs_columns,
    size_t rhs_row,
    const std::vector<bool> & reverse_flags)
{
    const size_t n = lhs_columns.size();
    chassert(n == rhs_columns.size());

    if (reverse_flags.empty())
    {
        for (size_t i = 0; i < n; ++i)
        {
            int cmp = lhs_columns[i]->compareAt(lhs_row, rhs_row, *rhs_columns[i], /*nan_direction_hint=*/ 1);
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }
    else
    {
        chassert(n == reverse_flags.size());
        for (size_t i = 0; i < n; ++i)
        {
            int cmp = lhs_columns[i]->compareAt(lhs_row, rhs_row, *rhs_columns[i], /*nan_direction_hint=*/ 1);
            if (cmp != 0)
                return reverse_flags[i] ? -cmp : cmp;
        }
    }

    return 0;
}

/// Galloping (exponential) partition-point search: returns the smallest `i` in `[begin, end)`
/// such that `compareSortKeyRows(search_key[i], pivot_key[pivot_row])` is `< 0` when
/// `is_lower_bound == true` (lower bound), or `>= 0` when `is_lower_bound == false` (upper bound).
/// When one side of the merge is much smaller, this collapses its complexity from `O(m + p)` to
/// `O(min * log(max / min))` comparisons; with `gap = 1` it costs only 1-2 extra comparisons per step.
template <bool is_lower_bound>
ALWAYS_INLINE size_t gallopingBinarySearch(
    const ColumnRawPtrs & search_key,
    size_t begin,
    size_t end,
    const ColumnRawPtrs & pivot_key,
    size_t pivot_row,
    const std::vector<bool> & reverse_flags)
{
    auto compare = [&](size_t i)
    {
        int res = compareSortKeyRows(search_key, i, pivot_key, pivot_row, reverse_flags);
        if constexpr (is_lower_bound)
            return res < 0;
        else
            return res <= 0;
    };

    static constexpr size_t max_step = 1ULL << 32;

    size_t prev = 0;
    size_t step = 1;

    while (begin + step <= end && compare(begin + step - 1))
    {
        prev = step;
        step = step < max_step ? step << 1 : max_step;
    }

    size_t lo = begin + prev;
    size_t hi = std::min(end, begin + step);

    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2;
        if (compare(mid))
            lo = mid + 1;
        else
            hi = mid;
    }

    return lo;
}

/// Pack `(block_number, block_offset)` into a `UInt128`: `block_offset` in the low 64 bits,
/// `block_number` in the high 64 bits. `UInt128TrivialHash` takes the low limb as the hash,
/// so putting the per-row-unique `block_offset` there keeps buckets well spread.
ALWAYS_INLINE UInt128 makeBlockIdentity(UInt64 block_number, UInt64 block_offset)
{
    return (UInt128(block_number) << 64) | UInt128(block_offset);
}

/// Cursor over one patch block in the merge of applyPatchesMergeOnKey.
struct PatchBlockCursor
{
    ColumnRawPtrs sorting_key_columns;
    const PaddedPODArray<UInt64> * block_number = nullptr;
    const PaddedPODArray<UInt64> * block_offset = nullptr;
    const PaddedPODArray<UInt64> * versions = nullptr;
    /// Group of patch blocks with the same set of updated columns.
    size_t group_idx = 0;
    /// Index of the block in the group's patch_blocks.
    UInt32 block_idx_in_group = 0;
    size_t row = 0;
    /// End of the current equal-sort-key run, valid while the cursor is extracted from the heap.
    size_t run_end = 0;
    size_t num_rows = 0;
};

/// Patch blocks with the same set of updated columns, combined into one patch.
struct PatchColumnsGroup
{
    Names updated_columns;
    std::shared_ptr<PatchToApply> patch;
};

/// An entry of the per-run hash map in applyPatchesMergeOnKey. For each row identity the map
/// keeps one entry per group - the matched patch row with the highest data version in that group.
struct RunEntry
{
    static constexpr UInt32 EMPTY_BLOCK = std::numeric_limits<UInt32>::max();

    UInt32 block_idx = EMPTY_BLOCK;
    UInt32 row_idx = 0;
    UInt64 version = 0;
};

}

std::vector<std::pair<Names, PatchToApplyPtr>> applyPatchesMergeOnKey(
    const Block & result_block,
    const std::vector<PatchBlockForMergeOnKey> & patch_blocks,
    const KeyDescription & sorting_key)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ApplyPatchMergeOnKeyMicroseconds);

    size_t main_rows = result_block.rows();
    if (main_rows == 0 || patch_blocks.empty())
        return {};

    Block result_block_copy;
    for (const auto & name : sorting_key.column_names)
        result_block_copy.insert(result_block.getByName(name));

    result_block_copy.insert(result_block.getByName(BlockNumberColumn::name));
    result_block_copy.insert(result_block.getByName(BlockOffsetColumn::name));

    for (auto & column : result_block_copy)
        column.column = removeSpecialRepresentations(column.column);

    const auto & reverse_flags = sorting_key.reverse_flags;
    const auto & result_block_number = getColumnUInt64Data(result_block_copy, BlockNumberColumn::name);
    const auto & result_block_offset = getColumnUInt64Data(result_block_copy, BlockOffsetColumn::name);
    const auto result_sorting_key = extractSortingKeyColumns(result_block_copy, sorting_key.column_names);

    std::vector<PatchColumnsGroup> groups;
    absl::flat_hash_map<UInt128, size_t, UInt128TrivialHash> group_by_hash;

    /// Sorting key columns (and their physical sources) are stored in patch parts only to
    /// identify updated rows and are never updated themselves. Exclude them from the updated
    /// columns of the emitted patches.
    NameSet sorting_key_columns(sorting_key.column_names.begin(), sorting_key.column_names.end());
    if (sorting_key.expression)
        for (const auto & name : sorting_key.expression->getRequiredColumns())
            sorting_key_columns.insert(name);

    /// Keeps columns referenced by cursors alive.
    std::vector<Block> block_copies;
    std::vector<PatchBlockCursor> cursors;

    block_copies.reserve(patch_blocks.size());
    cursors.reserve(patch_blocks.size());

    for (const auto & patch_block : patch_blocks)
    {
        size_t patch_rows = patch_block.block->rows();
        if (patch_rows == 0)
            continue;

        Block block_copy = *patch_block.block;
        for (auto & column : block_copy)
            column.column = removeSpecialRepresentations(column.column);

        /// Group patch blocks by the hash of names and types of updated columns present in the
        /// block. Types are included because a column may have different types in patch parts
        /// created before and after an ALTER MODIFY COLUMN that is not materialized yet, and
        /// blocks of one PatchToApply must have the same structure.
        SipHash hash;
        Names block_updated_columns;

        for (const auto & name : *patch_block.updated_columns)
        {
            if (sorting_key_columns.contains(name))
                continue;

            const auto * column = block_copy.findByName(name);
            if (column && column->column)
            {
                hash.update(name);
                hash.update(column->type->getName());
                block_updated_columns.push_back(name);
            }
        }

        if (block_updated_columns.empty())
            continue;

        auto [group_it, group_inserted] = group_by_hash.try_emplace(hash.get128(), groups.size());
        if (group_inserted)
        {
            auto & group = groups.emplace_back();
            group.updated_columns = block_updated_columns;
            group.patch = std::make_shared<PatchToApply>();
        }

        auto & group = groups[group_it->second];

        /// Keep only the updated columns and the data version column in the emitted block.
        Block emitted_block;
        for (const auto & name : block_updated_columns)
            emitted_block.insert(block_copy.getByName(name));
        emitted_block.insert(block_copy.getByName(PartDataVersionColumn::name));

        auto & cursor = cursors.emplace_back();
        cursor.sorting_key_columns = extractSortingKeyColumns(block_copy, sorting_key.column_names);
        cursor.block_number = &getColumnUInt64Data(block_copy, BlockNumberColumn::name);
        cursor.block_offset = &getColumnUInt64Data(block_copy, BlockOffsetColumn::name);
        cursor.versions = &getColumnUInt64Data(block_copy, PartDataVersionColumn::name);
        cursor.group_idx = group_it->second;
        cursor.block_idx_in_group = static_cast<UInt32>(group.patch->patch_blocks.size());
        cursor.num_rows = patch_rows;
        cursor.row = gallopingBinarySearch<true>(cursor.sorting_key_columns, 0, patch_rows, result_sorting_key, 0, reverse_flags);

        group.patch->patch_blocks.push_back(std::move(emitted_block));
        block_copies.push_back(std::move(block_copy));
    }

    size_t num_groups = groups.size();
    size_t main_idx = 0;

    /// Scratch structures for one equal-sort-key run, reused across runs.
    absl::flat_hash_map<UInt128, UInt32, UInt128TrivialHash> run_map;
    std::vector<RunEntry> run_entries;
    std::vector<size_t> equal_cursors;

    /// Processes one run of equal sort keys: matches main rows [main_run_begin, main_run_end)
    /// with rows [cursor.row, cursor.run_end) of cursors in `equal_cursors` by the
    /// (block_number, block_offset) identity and emits matches into the groups' patches.
    auto process_equal_run = [&](size_t main_run_begin, size_t main_run_end, size_t num_patch_rows_in_run)
    {
        if (num_patch_rows_in_run == 1 && main_run_end - main_run_begin == 1)
        {
            /// Common case for unique sort keys: no hash map, just compare identity directly.
            const auto & cursor = cursors[equal_cursors.front()];

            if ((*cursor.block_number)[cursor.row] == result_block_number[main_run_begin] && (*cursor.block_offset)[cursor.row] == result_block_offset[main_run_begin])
            {
                auto & patch = *groups[cursor.group_idx].patch;
                patch.result_row_indices.push_back(main_run_begin);
                patch.patch_block_indices.push_back(cursor.block_idx_in_group);
                patch.patch_row_indices.push_back(cursor.row);
            }

            return;
        }

        run_map.clear();
        run_entries.clear();
        run_map.reserve(num_patch_rows_in_run);

        for (size_t cursor_idx : equal_cursors)
        {
            const auto & cursor = cursors[cursor_idx];

            for (size_t i = cursor.row; i < cursor.run_end; ++i)
            {
                UInt128 identity = makeBlockIdentity((*cursor.block_number)[i], (*cursor.block_offset)[i]);
                auto [it, inserted] = run_map.try_emplace(identity, static_cast<UInt32>(run_entries.size()));

                if (inserted)
                    run_entries.resize(run_entries.size() + num_groups);

                /// Keep the row with the highest data version within each group. Conflicts
                /// across groups are resolved by row versions at application time.
                auto & entry = run_entries[it->second + cursor.group_idx];
                UInt64 version = (*cursor.versions)[i];

                if (entry.block_idx == RunEntry::EMPTY_BLOCK || version > entry.version)
                    entry = {cursor.block_idx_in_group, static_cast<UInt32>(i), version};
            }
        }

        /// Emit matches in the order of main rows, so that result rows are unique and
        /// ascending in each group, as required by updateFrom and updateInplaceFrom.
        for (size_t i = main_run_begin; i < main_run_end; ++i)
        {
            auto it = run_map.find(makeBlockIdentity(result_block_number[i], result_block_offset[i]));
            if (it == run_map.end())
                continue;

            for (size_t group_idx = 0; group_idx < num_groups; ++group_idx)
            {
                const auto & entry = run_entries[it->second + group_idx];
                if (entry.block_idx == RunEntry::EMPTY_BLOCK)
                    continue;

                auto & patch = *groups[group_idx].patch;
                patch.result_row_indices.push_back(i);
                patch.patch_block_indices.push_back(entry.block_idx);
                patch.patch_row_indices.push_back(entry.row_idx);
            }
        }
    };

    /// Patch blocks are typically much smaller than the main stream, so we drive the merge
    /// from the patch side using galloping search into main. This skips over long runs of main
    /// rows below the current patch key in `O(log gap)` comparisons per patch row.
    ///
    /// With few cursors the minimal key is found by a linear scan: it needs no more key
    /// comparisons than heap maintenance and the code is simpler and branch-predictable.
    /// With one cursor it degenerates into a plain two-pointer merge with no overhead.
    /// A heap pays off only with many cursors.
    static constexpr size_t max_cursors_for_linear_scan = 8;

    if (cursors.size() <= max_cursors_for_linear_scan)
    {
        /// Indices of cursors with unprocessed rows.
        std::vector<size_t> live_cursors;
        live_cursors.reserve(cursors.size());

        for (size_t i = 0; i < cursors.size(); ++i)
        {
            if (cursors[i].row < cursors[i].num_rows)
                live_cursors.push_back(i);
        }

        while (main_idx < main_rows && !live_cursors.empty())
        {
            /// Find the cursor with the minimal current key.
            size_t min_pos = 0;
            for (size_t i = 1; i < live_cursors.size(); ++i)
            {
                const auto & cursor = cursors[live_cursors[i]];
                const auto & min_cursor = cursors[live_cursors[min_pos]];

                if (compareSortKeyRows(cursor.sorting_key_columns, cursor.row, min_cursor.sorting_key_columns, min_cursor.row, reverse_flags) < 0)
                    min_pos = i;
            }

            auto & top_cursor = cursors[live_cursors[min_pos]];

            main_idx = gallopingBinarySearch<true>(result_sorting_key, main_idx, main_rows, top_cursor.sorting_key_columns, top_cursor.row, reverse_flags);
            if (main_idx == main_rows)
                break;

            /// main[main_idx] > patch[row]: the current patch key has no match in main.
            /// Advance the cursor to the first key that is not less than the main key.
            if (compareSortKeyRows(result_sorting_key, main_idx, top_cursor.sorting_key_columns, top_cursor.row, reverse_flags) > 0)
            {
                top_cursor.row = gallopingBinarySearch<true>(top_cursor.sorting_key_columns, top_cursor.row, top_cursor.num_rows, result_sorting_key, main_idx, reverse_flags);

                if (top_cursor.row == top_cursor.num_rows)
                {
                    live_cursors[min_pos] = live_cursors.back();
                    live_cursors.pop_back();
                }

                continue;
            }

            /// cmp == 0: equal-sort-key run on both sides. Find the run extents. Gallop on the
            /// main side. The patch side is scanned linearly because patch runs are small.
            size_t main_run_end = gallopingBinarySearch<false>(result_sorting_key, main_idx + 1, main_rows, result_sorting_key, main_idx, reverse_flags);

            /// Collect cursors with the key equal to the main key. The minimal cursor is equal
            /// by construction, the rest need one comparison each.
            equal_cursors.clear();
            size_t num_patch_rows_in_run = 0;

            for (size_t i = 0; i < live_cursors.size(); ++i)
            {
                auto & cursor = cursors[live_cursors[i]];

                if (i != min_pos && compareSortKeyRows(cursor.sorting_key_columns, cursor.row, result_sorting_key, main_idx, reverse_flags) != 0)
                    continue;

                cursor.run_end = cursor.row + 1;
                while (cursor.run_end < cursor.num_rows && compareSortKeyRows(cursor.sorting_key_columns, cursor.run_end, result_sorting_key, main_idx, reverse_flags) == 0)
                    ++cursor.run_end;

                num_patch_rows_in_run += cursor.run_end - cursor.row;
                equal_cursors.push_back(live_cursors[i]);
            }

            process_equal_run(main_idx, main_run_end, num_patch_rows_in_run);

            for (size_t cursor_idx : equal_cursors)
                cursors[cursor_idx].row = cursors[cursor_idx].run_end;

            for (size_t i = 0; i < live_cursors.size();)
            {
                if (cursors[live_cursors[i]].row == cursors[live_cursors[i]].num_rows)
                {
                    live_cursors[i] = live_cursors.back();
                    live_cursors.pop_back();
                }
                else
                    ++i;
            }

            main_idx = main_run_end;
        }
    }
    else
    {
        /// Heap of cursors ordered by the sort key of the current row, the smallest key at the top.
        std::vector<size_t> heap;
        heap.reserve(cursors.size());

        for (size_t i = 0; i < cursors.size(); ++i)
        {
            if (cursors[i].row < cursors[i].num_rows)
                heap.push_back(i);
        }

        auto greater = [&](size_t lhs, size_t rhs)
        {
            return compareSortKeyRows(
                cursors[lhs].sorting_key_columns, cursors[lhs].row,
                cursors[rhs].sorting_key_columns, cursors[rhs].row,
                reverse_flags) > 0;
        };

        std::make_heap(heap.begin(), heap.end(), greater);

        while (main_idx < main_rows && !heap.empty())
        {
            auto & top_cursor = cursors[heap.front()];

            main_idx = gallopingBinarySearch<true>(result_sorting_key, main_idx, main_rows, top_cursor.sorting_key_columns, top_cursor.row, reverse_flags);
            if (main_idx == main_rows)
                break;

            /// main[main_idx] > patch[row]: the current patch key has no match in main.
            /// Advance the cursor to the first key that is not less than the main key.
            if (compareSortKeyRows(result_sorting_key, main_idx, top_cursor.sorting_key_columns, top_cursor.row, reverse_flags) > 0)
            {
                std::pop_heap(heap.begin(), heap.end(), greater);
                top_cursor.row = gallopingBinarySearch<true>(top_cursor.sorting_key_columns, top_cursor.row, top_cursor.num_rows, result_sorting_key, main_idx, reverse_flags);

                if (top_cursor.row == top_cursor.num_rows)
                    heap.pop_back();
                else
                    std::push_heap(heap.begin(), heap.end(), greater);

                continue;
            }

            /// cmp == 0: equal-sort-key run on both sides. Find the run extents. Gallop on the main
            /// side. The patch side is scanned linearly because patch runs are small.
            size_t main_run_end = gallopingBinarySearch<false>(result_sorting_key, main_idx + 1, main_rows, result_sorting_key, main_idx, reverse_flags);

            /// Extract all cursors with the key equal to the main key. Cursors are compared against
            /// the main pivot row because extracted cursors may point one past their run's end.
            equal_cursors.clear();
            size_t num_patch_rows_in_run = 0;

            while (!heap.empty())
            {
                auto & cursor = cursors[heap.front()];
                if (compareSortKeyRows(cursor.sorting_key_columns, cursor.row, result_sorting_key, main_idx, reverse_flags) != 0)
                    break;

                cursor.run_end = cursor.row + 1;
                while (cursor.run_end < cursor.num_rows && compareSortKeyRows(cursor.sorting_key_columns, cursor.run_end, result_sorting_key, main_idx, reverse_flags) == 0)
                    ++cursor.run_end;

                num_patch_rows_in_run += cursor.run_end - cursor.row;
                equal_cursors.push_back(heap.front());

                std::pop_heap(heap.begin(), heap.end(), greater);
                heap.pop_back();
            }

            process_equal_run(main_idx, main_run_end, num_patch_rows_in_run);

            for (size_t cursor_idx : equal_cursors)
            {
                auto & cursor = cursors[cursor_idx];
                cursor.row = cursor.run_end;

                if (cursor.row < cursor.num_rows)
                {
                    heap.push_back(cursor_idx);
                    std::push_heap(heap.begin(), heap.end(), greater);
                }
            }

            main_idx = main_run_end;
        }
    }

    std::vector<std::pair<Names, PatchToApplyPtr>> result;
    result.reserve(groups.size());

    for (auto & group : groups)
    {
        /// Block indices can be omitted in case of one source.
        if (group.patch->getNumSources() == 1)
            group.patch->patch_block_indices.clear();

        result.emplace_back(std::move(group.updated_columns), std::move(group.patch));
    }

    return result;
}

PatchToApplyPtr applyPatchJoin(const Block & result_block, const PatchJoinCache::Entry & join_entry)
{
    std::shared_lock lock(join_entry.mutex);

    auto patch_to_apply = std::make_shared<PatchToApply>();
    patch_to_apply->patch_blocks.reserve(join_entry.blocks.size());

    for (const auto & block : join_entry.blocks)
    {
        if (block->rows() != 0)
            patch_to_apply->patch_blocks.push_back(*block);
    }

    size_t num_rows = result_block.rows();
    if (num_rows == 0 || join_entry.hash_map.empty())
        return patch_to_apply;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesJoinMicroseconds);

    auto block_number_column = result_block.getByName(BlockNumberColumn::name).column->convertToFullIfNeeded();
    auto block_offset_column = result_block.getByName(BlockOffsetColumn::name).column->convertToFullIfNeeded();

    const auto & result_block_number = assert_cast<const ColumnUInt64 &>(*block_number_column).getData();
    const auto & result_block_offset = assert_cast<const ColumnUInt64 &>(*block_offset_column).getData();

    size_t size_to_reserve = std::min(num_rows, join_entry.hash_map.size());
    patch_to_apply->result_row_indices.reserve(size_to_reserve);
    patch_to_apply->patch_block_indices.reserve(size_to_reserve);
    patch_to_apply->patch_row_indices.reserve(size_to_reserve);

    struct IteratorsPair
    {
        bool found = false;
        PatchOffsetsMap::const_iterator it;
        PatchOffsetsMap::const_iterator end;
    };

    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    /// Mapping from block number to iterator in offsets map.
    absl::flat_hash_map<UInt64, IteratorsPair, HashCRC32<UInt64>> offsets_iterators;
    IteratorsPair * current_offset_iterators = nullptr;

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Check that offsets are sorted within each block number.
    absl::flat_hash_map<UInt64, UInt64> last_offset_by_block_number;
#endif

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (result_block_number[row] < join_entry.min_block || result_block_number[row] > join_entry.max_block)
            continue;

#ifdef DEBUG_OR_SANITIZER_BUILD
        {
            auto it = last_offset_by_block_number.find(result_block_number[row]);
            if (it != last_offset_by_block_number.end() && it->second >= result_block_offset[row])
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Block offsets ({}, {}) are not sorted within block number {}", it->second, result_block_offset[row], result_block_number[row]);

            last_offset_by_block_number[result_block_number[row]] = result_block_offset[row];
        }
#endif

        if (result_block_number[row] != prev_block_number)
        {
            prev_block_number = result_block_number[row];
            auto [block_number_it, inserted] = offsets_iterators.try_emplace(result_block_number[row]);

            if (inserted)
            {
                auto it = join_entry.hash_map.find(result_block_number[row]);

                if (it != join_entry.hash_map.end())
                {
                    const auto & offsets_map = it->second;
                    auto & iterators = block_number_it->second;

                    iterators.found = true;
                    iterators.it = offsets_map.lower_bound(result_block_offset[row]);
                    iterators.end = offsets_map.end();
                }
            }

            current_offset_iterators = &block_number_it->second;
        }

        chassert(current_offset_iterators);
        auto & iterators = *current_offset_iterators;

        if (iterators.found)
        {
            while (iterators.it != iterators.end && iterators.it->first < result_block_offset[row])
            {
                ++iterators.it;
            }

            if (iterators.it != iterators.end && iterators.it->first == result_block_offset[row])
            {
                const auto & [patch_block_index, patch_row_index] = iterators.it->second;

                patch_to_apply->result_row_indices.push_back(row);
                patch_to_apply->patch_block_indices.push_back(patch_block_index);
                patch_to_apply->patch_row_indices.push_back(patch_row_index);
            }
        }
    }

    return patch_to_apply;
}

void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Names & updated_columns,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ApplyPatchesMicroseconds);
    NameSet updated_columns_set(updated_columns.begin(), updated_columns.end());
    auto updated_header = getUpdatedHeader(patches, updated_columns_set);

    if (canApplyPatchesRaw(patches))
        applyPatchesToBlockRaw(result_block, versions_block, patches, updated_header, source_data_version);
    else
        applyPatchesToBlockCombined(result_block, versions_block, patches, updated_header, source_data_version);
}

}
