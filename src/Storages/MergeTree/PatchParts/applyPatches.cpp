#include <Core/CompareHelper.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/PatchParts/applyPatchesLegacy.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/KeyDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Core/Names.h>
#include <absl/container/flat_hash_map.h>
#include <base/types.h>
#include <algorithm>

namespace ProfileEvents
{
    extern const Event ApplyPatchesMicroseconds;
    extern const Event ApplyPatchMergeOnKeyMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
static VectorWithMemoryTracking<IColumn::Patch::Source> createPatchSources(const Blocks & patch_blocks, const ColumnWithTypeAndName & result_column, Columns & columns_holder)
{
    VectorWithMemoryTracking<IColumn::Patch::Source> sources;
    sources.reserve(patch_blocks.size());

    for (const auto & patch_block : patch_blocks)
    {
        const auto & patch_column = patch_block.getByName(result_column.name);
        if (!patch_column.column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} has null data in patch block", result_column.name);

        const IColumn * source_col = patch_column.column.get();

        /// Patch column may have a different on-disk type when it predates
        /// an ALTER MODIFY COLUMN that hasn't been materialized yet.
        if (!result_column.column->structureEquals(*source_col))
        {
            columns_holder.push_back(castColumn(patch_column, result_column.type));
            source_col = columns_holder.back().get();
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

Block getUpdatedHeader(const PatchesIndices & patches)
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
            /// Ignore system columns and columns that have no data.
            if (isPatchPartSystemColumn(column.name) || !column.column)
                header.erase(column.name);
        }

        /// Sort columns by name so that assertCompatibleHeader
        /// below compares matching columns at the same positions.
        headers.push_back(header.sortColumns());
    }

    if (headers.empty())
        return {};

    /// Schema evolution may cause type mismatches across patch headers.
    /// Skip assertion in that case — castColumn in apply handles conversion.
    for (size_t i = 1; i < headers.size(); ++i)
    {
        if (!isCompatibleHeader(headers[i], headers[0]))
            return headers.front();
    }

    for (size_t i = 1; i < headers.size(); ++i)
        assertCompatibleHeader(headers[i], headers[0], "patch parts");

    return headers.front();
}

/// Applies each patch as-is, without combining row indices across patches.
/// Patches may have multiple source blocks (e.g. built by applyPatchesMergeOnKey).
void applyPatchesIndices(
    Block & result_block,
    Block & versions_block,
    const PatchesIndices & patches,
    const Block & updated_header,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    for (auto & result_column : result_block)
    {
        /// A column without data is not filled yet at this stage of reading (e.g. it is filled
        /// with evaluated defaults at the last stage) and is patched when it gets the data.
        if (!result_column.column || !updated_header.has(result_column.name))
            continue;

        auto & result_versions = addDataVersionForColumn(versions_block, result_column.name, result_block.rows(), source_data_version);
        result_column.column = removeSpecialRepresentations(result_column.column);

        for (const auto & patch_indices : patches)
        {
            if (patch_indices->patch_blocks.empty())
                continue;

            const auto * patch_column = patch_indices->patch_blocks.front().findByName(result_column.name);
            if (!patch_column || !patch_column->column)
                continue;

            /// Local storage so cast results are released after each patch.
            Columns converted_columns;

            auto patch = IColumn::Patch
            {
                .sources = createPatchSources(patch_indices->patch_blocks, result_column, converted_columns),
                .src_col_indices = patch_indices->getNumSources() > 1 ? &patch_indices->patch_block_indices : nullptr,
                .src_row_indices = patch_indices->patch_row_indices,
                .dst_row_indices = patch_indices->result_row_indices,
                .dst_versions = result_versions,
            };

            if (canApplyPatchInplace(*result_column.column))
            {
                auto mutable_column = IColumn::mutate(std::move(result_column.column));
                mutable_column->updateInplaceFrom(patch);
                result_column.column = std::move(mutable_column);
            }
            else
            {
                result_column.column = result_column.column->updateFrom(patch);
            }
        }
    }
}

namespace
{

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

ColumnRawPtrs extractRawColumns(const Block & block, const Names & column_names)
{
    ColumnRawPtrs out;
    out.reserve(column_names.size());

    for (const auto & name : column_names)
        out.push_back(block.getByName(name).column.get());

    return out;
}

Block getBlockWithSortingKey(const Block & block, const KeyDescription & sorting_key)
{
    Block result;
    for (const auto & name : sorting_key.column_names)
        result.insert(block.getByName(name));

    result.insert(block.getByName(BlockNumberColumn::name));
    result.insert(block.getByName(BlockOffsetColumn::name));

    /// Key comparisons require the same column class on all sides.
    for (auto & column : result)
    {
        column.column = recursiveRemoveLowCardinality(removeSpecialRepresentations(column.column->convertToFullColumnIfConst()));
        column.type = recursiveRemoveLowCardinality(column.type);
    }

    return result;
}

/// Read results of MergeOnKey patches that share one sorting key and are applied in one merge pass.
/// `updated_columns[i]` is the set of result columns updated from `blocks[i]`.
struct MergeOnKeyGroup
{
    const KeyDescription * sorting_key = nullptr;
    std::vector<const Names *> updated_columns;
    std::vector<const Block *> blocks;
};

/// Combined patches, one per distinct set of updated columns.
using PatchIndicesGroups = std::vector<std::shared_ptr<PatchIndices>>;

/// Cursor over one block (a patch block or the result block) in the merge of applyPatchesMergeOnKey.
/// `row` and `run_end` delimit the current run of equal sort keys.
struct BlockCursor
{
    size_t num_rows = 0;
    ColumnRawPtrs sorting_key_columns;
    const PaddedPODArray<UInt64> * block_number = nullptr;
    const PaddedPODArray<UInt64> * block_offset = nullptr;
    const PaddedPODArray<UInt64> * versions = nullptr;

    /// Group of patch blocks with the same set of updated columns.
    size_t group_idx = 0;
    /// Index of the block in the group's patch_blocks.
    UInt32 block_idx_in_group = 0;
    /// Current row in the block.
    size_t row = 0;
    /// End of the current run of equal sort keys.
    size_t run_end = 0;

    BlockCursor(const Block & block, const KeyDescription & sorting_key)
        : num_rows(block.rows())
        , sorting_key_columns(extractRawColumns(block, sorting_key.column_names))
        , block_number(&getColumnUInt64Data(block, BlockNumberColumn::name))
        , block_offset(&getColumnUInt64Data(block, BlockOffsetColumn::name))
        , versions(block.has(PartDataVersionColumn::name) ? &getColumnUInt64Data(block, PartDataVersionColumn::name) : nullptr)
    {
    }

    ALWAYS_INLINE size_t blockNumber() const { return (*block_number)[row]; }
    ALWAYS_INLINE size_t blockOffset() const { return (*block_offset)[row]; }
    ALWAYS_INLINE size_t runLength() const { return run_end - row; }
    ALWAYS_INLINE bool isFinished() const { return row >= num_rows; }

    ALWAYS_INLINE int compare(const BlockCursor & other, const std::vector<bool> & reverse_flags) const
    {
        return compareSortKeyRows(sorting_key_columns, row, other.sorting_key_columns, other.row, reverse_flags);
    }

    void advanceRowToCursor(const BlockCursor & other, const std::vector<bool> & reverse_flags)
    {
        row = gallopingBinarySearch<true>(sorting_key_columns, row, num_rows, other.sorting_key_columns, other.row, reverse_flags);
    }

    void advanceRunEndGalloping(const BlockCursor & other, const std::vector<bool> & reverse_flags)
    {
        run_end = gallopingBinarySearch<false>(sorting_key_columns, row + 1, num_rows, other.sorting_key_columns, other.row, reverse_flags);
    }

    void advanceRunEndLinear(const BlockCursor & other, const std::vector<bool> & reverse_flags)
    {
        run_end = row + 1;
        while (run_end < num_rows && compareSortKeyRows(sorting_key_columns, run_end, other.sorting_key_columns, other.row, reverse_flags) == 0)
            ++run_end;
    }
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

/// Scratch structures for one equal-sort-key run, reused across runs.
struct EqualRunScratch
{
    absl::flat_hash_map<UInt128, UInt32, UInt128TrivialHash> run_map;
    PaddedPODArray<RunEntry> run_entries;
};

/// Emits a matched pair of a result row and a patch row into `patch`.
ALWAYS_INLINE void addMatchedRow(PatchIndices & patch, UInt64 result_row, UInt32 block_idx, UInt64 patch_row)
{
    patch.result_row_indices.push_back(result_row);
    patch.patch_row_indices.push_back(patch_row);

    /// The number of sources is final before the merge, and one source needs no block indices.
    if (patch.getNumSources() > 1)
        patch.patch_block_indices.push_back(block_idx);
}

/// Processes one run of equal sort keys: matches result rows [result_cursor.row, result_cursor.run_end)
/// with rows [cursor.row, cursor.run_end) of cursors in `equal_cursors` by the
/// (block_number, block_offset) identity and emits matches into the groups' patches.
void processEqualKeyCursors(
    const BlockCursor & result_cursor,
    size_t num_patch_rows_in_run,
    const std::vector<size_t> & equal_cursors,
    const std::vector<BlockCursor> & cursors,
    PatchIndicesGroups & groups,
    EqualRunScratch & scratch)
{
    if (num_patch_rows_in_run == 1 && result_cursor.runLength() == 1)
    {
        /// Common case for unique sort keys: no hash map, just compare identity directly.
        chassert(equal_cursors.size() == 1);
        const auto & cursor = cursors[equal_cursors.front()];

        if (cursor.blockNumber() == result_cursor.blockNumber() && cursor.blockOffset() == result_cursor.blockOffset())
            addMatchedRow(*groups[cursor.group_idx], result_cursor.row, cursor.block_idx_in_group, cursor.row);

        return;
    }

    size_t num_groups = groups.size();
    auto & run_map = scratch.run_map;
    auto & run_entries = scratch.run_entries;

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
                run_entries.resize_fill(run_entries.size() + num_groups, RunEntry{});

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
    for (size_t i = result_cursor.row; i < result_cursor.run_end; ++i)
    {
        auto identity = makeBlockIdentity((*result_cursor.block_number)[i], (*result_cursor.block_offset)[i]);
        auto it = run_map.find(identity);

        if (it == run_map.end())
            continue;

        for (size_t group_idx = 0; group_idx < num_groups; ++group_idx)
        {
            const auto & entry = run_entries[it->second + group_idx];
            if (entry.block_idx == RunEntry::EMPTY_BLOCK)
                continue;

            addMatchedRow(*groups[group_idx], i, entry.block_idx, entry.row_idx);
        }
    }
}

/// Drives the merge with a linear scan for the cursor with the minimal key.
void applyCursorsLinear(
    BlockCursor & result_cursor,
    std::vector<BlockCursor> & cursors,
    PatchIndicesGroups & groups,
    const std::vector<bool> & reverse_flags)
{
    EqualRunScratch run_scratch;
    std::vector<size_t> equal_cursors;

    /// Indices of cursors with unprocessed rows.
    std::vector<size_t> live_cursors;
    live_cursors.reserve(cursors.size());

    for (size_t i = 0; i < cursors.size(); ++i)
    {
        if (!cursors[i].isFinished())
            live_cursors.push_back(i);
    }

    while (!result_cursor.isFinished() && !live_cursors.empty())
    {
        /// Find the cursor with the minimal current key.
        size_t min_pos = 0;

        for (size_t i = 1; i < live_cursors.size(); ++i)
        {
            const auto & cursor = cursors[live_cursors[i]];
            const auto & min_cursor = cursors[live_cursors[min_pos]];

            if (cursor.compare(min_cursor, reverse_flags) < 0)
                min_pos = i;
        }

        auto & top_cursor = cursors[live_cursors[min_pos]];
        result_cursor.advanceRowToCursor(top_cursor, reverse_flags);

        if (result_cursor.isFinished())
            break;

        /// main[row] > patch[row]: the current patch key has no match in main.
        /// Advance the cursor to the first key that is not less than the main key.
        if (result_cursor.compare(top_cursor, reverse_flags) > 0)
        {
            top_cursor.advanceRowToCursor(result_cursor, reverse_flags);

            if (top_cursor.isFinished())
            {
                live_cursors[min_pos] = live_cursors.back();
                live_cursors.pop_back();
            }

            continue;
        }

        /// cmp == 0: equal-sort-key run on both sides. Find the run extents.
        result_cursor.advanceRunEndGalloping(top_cursor, reverse_flags);
        equal_cursors.clear();
        size_t num_patch_rows_in_run = 0;

        for (size_t i = 0; i < live_cursors.size(); ++i)
        {
            auto & cursor = cursors[live_cursors[i]];
            if (i != min_pos && cursor.compare(result_cursor, reverse_flags) != 0)
                continue;

            /// Scan the patch side linearly because patch runs are usually small.
            cursor.advanceRunEndLinear(result_cursor, reverse_flags);
            num_patch_rows_in_run += cursor.runLength();
            equal_cursors.push_back(live_cursors[i]);
        }

        processEqualKeyCursors(result_cursor, num_patch_rows_in_run, equal_cursors, cursors, groups, run_scratch);

        for (size_t cursor_idx : equal_cursors)
        {
            cursors[cursor_idx].row = cursors[cursor_idx].run_end;
        }

        for (size_t i = 0; i < live_cursors.size();)
        {
            if (cursors[live_cursors[i]].isFinished())
            {
                live_cursors[i] = live_cursors.back();
                live_cursors.pop_back();
            }
            else
            {
                ++i;
            }
        }

        result_cursor.row = result_cursor.run_end;
    }
}

/// Drives the merge with a heap of cursors ordered by the sort key of the current row.
void applyCursorsHeap(
    BlockCursor & result_cursor,
    std::vector<BlockCursor> & cursors,
    PatchIndicesGroups & groups,
    const std::vector<bool> & reverse_flags)
{
    EqualRunScratch run_scratch;
    std::vector<size_t> equal_cursors;

    /// Heap of cursors ordered by the sort key of the current row, the smallest key at the top.
    std::vector<size_t> heap;
    heap.reserve(cursors.size());

    for (size_t i = 0; i < cursors.size(); ++i)
    {
        if (!cursors[i].isFinished())
            heap.push_back(i);
    }

    auto greater = [&](size_t lhs, size_t rhs)
    {
        return cursors[lhs].compare(cursors[rhs], reverse_flags) > 0;
    };

    std::make_heap(heap.begin(), heap.end(), greater);

    while (!result_cursor.isFinished() && !heap.empty())
    {
        auto & top_cursor = cursors[heap.front()];
        result_cursor.advanceRowToCursor(top_cursor, reverse_flags);

        if (result_cursor.isFinished())
            break;

        /// main[row] > patch[row]: the current patch key has no match in main.
        /// Advance the cursor to the first key that is not less than the main key.
        if (result_cursor.compare(top_cursor, reverse_flags) > 0)
        {
            std::pop_heap(heap.begin(), heap.end(), greater);
            top_cursor.advanceRowToCursor(result_cursor, reverse_flags);

            if (top_cursor.isFinished())
                heap.pop_back();
            else
                std::push_heap(heap.begin(), heap.end(), greater);

            continue;
        }

        /// cmp == 0: equal-sort-key run on both sides. Find the run extents. Gallop on the main side.
        result_cursor.advanceRunEndGalloping(top_cursor, reverse_flags);
        equal_cursors.clear();
        size_t num_patch_rows_in_run = 0;

        while (!heap.empty())
        {
            auto & cursor = cursors[heap.front()];
            if (cursor.compare(result_cursor, reverse_flags) != 0)
                break;

            /// Scan the patch side linearly because patch runs are usually small.
            cursor.advanceRunEndLinear(result_cursor, reverse_flags);
            num_patch_rows_in_run += cursor.runLength();
            equal_cursors.push_back(heap.front());

            std::pop_heap(heap.begin(), heap.end(), greater);
            heap.pop_back();
        }

        processEqualKeyCursors(result_cursor, num_patch_rows_in_run, equal_cursors, cursors, groups, run_scratch);

        for (size_t cursor_idx : equal_cursors)
        {
            auto & cursor = cursors[cursor_idx];
            cursor.row = cursor.run_end;

            if (!cursor.isFinished())
            {
                heap.push_back(cursor_idx);
                std::push_heap(heap.begin(), heap.end(), greater);
            }
        }

        result_cursor.row = result_cursor.run_end;
    }
}

void updateHashWithColumn(SipHash & hash, const ColumnWithTypeAndName & column)
{
    auto type_name = column.type->getName();

    hash.update(column.name.size());
    hash.update(column.name.data(), column.name.size());
    hash.update(type_name.size());
    hash.update(type_name.data(), type_name.size());
}

std::vector<PatchIndicesPtr> applyPatchesMergeOnKey(const Block & result_block, const MergeOnKeyGroup & group)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ApplyPatchMergeOnKeyMicroseconds);
    size_t main_rows = result_block.rows();

    if (main_rows == 0 || group.blocks.empty())
        return {};

    const auto & sorting_key = *group.sorting_key;
    const auto & reverse_flags = sorting_key.reverse_flags;
    auto sorting_key_block = getBlockWithSortingKey(result_block, sorting_key);
    BlockCursor result_cursor(sorting_key_block, sorting_key);

    PatchIndicesGroups indices_groups;
    absl::flat_hash_map<UInt128, size_t, UInt128TrivialHash> group_index_by_columns;
    std::vector<Block> patch_blocks; // Keeps columns referenced by cursors alive.
    std::vector<BlockCursor> cursors;

    patch_blocks.reserve(group.blocks.size());
    cursors.reserve(group.blocks.size());
    chassert(group.blocks.size() == group.updated_columns.size());

    for (size_t block_idx = 0; block_idx < group.blocks.size(); ++block_idx)
    {
        if (group.blocks[block_idx]->rows() == 0)
            continue;

        Block patch_block = *group.blocks[block_idx];
        for (auto & column : patch_block)
            column.column = removeSpecialRepresentations(column.column);

        /// Group patch blocks by the hash of names and types of updated columns present in the block.
        SipHash hash;
        Block updated_block;

        for (const auto & name : *group.updated_columns[block_idx])
        {
            const auto * column = patch_block.findByName(name);

            if (column && column->column)
            {
                updateHashWithColumn(hash, *column);
                updated_block.insert(*column);
            }
        }

        if (updated_block.empty())
            continue;

        updated_block.insert(patch_block.getByName(PartDataVersionColumn::name));

        auto [group_it, group_inserted] = group_index_by_columns.try_emplace(hash.get128(), indices_groups.size());
        if (group_inserted)
            indices_groups.push_back(std::make_shared<PatchIndices>());

        auto & columns_group = indices_groups[group_it->second];
        auto & cursor = cursors.emplace_back(patch_block, sorting_key);

        cursor.group_idx = group_it->second;
        cursor.block_idx_in_group = static_cast<UInt32>(columns_group->patch_blocks.size());
        cursor.advanceRowToCursor(result_cursor, reverse_flags);

        columns_group->patch_blocks.push_back(std::move(updated_block));
        patch_blocks.push_back(std::move(patch_block));
    }

    static constexpr size_t max_cursors_for_linear_apply = 8;

    if (cursors.size() <= max_cursors_for_linear_apply)
        applyCursorsLinear(result_cursor, cursors, indices_groups, reverse_flags);
    else
        applyCursorsHeap(result_cursor, cursors, indices_groups, reverse_flags);

    std::vector<PatchIndicesPtr> result;
    result.reserve(indices_groups.size());

    for (auto & columns_group : indices_groups)
        result.emplace_back(std::move(columns_group));

    return result;
}

}

void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const std::vector<PatchReadResultToApply> & patch_read_results,
    UInt64 source_data_version)
{
    applyPatchesToBlockLegacy(result_block, versions_block, patch_read_results, source_data_version);
    std::vector<MergeOnKeyGroup> merge_on_key_groups;

    for (const auto & [patch, read_result, updated_columns] : patch_read_results)
    {
        if (patch.mode != PatchMode::MergeOnKey)
            continue;

        /// MergeTreeData::getPatchPartSortingKey returns one object per
        /// effective key, so semantically equal keys are pointer-identical here.
        auto group_it = std::ranges::find_if(merge_on_key_groups, [&](const auto & group)
        {
            return group.sorting_key == patch.sorting_key.get();
        });

        if (group_it == merge_on_key_groups.end())
        {
            group_it = merge_on_key_groups.emplace(merge_on_key_groups.end());
            group_it->sorting_key = patch.sorting_key.get();
        }

        const auto & patch_data = typeid_cast<const PatchMergeOnKeyReadResult &>(*read_result);
        group_it->blocks.emplace_back(&patch_data.block);
        group_it->updated_columns.emplace_back(&updated_columns);
    }

    /// A combined MergeOnKey patch already has version-resolved row indices
    /// and is applied directly, without combining with other patches.
    for (const auto & group : merge_on_key_groups)
    {
        auto merge_on_key_patches = applyPatchesMergeOnKey(result_block, group);

        for (auto & patch_indices : merge_on_key_patches)
        {
            if (patch_indices->empty())
                continue;

            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ApplyPatchesMicroseconds);
            PatchesIndices patches{std::move(patch_indices)};
            applyPatchesIndices(result_block, versions_block, patches, getUpdatedHeader(patches), source_data_version);
        }
    }
}

}
