#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>

namespace ProfileEvents
{
    extern const Event ApplyPatchesMicroseconds;
    extern const Event BuildPatchesJoinMicroseconds;
    extern const Event BuildPatchesMergeMicroseconds;
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

struct CombinedPatchBuilder
{
public:
    explicit CombinedPatchBuilder(const PatchesToApply & patches_) : patches(patches_)
    {
        build();
    }

    IColumn::Patch createPatchForColumn(const String & column_name, IColumn::Versions & dst_versions) const;

private:
    void build();

    UInt64 getResultIndex(UInt64 col_idx, UInt64 row_idx) const
    {
        return patches[col_idx]->result_indices[row_idx];
    }

    UInt64 getPatchIndex(UInt64 col_idx, UInt64 row_idx) const
    {
        return patches[col_idx]->patch_indices[row_idx];
    }

    PatchesToApply patches;
    IColumn::Offsets src_col_indices;
    IColumn::Offsets src_row_indices;
    IColumn::Offsets dst_row_indices;
};

void CombinedPatchBuilder::build()
{
    std::vector<UInt64> heap;
    std::vector<UInt64> cursors(patches.size());
    std::vector<const IColumn::Versions *> versions(patches.size());

    for (size_t i = 0; i < patches.size(); ++i)
    {
        versions[i] = &getColumnUInt64Data(patches[i]->patch_block, PartDataVersionColumn::name);

        if (!patches[i]->empty())
            heap.push_back(i);
    }

    enum class RowOp
    {
        Skip,
        Add,
        Update,
    };

    auto get_row_op = [&](UInt64 col_idx, UInt64 row_idx)
    {
        chassert(src_col_indices.size() == dst_row_indices.size());
        chassert(src_row_indices.size() == dst_row_indices.size());

        if (dst_row_indices.empty())
            return RowOp::Add;

        UInt64 last_dst_row = dst_row_indices.back();
        UInt64 current_dst_row = getResultIndex(col_idx, row_idx);
        chassert(current_dst_row >= last_dst_row);

        if (current_dst_row != last_dst_row)
            return RowOp::Add;

        UInt64 last_src_col = src_col_indices.back();
        UInt64 last_src_row = src_row_indices.back();
        UInt64 current_src_row = getPatchIndex(col_idx, row_idx);

        UInt64 last_version = (*versions[last_src_col])[last_src_row];
        UInt64 current_version = (*versions[col_idx])[current_src_row];

        return current_version > last_version ? RowOp::Update : RowOp::Skip;
    };

    auto greater = [&](UInt64 lhs, UInt64 rhs)
    {
        return getResultIndex(lhs, cursors[lhs]) > getResultIndex(rhs, cursors[rhs]);
    };

    std::make_heap(heap.begin(), heap.end(), greater);

    while (!heap.empty())
    {
        UInt64 col_idx = heap.front();
        UInt64 row_idx = cursors[col_idx];

        std::pop_heap(heap.begin(), heap.end(), greater);
        heap.pop_back();

        auto row_op = get_row_op(col_idx, row_idx);

        if (row_op != RowOp::Skip)
        {
            UInt64 patch_row = getPatchIndex(col_idx, row_idx);
            UInt64 result_row = getResultIndex(col_idx, row_idx);

            if (row_op == RowOp::Update)
            {
                src_col_indices.back() = col_idx;
                src_row_indices.back() = patch_row;
                dst_row_indices.back() = result_row;
            }
            else
            {
                src_col_indices.push_back(col_idx);
                src_row_indices.push_back(patch_row);
                dst_row_indices.push_back(result_row);
            }
        }

        ++cursors[col_idx];
        if (cursors[col_idx] < patches[col_idx]->rows())
        {
            heap.push_back(col_idx);
            std::push_heap(heap.begin(), heap.end(), greater);
        }
    }
}

IColumn::Patch CombinedPatchBuilder::createPatchForColumn(const String & column_name, IColumn::Versions & dst_versions) const
{
    std::vector<IColumn::Patch::Source> sources;

    for (const auto & patch : patches)
    {
        IColumn::Patch::Source source =
        {
            .column = *patch->patch_block.getByName(column_name).column,
            .versions = getColumnUInt64Data(patch->patch_block, PartDataVersionColumn::name),
        };

        sources.push_back(std::move(source));
    }

    return IColumn::Patch
    {
        .sources = std::move(sources),
        .src_col_indices = &src_col_indices,
        .src_row_indices = src_row_indices,
        .dst_row_indices = dst_row_indices,
        .dst_versions = dst_versions,
    };
}

[[maybe_unused]] void assertPatchesBlockStructure(const PatchesToApply & patches)
{
    std::vector<Block> headers;

    for (const auto & patch : patches)
    {
        Block header = patch->patch_block.cloneEmpty();

        for (const auto & column : patch->patch_block)
        {
            /// System columns may differ because we allow to apply combined patches with different modes.
            if (isPatchPartSystemColumn(column.name))
                header.erase(column.name);
        }

        headers.push_back(std::move(header));
    }

    for (size_t i = 1; i < headers.size(); ++i)
        assertCompatibleHeader(headers[i], headers[0], "patch parts");
}

void applyPatchesToBlockRaw(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    for (auto & result_column : result_block)
    {
        if (!patches[0]->patch_block.has(result_column.name))
            continue;

        if (isPatchPartSystemColumn(result_column.name))
            continue;

        auto & result_versions = addDataVersionForColumn(versions_block, result_column.name, result_block.rows(), source_data_version);
        result_column.column = recursiveRemoveSparse(result_column.column);

        for (const auto & patch_to_apply : patches)
        {
            IColumn::Patch::Source source =
            {
                .column = *patch_to_apply->patch_block.getByName(result_column.name).column,
                .versions = getColumnUInt64Data(patch_to_apply->patch_block, PartDataVersionColumn::name),
            };

            IColumn::Patch patch =
            {
                .sources = {std::move(source)},
                .src_col_indices = nullptr,
                .src_row_indices = patch_to_apply->patch_indices,
                .dst_row_indices = patch_to_apply->result_indices,
                .dst_versions = result_versions,
            };

            if (canApplyPatchInplace(*result_column.column))
                result_column.column->assumeMutableRef().updateInplaceFrom(patch);
            else
                result_column.column = result_column.column->updateFrom(patch);
        }
    }
}

void applyPatchesToBlockCombined(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    CombinedPatchBuilder builder(patches);

    for (auto & result_column : result_block)
    {
        if (!patches[0]->patch_block.has(result_column.name))
            continue;

        if (isPatchPartSystemColumn(result_column.name))
            continue;

        auto & result_versions = addDataVersionForColumn(versions_block, result_column.name, result_block.rows(), source_data_version);
        auto multi_patch = builder.createPatchForColumn(result_column.name, result_versions);
        result_column.column = recursiveRemoveSparse(result_column.column);

        if (canApplyPatchInplace(*result_column.column))
            result_column.column->assumeMutableRef().updateInplaceFrom(multi_patch);
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
    patch_to_apply->patch_block = patch_block;

    size_t num_rows = result_block.rows();
    size_t patch_rows = patch_block.rows();

    if (num_rows == 0 || patch_rows == 0)
        return patch_to_apply;

    Stopwatch watch;

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

    patch_to_apply->result_indices.reserve(size_to_reserve);
    patch_to_apply->patch_indices.reserve(size_to_reserve);

    /// Optimize in case when _part_offset has all consecutive rows.
    if (last_result_offset - first_result_offset + 1 == num_rows)
    {
        for (size_t patch_row = patch_begin; patch_row < patch_end; ++patch_row)
        {
            chassert(patch_offset_data[patch_row] >= first_result_offset);
            size_t result_row = patch_offset_data[patch_row] - first_result_offset;

            patch_to_apply->patch_indices.push_back(patch_row);
            patch_to_apply->result_indices.push_back(result_row);
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
                patch_to_apply->patch_indices.push_back(patch_it++);
                patch_to_apply->result_indices.push_back(result_it++);
            }
        }
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesMergeMicroseconds, elapsed);

    return patch_to_apply;
}

std::shared_ptr<PatchJoinSharedData> buildPatchJoinData(const Block & patch_block)
{
    Stopwatch watch;

    size_t num_patch_rows = patch_block.rows();

    const auto & patch_block_number = getColumnUInt64Data(patch_block, BlockNumberColumn::name);
    const auto & patch_block_offset = getColumnUInt64Data(patch_block, BlockOffsetColumn::name);
    const auto & patch_data_version = getColumnUInt64Data(patch_block, PartDataVersionColumn::name);

    auto data = std::make_shared<PatchJoinSharedData>();
    data->hash_map.reserve(num_patch_rows);
    bool is_first = true;

    for (size_t i = 0; i < num_patch_rows; ++i)
    {
        UInt128 key;
        key.items[0] = patch_block_number[i];
        key.items[1] = patch_block_offset[i];

        bool inserted = false;
        PatchHashMap::LookupResult it;
        data->hash_map.emplace(key, it, inserted);

        /// Keep only the row with the highest version.
        if (!inserted && patch_data_version[i] <= patch_data_version[it->getMapped()])
            continue;

        it->getMapped() = i;

        if (std::exchange(is_first, false))
        {
            data->min_block = patch_block_number[i];
            data->max_block = patch_block_number[i];
        }
        else if (patch_block_number[i] < data->min_block)
        {
            data->min_block = patch_block_number[i];
        }
        else if (patch_block_number[i] > data->max_block)
        {
            data->max_block = patch_block_number[i];
        }
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesJoinMicroseconds, elapsed);

    return data;
}

PatchToApplyPtr applyPatchJoin(const Block & result_block, const Block & patch_block, const PatchJoinSharedData & join_data)
{
    auto patch_to_apply = std::make_shared<PatchToApply>();
    patch_to_apply->patch_block = patch_block;

    size_t num_rows = result_block.rows();
    size_t num_patch_rows = patch_block.rows();

    if (num_rows == 0 || num_patch_rows == 0 || join_data.hash_map.empty())
        return patch_to_apply;

    Stopwatch watch;

    auto block_number_column = result_block.getByName(BlockNumberColumn::name).column->convertToFullIfNeeded();
    auto block_offset_column = result_block.getByName(BlockOffsetColumn::name).column->convertToFullIfNeeded();

    const auto & result_block_number = assert_cast<const ColumnUInt64 &>(*block_number_column).getData();
    const auto & result_block_offset = assert_cast<const ColumnUInt64 &>(*block_offset_column).getData();

    size_t size_to_reserve = std::min(num_rows, join_data.hash_map.size());
    patch_to_apply->result_indices.reserve(size_to_reserve);
    patch_to_apply->patch_indices.reserve(size_to_reserve);

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (result_block_number[row] < join_data.min_block || result_block_number[row] > join_data.max_block)
            continue;

        UInt128 key;
        key.items[0] = result_block_number[row];
        key.items[1] = result_block_offset[row];

        if (const auto * found = join_data.hash_map.find(key))
        {
            patch_to_apply->result_indices.push_back(row);
            patch_to_apply->patch_indices.push_back(found->getMapped());
        }
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesJoinMicroseconds, elapsed);

    return patch_to_apply;
}

void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    UInt64 source_data_version)
{
    if (patches.empty())
        return;

    Stopwatch watch;

#ifdef DEBUG_OR_SANITIZER_BUILD
    assertPatchesBlockStructure(patches);
#endif

    bool can_apply_all_inplace = true;
    const auto & patch_header = patches[0]->patch_block;

    for (const auto & column : patch_header)
    {
        if (!isPatchPartSystemColumn(column.name) && !canApplyPatchInplace(*column.column))
        {
            can_apply_all_inplace = false;
            break;
        }
    }

    if (patches.size() == 1 || can_apply_all_inplace)
        applyPatchesToBlockRaw(result_block, versions_block, patches, source_data_version);
    else
        applyPatchesToBlockCombined(result_block, versions_block, patches, source_data_version);

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::ApplyPatchesMicroseconds, elapsed);
}

}
