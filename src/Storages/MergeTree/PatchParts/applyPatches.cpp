#include <shared_mutex>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>

namespace ProfileEvents
{
    extern const Event ApplyPatchesMicroseconds;
    extern const Event BuildPatchesJoinMicroseconds;
    extern const Event BuildPatchesMergeMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
    extern const Metric IOThreadsScheduled;
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

[[maybe_unused]] void assertPatchesBlockStructure(const PatchesToApply & patches, const Block & result_block)
{
    std::vector<Block> headers;

    for (const auto & patch : patches)
    {
        Block header = patch->patch_block.cloneEmpty();

        for (const auto & column : patch->patch_block)
        {
            /// System columns may differ because we allow to apply combined patches with different modes.
            /// Ignore columns that are not present in result block.
            if (isPatchPartSystemColumn(column.name) || !result_block.has(column.name))
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

PatchJoinCache::PatchJoinCache(size_t num_buckets_, ThreadPool & thread_pool_)
    : num_buckets(num_buckets_), thread_pool(thread_pool_)
{
}

void PatchJoinCache::init(const RangesInPatchParts & ranges_in_pathces)
{
    const auto & all_ranges = ranges_in_pathces.getRanges();

    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (ranges.empty())
            continue;

        size_t current_buckets = std::min(num_buckets, ranges.size());
        size_t num_ranges_in_bucket = ranges.size() / current_buckets;

        auto & entries = cache[patch_name];
        auto & buckets = ranges_to_buckets[patch_name];

        entries.reserve(current_buckets);
        for (size_t i = 0; i < ranges.size(); ++i)
        {
            size_t idx = i / num_ranges_in_bucket;
            buckets[ranges[i]] = idx;
            entries.push_back(std::make_shared<PatchJoinCache::Entry>());
        }
    }
}

PatchJoinCache::Entries PatchJoinCache::getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader)
{
    auto [entries, ranges_for_entries] = getEntriesAndRanges(patch_name, ranges);
    std::vector<std::shared_future<void>> futures;

    for (size_t i = 0; i < entries.size(); ++i)
    {
        auto entry_futures = entries[i]->addRangesAsync(ranges_for_entries[i], thread_pool, reader);
        futures.insert(futures.end(), entry_futures.begin(), entry_futures.end());
    }

    for (const auto & future : futures)
        future.get();

    return entries;
}

std::pair<PatchJoinCache::Entries, std::vector<MarkRanges>> PatchJoinCache::getEntriesAndRanges(const String & patch_name, const MarkRanges & ranges)
{
    std::lock_guard lock(mutex);
    const auto & entries = cache.at(patch_name);

    if (entries.empty())
        return {};

    std::set<size_t> entries_indexes;
    std::map<size_t, MarkRanges> ranges_for_entries;
    const auto & buckets = ranges_to_buckets.at(patch_name);

    for (const auto & range : ranges)
    {
        size_t idx = buckets.at(range);
        entries_indexes.insert(idx);
        ranges_for_entries[idx].push_back(range);
    }

    Entries result_entries;
    std::vector<MarkRanges> result_ranges;

    for (const auto & idx : entries_indexes)
    {
        result_entries.push_back(entries.at(idx));
        result_ranges.push_back(ranges_for_entries.at(idx));
    }

    return {result_entries, result_ranges};
}

std::vector<std::shared_future<void>> PatchJoinCache::Entry::addRangesAsync(const MarkRanges & ranges, ThreadPool & pool, Reader reader)
{
    std::vector<std::shared_future<void>> futures;
    std::vector<std::shared_ptr<std::promise<void>>> promises;
    MarkRanges ranges_to_read;

    {
        std::lock_guard lock(mutex);

        for (const auto & range : ranges)
        {
            if (ranges_futures.contains(range))
            {
                futures.push_back(ranges_futures.at(range));
            }
            else
            {
                ranges_to_read.push_back(range);

                auto promise = std::make_shared<std::promise<void>>();
                auto future = promise->get_future().share();

                ranges_futures.emplace(range, future);
                promises.push_back(promise);
                futures.push_back(future);
            }
        }
    }

    if (!ranges_to_read.empty())
    {
        pool.scheduleOrThrowOnError([this, ranges_to_read, reader, promises]
        {
            try
            {
                auto read_block = reader(ranges_to_read);
                addBlock(std::move(read_block));

                for (const auto & promise : promises)
                    promise->set_value();
            }
            catch (...)
            {
                for (const auto & promise : promises)
                    promise->set_exception(std::current_exception());
            }
        });
    }

    return futures;
}

void PatchJoinCache::Entry::addBlock(Block read_block)
{
    Stopwatch watch;

    size_t old_num_rows = block.rows();
    size_t num_read_rows = read_block.rows();

    if (num_read_rows == 0)
        return;

    std::lock_guard lock(mutex);

    if (old_num_rows == 0)
    {
        block = read_block;
    }
    else
    {
        for (size_t i = 0; i < read_block.columns(); ++i)
        {
            auto & result_column = block.safeGetByPosition(i).column;
            const auto & read_column = read_block.safeGetByPosition(i).column;
            result_column->assumeMutableRef().insertRangeFrom(*read_column, 0, read_column->size());
        }
    }

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    OffsetsHashMap * offsets_hash_map = nullptr;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            offsets_hash_map = &hash_map[block_number];

            min_block = std::min(min_block, block_number);
            max_block = std::max(max_block, block_number);
        }

        auto [it, inserted] = offsets_hash_map->try_emplace(block_offset);

        /// Keep only the row with the highest version.
        if (inserted || data_version_column[i] > data_version_column[it->second])
            it->second = i + old_num_rows;
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesJoinMicroseconds, elapsed);
}

PatchToApplyPtr applyPatchJoin(const Block & result_block, const Block & patch_block, const PatchJoinCache::Entry & join_entry)
{
    std::shared_lock lock(join_entry.mutex);

    auto patch_to_apply = std::make_shared<PatchToApply>();
    patch_to_apply->patch_block = patch_block;

    size_t num_rows = result_block.rows();
    size_t num_patch_rows = patch_block.rows();

    if (num_rows == 0 || num_patch_rows == 0 || join_entry.hash_map.empty())
        return patch_to_apply;

    Stopwatch watch;

    auto block_number_column = result_block.getByName(BlockNumberColumn::name).column->convertToFullIfNeeded();
    auto block_offset_column = result_block.getByName(BlockOffsetColumn::name).column->convertToFullIfNeeded();

    const auto & result_block_number = assert_cast<const ColumnUInt64 &>(*block_number_column).getData();
    const auto & result_block_offset = assert_cast<const ColumnUInt64 &>(*block_offset_column).getData();

    size_t size_to_reserve = std::min(num_rows, join_entry.hash_map.size());
    patch_to_apply->result_indices.reserve(size_to_reserve);
    patch_to_apply->patch_indices.reserve(size_to_reserve);

    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    const OffsetsHashMap * offsets_hash_map = nullptr;

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (result_block_number[row] < join_entry.min_block || result_block_number[row] > join_entry.max_block)
        {
            continue;
        }

        if (result_block_number[row] != prev_block_number)
        {
            prev_block_number = result_block_number[row];
            auto it = join_entry.hash_map.find(prev_block_number);
            offsets_hash_map = it != join_entry.hash_map.end() ? &it->second : nullptr;
        }

        if (offsets_hash_map)
        {
            auto offset_it = offsets_hash_map->find(result_block_offset[row]);

            if (offset_it != offsets_hash_map->end())
            {
                patch_to_apply->result_indices.push_back(row);
                patch_to_apply->patch_indices.push_back(offset_it->second);
            }
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
    assertPatchesBlockStructure(patches, result_block);
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
