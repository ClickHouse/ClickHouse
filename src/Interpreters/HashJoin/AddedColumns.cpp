#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/fillJoinOutputColumns.h>
#include <DataTypes/NullableUtils.h>

namespace DB
{

JoinOnKeyColumns::JoinOnKeyColumns(
    const ScatteredBlock & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_,
    bool keep_lowcardinality)
    : key_names(key_names_)
    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    /// Exception: single-LowCardinality-column joins keep the dictionary so the key getter can use it.
    , materialized_keys_holder(keep_lowcardinality
          ? JoinCommon::materializeColumnsKeepLowCardinality(block.getSourceBlock(), key_names)
          : JoinCommon::materializeColumns(block.getSourceBlock(), key_names))
    , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
    , null_map(nullptr)
    , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
    , join_mask_column(JoinCommon::getColumnAsMask(block.getSourceBlock(), cond_column_name))
    , key_sizes(key_sizes_)
{
}

template<typename F>
void LazyOutput::dispatchOutputs(F && f) const
{
    if (!has_row_store)
        f.template operator()<false, true>();
    else if (!has_columns)
        f.template operator()<true, false>();
    else
        f.template operator()<true, true>();
}

namespace
{

/// Core of JoinOnKeyColumns::buildRowSkipData; `fill_selected(set_at)` applies `set_at`
/// to every row position the caller is going to probe.
template <typename FillSelected>
const UInt8 * buildRowSkipDataImpl(
    ConstNullMapPtr null_map, const JoinCommon::JoinMask & mask, IColumn::Filter & buffer, FillSelected && fill_selected)
{
    const UInt8 * null_map_data = null_map ? null_map->data() : nullptr;
    const auto mask_kind = mask.getKind();
    if (mask_kind == JoinCommon::JoinMask::Kind::AllTrue)
        return null_map_data;

    const size_t mask_size = mask.getSize();
    chassert(!null_map || null_map->size() == mask_size);
    buffer.resize(mask_size);
    if (mask_kind == JoinCommon::JoinMask::Kind::AllFalse)
    {
        fill_selected([&](size_t i) { buffer[i] = 1; });
    }
    else
    {
        const UInt8 * mask_data = mask.getRawDataOrNull();
        /// The mask bytes are only guaranteed to be boolean-like (0 = filtered), not 0/1.
        if (null_map_data)
            fill_selected([&](size_t i) { buffer[i] = null_map_data[i] | static_cast<UInt8>(!mask_data[i]); });
        else
            fill_selected([&](size_t i) { buffer[i] = static_cast<UInt8>(!mask_data[i]); });
    }
    return buffer.data();
}

}

const UInt8 * JoinOnKeyColumns::buildRowSkipData(IColumn::Filter & buffer, size_t range_begin, size_t range_size) const
{
    return buildRowSkipDataImpl(null_map, join_mask_column, buffer, [&](auto && set_at)
    {
        for (size_t i = range_begin; i < range_begin + range_size; ++i)
            set_at(i);
    });
}

const UInt8 * JoinOnKeyColumns::buildRowSkipData(IColumn::Filter & buffer, const ScatteredBlock::Indexes & indexes) const
{
    return buildRowSkipDataImpl(null_map, join_mask_column, buffer, [&](auto && set_at)
    {
        for (size_t i : indexes.getData())
            set_at(i);
    });
}

size_t LazyOutput::buildOutput(
    size_t size_to_reserve,
    const Block & left_block,
    const IColumn::Offsets & left_offsets,
    MutableColumns & columns,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_offset,
    size_t rows_limit,
    size_t bytes_limit) const
{
    if (!output_by_row_list)
        dispatchOutputs([&]<bool from_row_store, bool from_columns>()
        {
            buildOutputFromBlocks<false, from_row_store, from_columns>(size_to_reserve, columns, row_refs_begin, row_refs_end);
        });
    else
    {
        if (rows_limit)
        {
            PaddedPODArray<UInt64> left_sizes;
            if (bytes_limit)
            {
                for (const auto & col : left_block)
                    col.column->collectSerializedValueSizes(left_sizes, nullptr, nullptr);
            }

            size_t added_rows = 0;
            dispatchOutputs([&]<bool from_row_store, bool from_columns>()
            {
                added_rows = buildOutputFromBlocksLimitAndOffset<from_row_store, from_columns>(columns, row_refs_begin, row_refs_end, left_sizes, left_offsets, rows_offset, rows_limit, bytes_limit);
            });
            return added_rows;
        }
        /// `buildOutputFromRowRefLists` reads stored columns directly from raw RowRefList pointers (deep
        /// inside `fillFromRowRefs`) and is not decompression-aware, so route through the blocks path,
        /// which resolves compressed blocks, whenever compression is active.
        if (have_compressed || (!join_data_sorted && join_data_avg_perkey_rows < output_by_row_list_threshold))
            dispatchOutputs([&]<bool from_row_store, bool from_columns>()
            {
                buildOutputFromBlocks<true, from_row_store, from_columns>(size_to_reserve, columns, row_refs_begin, row_refs_end);
            });
        else
            buildOutputFromRowRefLists(size_to_reserve, columns, row_refs_begin, row_refs_end);
    }
    /// Without rows_limit, all possible rows are added and result value is not used.
    return 0;
}

void LazyOutput::buildOutputFromRowRefLists(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    chassert(!has_row_store || !join_data_sorted, "Row store should be disabled when join data rerange optimization is used.");

    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
    {
        const auto & access_index = output_access_indexes[dst_idx];
        auto & col = columns[dst_idx];
        col->reserve(col->size() + size_to_reserve);
        if (access_index.type == ColumnAccessIndex::Type::RowStore)
            col->fillFromRowRefsWithRowStore(type_name[dst_idx].type, access_index.field_offset, access_index.field_size, row_refs_begin, row_refs_end, block_row_stores);
        else
            col->fillFromRowRefs(type_name[dst_idx].type, row_refs_begin, row_refs_end, join_data_sorted, emit_block_columns[access_index.index], emit_block_replicated[access_index.index]);
    }
}

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const StoredBlock * block, size_t row_num, size_t column_index)
{
    if (const auto * replicated_column_from_block = block->replicated_columns[column_index])
        return {replicated_column_from_block->getNestedColumn().get(), replicated_column_from_block->getIndexes().getIndexAt(row_num)};
    return {block->columns[column_index].get(), row_num};
}

/// Copies collected (stored block, row number) pairs into the output columns when the stored blocks
/// are compressed (`enable_join_in_memory_compression`). The pairs are processed grouped by stored
/// block, not in row order: each distinct block is decompressed exactly once per flush, and the
/// decompressed working set is released between groups once it outgrows the resolver's budget -
/// groups are disjoint, so nothing released is ever referenced again. Copying in row order instead
/// would re-decompress on every block switch when the rows alternate between blocks that do not fit
/// the budget together (e.g. a build side of a few compressed blocks tens of MiB each, probed by
/// keys that jump between them), degrading the probe to O(rows * block size). When the rows already
/// reference the blocks in group order (the common case: the build side is probed in insert order),
/// they are copied into the output directly; otherwise they are copied group-major into scratch
/// columns and restored to the original row order with a permutation.
/// In-memory compression and the row store are mutually exclusive (see the HashJoin constructor), so
/// every entry of `output_access_indexes` here is a plain columnar position.
static void fillColumnsFromPairsGroupedByBlock(
    DecompressResolver & resolve,
    MutableColumns & columns,
    const ColumnsWithRowNumbers & pairs,
    const NamesAndTypes & type_name,
    const ColumnAccessIndexes & output_access_indexes)
{
    const size_t num_rows = pairs.columns.size();
    chassert(pairs.row_numbers.size() == num_rows);
    if (num_rows == 0)
        return;

    /// Group ordinals in first-appearance order; non-matched rows (nullptr) form a group too.
    std::unordered_map<const StoredBlock *, UInt32> ordinal_of_block;
    PaddedPODArray<UInt32> ordinals(num_rows);
    bool rows_are_in_group_order = true;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const UInt32 next_ordinal = static_cast<UInt32>(ordinal_of_block.size());
        const UInt32 ordinal = ordinal_of_block.try_emplace(pairs.columns[i], next_ordinal).first->second;
        rows_are_in_group_order = rows_are_in_group_order && (i == 0 || ordinal >= ordinals[i - 1]);
        ordinals[i] = ordinal;
    }
    const size_t num_groups = ordinal_of_block.size();

    /// Copies runs of consecutive equal stored pointers from `grouped` into `to`, resolving each
    /// run's block once and releasing the working set between runs when it is over budget (safe:
    /// the rows of the previous runs are fully copied out, and no block appears in two runs).
    const auto copy_grouped_runs = [&](const ColumnsWithRowNumbers & grouped, MutableColumns & to)
    {
        ColumnsWithRowNumbers run;
        size_t begin = 0;
        while (begin < num_rows)
        {
            const StoredBlock * stored = grouped.columns[begin];
            size_t end = begin + 1;
            while (end < num_rows && grouped.columns[end] == stored)
                ++end;
            if (resolve.needReleaseBefore(stored))
                resolve.release(/*forced_by_budget=*/ true);
            const StoredBlock * block = resolve(stored);
            run.columns.assign(end - begin, block);
            run.row_numbers.assign(grouped.row_numbers.begin() + begin, grouped.row_numbers.begin() + end);
            run.has_defaults = pairs.has_defaults;
            for (size_t i = 0; i < to.size(); ++i)
                to[i]->fillFromBlocksAndRowNumbers(type_name[i].type, output_access_indexes[i].index, run);
            begin = end;
        }
    };

    if (rows_are_in_group_order || num_groups == 1)
    {
        copy_grouped_runs(pairs, columns);
        return;
    }

    /// Counting sort of the rows by group, stable within a group: `rank[i]` is the position of
    /// row `i` in the group-major order.
    std::vector<size_t> group_begin(num_groups + 1, 0);
    for (size_t i = 0; i < num_rows; ++i)
        ++group_begin[ordinals[i] + 1];
    for (size_t g = 1; g <= num_groups; ++g)
        group_begin[g] += group_begin[g - 1];
    IColumn::Permutation rank(num_rows);
    {
        std::vector<size_t> cursor(group_begin.begin(), group_begin.end() - 1);
        for (size_t i = 0; i < num_rows; ++i)
            rank[i] = cursor[ordinals[i]]++;
    }

    /// Scatter the pairs into group-major order. The stored pointers stay unresolved here:
    /// resolution happens run by run in copy_grouped_runs, after the previous runs' rows are
    /// already copied out, so a release between runs invalidates nothing that is still needed.
    ColumnsWithRowNumbers sorted;
    sorted.has_defaults = pairs.has_defaults;
    sorted.columns.resize(num_rows);
    sorted.row_numbers.resize(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        sorted.columns[rank[i]] = pairs.columns[i];
        sorted.row_numbers[rank[i]] = pairs.row_numbers[i];
    }

    MutableColumns scratch;
    scratch.reserve(columns.size());
    for (const auto & col : columns)
    {
        scratch.push_back(col->cloneEmpty());
        scratch.back()->reserve(num_rows);
    }

    copy_grouped_runs(sorted, scratch);

    /// Restore the original row order and append to the output.
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto restored = scratch[i]->permute(rank, 0);
        columns[i]->insertRangeFrom(*restored, 0, num_rows);
    }
}

void LazyOutput::buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    /// Rows in the outer loop (not columns) so that all reads from a resolved block happen before
    /// the next row: the decompressed working set can then be released as soon as it grows past
    /// the resolver's budget. `joinGet` returns a single column, so the loop order does not matter
    /// for cache efficiency.
    DecompressResolver resolve(*join);
    for (auto & col : columns)
        col->reserve(col->size() + size_to_reserve);
    for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (!*row_ref_i)
        {
            for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
                type_name[dst_idx].type->insertDefaultInto(*columns[dst_idx]);
            continue;
        }
        chassert(refWordIsInline(*row_ref_i));
        const StoredBlock * stored = stored_columns[refWordBlockNo(*row_ref_i)];
        /// All previous rows are fully copied out, so the working set can be dropped right away.
        if (resolve.needReleaseBefore(stored))
            resolve.release(/*forced_by_budget=*/ true);
        const auto * block = resolve(stored);
        for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
        {
            const auto & access_index = output_access_indexes[dst_idx];
            chassert(access_index.type != ColumnAccessIndex::Type::RowStore);

            auto & col = columns[dst_idx];
            const auto [column_from_block, row_num] = getBlockColumnAndRow(block, refWordRowNo(*row_ref_i), access_index.index);
            if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get()); nullable_col && !column_from_block->isNullable())
                nullable_col->insertFromNotNullable(*column_from_block, row_num);
            else
                col->insertFrom(*column_from_block, row_num);
        }
    }
}

/// Returns how many rows were added to columns, up to rows_limit
template<bool from_row_store, bool from_columns>
size_t LazyOutput::buildOutputFromBlocksLimitAndOffset(
    MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
    const PaddedPODArray<UInt64> & left_sizes, const IColumn::Offsets & left_offsets,
    size_t rows_offset, size_t rows_limit, size_t bytes_limit) const
{
    if (columns.empty())
        return rows_limit;

    ColumnsWithRowNumbers columns_with_row_numbers;
    [[maybe_unused]] auto & many_columns = columns_with_row_numbers.columns;
    [[maybe_unused]] auto & row_nums = columns_with_row_numbers.row_numbers;
    if constexpr (from_columns)
    {
        many_columns.reserve(rows_limit);
        row_nums.reserve(rows_limit);
    }

    [[maybe_unused]] RowStorePointers row_store_ptrs;
    [[maybe_unused]] std::optional<size_t> row_store_batch_size;
    if constexpr (from_row_store)
        row_store_ptrs.ptrs.reserve(rows_limit);

    size_t added_rows = 0;
    size_t row_idx = 0;
    size_t total_byte_size = 0;
    size_t left_idx = 0; /// position in non-replicated left block
    DecompressResolver resolve(*join);

    /// The bytes-limit accounting below needs each matched row's decompressed sizes, so with a
    /// bytes limit compressed blocks are resolved inline, in row order (the resolver's thrash
    /// fallback bounds the worst case). Without one, the pairs are collected unresolved and the
    /// flush copies them grouped by block (fillColumnsFromPairsGroupedByBlock).
    const bool inline_resolve = resolve.active && bytes_limit != 0;

    /// Copy the pairs collected so far into the output columns and drop them, so the decompressed
    /// blocks they reference can be released when the resolver's working set grows past its budget.
    auto fill_columns = [&]
    {
        if (resolve.active && !inline_resolve)
        {
            /// Compression and the row store are mutually exclusive, so a compressed join has no
            /// row-store pointers to flush here.
            fillColumnsFromPairsGroupedByBlock(resolve, columns, columns_with_row_numbers, type_name, output_access_indexes);
        }
        else
        {
            fillJoinOutputColumns(
                columns, output_access_indexes, row_store_ptrs, row_store_batch_size, columns_with_row_numbers, type_name);
        }

        if constexpr (from_columns)
        {
            many_columns.clear();
            row_nums.clear();
            columns_with_row_numbers.has_defaults = false;
        }
        if constexpr (from_row_store)
        {
            row_store_ptrs.ptrs.clear();
            row_store_ptrs.has_defaults = false;
        }
    };

    for (const UInt64 * row_ref_i = row_refs_begin; rows_limit > 0 && row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (*row_ref_i)
        {
            for (const UInt64 ref_word : refsOf(*row_ref_i))
            {
                if (rows_limit == 0)
                    break;

                if (row_idx < rows_offset)
                {
                    ++row_idx;
                    continue;
                }

                const UInt32 block_no = refWordBlockNo(ref_word);
                const size_t row_num = refWordRowNo(ref_word);

                [[maybe_unused]] const StoredBlock * block = nullptr;
                [[maybe_unused]] const RowDataStore * row_store = nullptr;
                if constexpr (from_columns)
                {
                    block = stored_columns[block_no];
                    if (inline_resolve)
                    {
                        if (resolve.needReleaseBefore(block))
                        {
                            fill_columns();
                            resolve.release(/*forced_by_budget=*/ true);
                        }
                        block = resolve(block);
                    }
                }
                if constexpr (from_row_store)
                    row_store = block_row_stores[block_no];

                if (bytes_limit)
                {
                    /// Check if we are still in the same left row or moved to next one
                    while (row_idx >= left_offsets[left_idx])
                        ++left_idx;
                    chassert(left_sizes.size() > left_idx);
                    total_byte_size += left_sizes[left_idx];

                    /// Add size of right matched rows
                    if constexpr (from_row_store)
                        total_byte_size += row_store->byteSizeAt(row_num);
                    if constexpr (from_columns)
                        for (const auto & col : block->columns)
                            total_byte_size += col->byteSizeAt(row_num);
                }

                ++row_idx;
                --rows_limit;
                ++added_rows;
                if constexpr (from_columns)
                {
                    many_columns.emplace_back(block);
                    row_nums.emplace_back(static_cast<UInt32>(row_num));
                }
                if constexpr (from_row_store)
                {
                    row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row_num));
                    if (!row_store_batch_size)
                        row_store_batch_size = row_store->getBatchSize();
                }

                if (bytes_limit && total_byte_size > bytes_limit)
                    rows_limit = 0;
            }
        }
        else
        {
            if (row_idx < rows_offset)
            {
                ++row_idx;
                continue;
            }
            if constexpr (from_columns)
            {
                many_columns.emplace_back(nullptr);
                row_nums.emplace_back(0);
                columns_with_row_numbers.has_defaults = true;
            }
            if constexpr (from_row_store)
            {
                row_store_ptrs.ptrs.emplace_back(nullptr);
                row_store_ptrs.has_defaults = true;
            }
            ++row_idx;
            --rows_limit;
            ++added_rows;
            /// Here we do not account byte size, since limit targets to avoid only huge blocks with large strings being replicated many times.
            /// In case of non-matched rows, left row is added only once and right columns are filled with defaults which have fixed small size.
        }
    }

    fill_columns();
    return added_rows;
}

template<bool from_row_list, bool from_row_store, bool from_columns>
void LazyOutput::buildOutputFromBlocks(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if (columns.empty())
        return;

    ColumnsWithRowNumbers columns_with_row_numbers;
    [[maybe_unused]] auto & many_columns = columns_with_row_numbers.columns;
    [[maybe_unused]] auto & row_nums = columns_with_row_numbers.row_numbers;
    if constexpr (from_columns)
    {
        many_columns.reserve(size_to_reserve);
        row_nums.reserve(size_to_reserve);
    }

    [[maybe_unused]] RowStorePointers row_store_ptrs;
    [[maybe_unused]] std::optional<size_t> row_store_batch_size;
    if constexpr (from_row_store)
        row_store_ptrs.ptrs.reserve(size_to_reserve);

    /// The (stored block, row number) pairs are only collected here; nothing is decompressed.
    /// When the stored blocks are compressed, the flush below copies them grouped by block
    /// (fillColumnsFromPairsGroupedByBlock), which both bounds the decompressed working set and
    /// never decompresses one block twice.
    auto collect = [&](const UInt64 row_ref_i)
    {
        if constexpr (from_columns)
        {
            many_columns.emplace_back(stored_columns[refWordBlockNo(row_ref_i)]);
            row_nums.emplace_back(refWordRowNo(row_ref_i));
        }
        if constexpr (from_row_store)
        {
            const auto & row_store = block_row_stores[refWordBlockNo(row_ref_i)];
            row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(refWordRowNo(row_ref_i)));
            if (!row_store_batch_size)
                row_store_batch_size = row_store->getBatchSize();
        }
    };

    auto collect_null = [&]()
    {
        if constexpr (from_columns)
        {
            many_columns.emplace_back(nullptr);
            row_nums.emplace_back(0);
            columns_with_row_numbers.has_defaults = true;
        }
        if constexpr (from_row_store)
        {
            row_store_ptrs.ptrs.emplace_back(nullptr);
            row_store_ptrs.has_defaults = true;
        }
    };

    for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (!*row_ref_i)
        {
            collect_null();
            continue;
        }

        if constexpr (from_row_list)
        {
            for (const UInt64 ref_word : refsOf(*row_ref_i))
                collect(ref_word);
        }
        else
        {
            /// A single inline ref word (a unique-key match or an ASOF match).
            chassert(refWordIsInline(*row_ref_i));
            collect(*row_ref_i);
        }
    }

    DecompressResolver resolve(*join);
    if (resolve.active)
    {
        /// Compression and the row store are mutually exclusive, so there are no row-store pointers.
        fillColumnsFromPairsGroupedByBlock(resolve, columns, columns_with_row_numbers, type_name, output_access_indexes);
        return;
    }

    fillJoinOutputColumns(columns, output_access_indexes, row_store_ptrs, row_store_batch_size, columns_with_row_numbers, type_name);
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = lazy_output.type_name.size(); j < size; ++j)
            JoinCommon::addDefaultValues(*columns[j], lazy_output.type_name[j].type, lazy_defaults_count);
        lazy_defaults_count = 0;
    }
}

template<>
void AddedColumns<true>::applyLazyDefaults() {}

/// Materializes one right-table row into the output columns (non-lazy mode and joinGet).
template <>
void AddedColumns<false>::appendFromBlock(UInt64 ref_word, const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

    chassert(refWordIsInline(ref_word));
    /// When the join compressed its stored blocks, `decompress_resolver` decompresses this block
    /// before reading (deduplicated per distinct block across the `appendFromBlock` calls of this
    /// batch, with the working set released early once it grows past the resolver's budget - the
    /// rows of the previous calls are fully copied out already, so nothing dangles).
    const StoredBlock * stored = lazy_output.stored_columns[refWordBlockNo(ref_word)];
    if (decompress_resolver.needReleaseBefore(stored))
        decompress_resolver.release(/*forced_by_budget=*/ true);
    const StoredBlock * block = decompress_resolver(stored);
    const size_t row_num = refWordRowNo(ref_word);
#ifndef NDEBUG
    checkColumns(*block);
#endif

    if (is_join_get)
    {
        for (size_t dst_idx = 0; dst_idx < lazy_output.output_access_indexes.size(); ++dst_idx)
        {
            const auto & access_index = lazy_output.output_access_indexes[dst_idx];
            chassert(access_index.type == ColumnAccessIndex::Type::Columns);

            const auto [column_from_block, src_row_num] = getBlockColumnAndRow(block, row_num, access_index.index);
            if (auto * nullable_col = nullable_column_ptrs[dst_idx])
                nullable_col->insertFromNotNullable(*column_from_block, src_row_num);
            else
                columns[dst_idx]->insertFrom(*column_from_block, src_row_num);
        }
    }
    else
    {
        for (size_t dst_idx = 0; dst_idx < lazy_output.output_access_indexes.size(); ++dst_idx)
        {
            const auto & access_index = lazy_output.output_access_indexes[dst_idx];
            chassert(access_index.type == ColumnAccessIndex::Type::Columns);

            const auto [column_from_block, src_row_num] = getBlockColumnAndRow(block, row_num, access_index.index);
            columns[dst_idx]->insertFrom(*column_from_block, src_row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(UInt64 ref_word, bool)
{
#ifndef NDEBUG
    /// `ref_word` may be an inline single ref or a list word (pointer + count); firstWord yields
    /// the head ref of either, whose block is valid for the column-structure assertion.
    checkColumns(*lazy_output.stored_columns[refWordBlockNo(RowRefList::fromWord(ref_word).firstWord())]);
#endif
    if (record_row_refs)
    {
        lazy_output.addRef(ref_word);
    }
}

}
