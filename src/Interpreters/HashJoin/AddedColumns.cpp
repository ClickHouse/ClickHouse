#include <Interpreters/HashJoin/AddedColumns.h>
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

void LazyOutput::addRef(UInt64 ref_word)
{
    chassert(ref_word != 0);
    row_refs.emplace_back(ref_word);
    const auto rows = refWordRows(ref_word);
    row_count += rows;
    hash_table_matches += rows;
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
    if (output_by_row_list && rows_limit)
    {
        PaddedPODArray<UInt64> left_sizes;
        if (bytes_limit)
        {
            for (const auto & col : left_block)
                col.column->collectSerializedValueSizes(left_sizes, nullptr, nullptr);
        }

        return buildOutputFromBlocksLimitAndOffset(
            columns, row_refs_begin, row_refs_end, left_sizes, left_offsets, rows_offset, rows_limit, bytes_limit);
    }

    /// A join that emits no right column still records refs when `EXPLAIN ANALYZE matches = 1` asks
    /// for an exact match count, and then there is nothing to emit from them.
    if (columns.empty())
        return 0;

    /// Without row lists every word is one inline ref. With them, the reranged build side is the
    /// one producer of the range shape.
    const RefWordShape shape = !output_by_row_list ? RefWordShape::Flat : join_data_sorted ? RefWordShape::Ranges : RefWordShape::Lists;
    const RefWordSelection selection{
        .begin = row_refs_begin, .end = row_refs_end, .rows = countRefWordRows({row_refs_begin, row_refs_end}, shape), .shape = shape};
    chassert(selection.rows <= size_to_reserve);

    emitColumnarOutputs(columns, selection);

    /// Without rows_limit, all possible rows are added and result value is not used.
    return 0;
}

void LazyOutput::emitColumnarOutputs(MutableColumns & columns, const RefWordSelection & selection) const
{
    /// Header derivation (`JoiningTransform::transformHeader`) runs the join over an empty block, and
    /// reaches here with nothing recorded and nothing to append.
    if (selection.begin == selection.end)
        return;

    /// The emit table holds raw planes of the stored columns, which are `ColumnCompressed`
    /// placeholders once the join compressed them, so it is not resolved then (see the `AddedColumns`
    /// constructor): read those blocks through their decompressed views instead. In-memory compression
    /// and the row store are mutually exclusive (see the HashJoin constructor).
    if (have_compressed)
    {
        DecompressResolver resolve(*join);
        emitCompressedColumnarOutputs(resolve, stored_columns, columns, output_access_indexes, type_name, selection);
        return;
    }

    /// Empty for joinGet, which emits through `buildJoinGetOutput` instead.
    chassert(!emit_gather.empty());
    gatherJoinOutputColumns(columns, emit_gather, selection, row_store_row_length);
}

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const StoredBlock * block, size_t row_num, size_t column_index)
{
    if (const auto * replicated_column_from_block = block->replicated_columns[column_index])
        return {replicated_column_from_block->getNestedColumn().get(), replicated_column_from_block->getIndexes().getIndexAt(row_num)};
    return {block->columns[column_index].get(), row_num};
}

namespace
{

/// (stored block, row number) pairs of one emit; a nullptr block is a default row.
struct StoredRowPairs
{
    PaddedPODArray<const StoredBlock *> blocks;
    PaddedPODArray<UInt32> row_numbers;
};

/// Appends `pairs` to the columnar outputs, reading each row from the (already resolved) block.
void insertStoredRowPairs(
    MutableColumns & to,
    const StoredRowPairs & pairs,
    size_t begin,
    size_t end,
    const NamesAndTypes & type_name,
    const ColumnAccessIndexes & output_access_indexes)
{
    for (size_t dst_idx = 0; dst_idx < to.size(); ++dst_idx)
    {
        const auto & access_index = output_access_indexes[dst_idx];
        if (access_index.type != ColumnAccessIndex::Type::Columns)
            continue;

        auto & col = to[dst_idx];
        col->reserve(col->size() + (end - begin));
        for (size_t i = begin; i < end; ++i)
        {
            const StoredBlock * block = pairs.blocks[i];
            if (!block)
            {
                type_name[dst_idx].type->insertDefaultInto(*col);
                continue;
            }
            const auto [column_from_block, row_num] = getBlockColumnAndRow(block, pairs.row_numbers[i], access_index.index);
            col->insertFrom(*column_from_block, row_num);
        }
    }
}

}

/// Copies the rows of `selection` into the columnar output columns when the stored blocks are
/// compressed (`enable_join_in_memory_compression`). The rows are processed grouped by stored
/// block, not in row order: each distinct block is decompressed exactly once per call, and the
/// decompressed working set is released between groups once it outgrows the resolver's budget -
/// groups are disjoint, so nothing released is ever referenced again. Copying in row order instead
/// would re-decompress on every block switch when the rows alternate between blocks that do not fit
/// the budget together (e.g. a build side of a few compressed blocks tens of MiB each, probed by
/// keys that jump between them), degrading the probe to O(rows * block size). When the rows already
/// reference the blocks in group order (the common case: the build side is probed in insert order),
/// they are copied into the output directly; otherwise they are copied group-major into scratch
/// columns and restored to the original row order with a permutation.
/// In-memory compression and the row store are mutually exclusive (see the HashJoin constructor), so
/// only the columnar entries of `output_access_indexes` are filled here.
void emitCompressedColumnarOutputs(
    DecompressResolver & resolve,
    const StoredBlock * const * stored_columns,
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const NamesAndTypes & type_name,
    const RefWordSelection & selection)
{
    StoredRowPairs pairs;
    pairs.blocks.reserve(selection.rows);
    pairs.row_numbers.reserve(selection.rows);
    for (const UInt64 * word = selection.begin; word != selection.end; ++word)
    {
        if (!*word)
        {
            pairs.blocks.push_back(nullptr);
            pairs.row_numbers.push_back(0);
            continue;
        }
        for (const UInt64 ref_word : refsOf(*word))
        {
            pairs.blocks.push_back(stored_columns[refWordBlockNo(ref_word)]);
            pairs.row_numbers.push_back(refWordRowNo(ref_word));
        }
    }

    const size_t num_rows = pairs.blocks.size();
    if (num_rows == 0)
        return;

    /// Group ordinals in first-appearance order; non-matched rows (nullptr) form a group too.
    std::unordered_map<const StoredBlock *, UInt32> ordinal_of_block;
    PaddedPODArray<UInt32> ordinals(num_rows);
    bool rows_are_in_group_order = true;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const UInt32 next_ordinal = static_cast<UInt32>(ordinal_of_block.size());
        const UInt32 ordinal = ordinal_of_block.try_emplace(pairs.blocks[i], next_ordinal).first->second;
        rows_are_in_group_order = rows_are_in_group_order && (i == 0 || ordinal >= ordinals[i - 1]);
        ordinals[i] = ordinal;
    }
    const size_t num_groups = ordinal_of_block.size();

    /// Copies runs of consecutive equal stored pointers from `grouped` into `to`, resolving each
    /// run's block once and releasing the working set between runs when it is over budget (safe:
    /// the rows of the previous runs are fully copied out, and no block appears in two runs).
    const auto copy_grouped_runs = [&](const StoredRowPairs & grouped, MutableColumns & to)
    {
        StoredRowPairs run;
        size_t begin = 0;
        while (begin < num_rows)
        {
            const StoredBlock * stored = grouped.blocks[begin];
            size_t end = begin + 1;
            while (end < num_rows && grouped.blocks[end] == stored)
                ++end;
            if (resolve.needReleaseBefore(stored))
                resolve.release(/*forced_by_budget=*/ true);
            const StoredBlock * block = resolve(stored);
            run.blocks.assign(end - begin, block);
            run.row_numbers.assign(grouped.row_numbers.begin() + begin, grouped.row_numbers.begin() + end);
            insertStoredRowPairs(to, run, 0, end - begin, type_name, output_access_indexes);
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
    StoredRowPairs sorted;
    sorted.blocks.resize(num_rows);
    sorted.row_numbers.resize(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        sorted.blocks[rank[i]] = pairs.blocks[i];
        sorted.row_numbers[rank[i]] = pairs.row_numbers[i];
    }

    MutableColumns scratch;
    scratch.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        scratch.push_back(columns[i]->cloneEmpty());
        if (output_access_indexes[i].type == ColumnAccessIndex::Type::Columns)
            scratch.back()->reserve(num_rows);
    }

    copy_grouped_runs(sorted, scratch);

    /// Restore the original row order and append to the output.
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (output_access_indexes[i].type != ColumnAccessIndex::Type::Columns)
            continue;
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
size_t LazyOutput::buildOutputFromBlocksLimitAndOffset(
    MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
    const PaddedPODArray<UInt64> & left_sizes, const IColumn::Offsets & left_offsets,
    size_t rows_offset, size_t rows_limit, size_t bytes_limit) const
{
    if (columns.empty())
        return rows_limit;

    /// The words this walk selects, cut by the row and byte limits, are the emit input for every
    /// output column.
    PaddedPODArray<UInt64> selected_words;
    selected_words.reserve(rows_limit);

    size_t added_rows = 0;
    size_t row_idx = 0;
    size_t total_byte_size = 0;
    size_t left_idx = 0; /// position in non-replicated left block
    DecompressResolver resolve(*join);

    /// The bytes-limit accounting below needs each matched row's decompressed sizes, so with a
    /// bytes limit compressed blocks are resolved inline, in row order (the resolver's thrash
    /// fallback bounds the worst case). Without one, the words are collected unresolved and the
    /// emit copies them grouped by block (emitCompressedColumnarOutputs).
    const bool inline_resolve = resolve.active && bytes_limit != 0;

    /// Emit the columnar words selected so far, reading compressed blocks through `resolve`.
    auto emit_selected = [&]
    {
        /// Every selected word is inline or zero by construction, which is the flat shape.
        const RefWordSelection selection{
            .begin = selected_words.data(),
            .end = selected_words.data() + selected_words.size(),
            .rows = selected_words.size(),
            .shape = RefWordShape::Flat};
        if (resolve.active)
            emitCompressedColumnarOutputs(resolve, stored_columns, columns, output_access_indexes, type_name, selection);
        else
            emitColumnarOutputs(columns, selection);
        selected_words.clear();
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

                /// With a bytes limit, a compressed block is resolved inline so that the accounting below
                /// sees its decompressed sizes.
                const StoredBlock * block = nullptr;
                if (has_columns)
                {
                    block = stored_columns[refWordBlockNo(ref_word)];
                    if (inline_resolve)
                    {
                        if (resolve.needReleaseBefore(block))
                        {
                            emit_selected();
                            resolve.release(/*forced_by_budget=*/ true);
                        }
                        block = resolve(block);
                    }
                }

                if (bytes_limit)
                {
                    /// Check if we are still in the same left row or moved to next one
                    while (row_idx >= left_offsets[left_idx])
                        ++left_idx;
                    chassert(left_sizes.size() > left_idx);
                    total_byte_size += left_sizes[left_idx];

                    if (row_store_row_length)
                        total_byte_size += *row_store_row_length;

                    if (has_columns)
                    {
                        const size_t row_num = refWordRowNo(ref_word);
                        for (const auto & col : block->columns)
                            total_byte_size += col->byteSizeAt(row_num);
                    }
                }

                ++row_idx;
                --rows_limit;
                ++added_rows;
                selected_words.push_back(ref_word);

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
            selected_words.push_back(0);
            ++row_idx;
            --rows_limit;
            ++added_rows;
            /// Here we do not account byte size, since limit targets to avoid only huge blocks with large strings being replicated many times.
            /// In case of non-matched rows, left row is added only once and right columns are filled with defaults which have fixed small size.
        }
    }

    emit_selected();
    return added_rows;
}

EmitPlan
planJoinEmit(const HashJoin::RightTableData & data, std::span<const size_t> positions, const NamesAndTypes & type_name, bool with_gather)
{
    const bool row_store_initialized = data.row_store_state == HashJoin::RowStoreState::Initialized;
    EmitPlan plan;
    plan.access_indexes.reserve(positions.size());
    std::vector<EmitColumnRequest> requests;
    requests.reserve(positions.size());
    for (size_t dst_idx = 0; dst_idx < positions.size(); ++dst_idx)
    {
        const ColumnAccessIndex access_index = row_store_initialized
            ? data.column_access_indexes[positions[dst_idx]]
            : ColumnAccessIndex{ColumnAccessIndex::Type::Columns, positions[dst_idx]};
        plan.access_indexes.push_back(access_index);
        if (access_index.type == ColumnAccessIndex::Type::RowStore)
            plan.row_store_row_length = RowDataStore::rowLengthOf(*data.row_store_layout);
        else
            plan.has_columns = true;
        requests.push_back({positions[dst_idx], access_index, type_name[dst_idx].type});
    }
    if (!with_gather)
        return plan;

    /// The emit table is keyed by saved-block position.
    std::vector<GatherColumn> gather_by_position;
    data.stored_columns_index->resolveEmitColumns(data.sample_block.columns(), requests, gather_by_position);

    plan.gather.assign(positions.size(), {});
    for (size_t dst_idx = 0; dst_idx < positions.size(); ++dst_idx)
        plan.gather[dst_idx] = gather_by_position[positions[dst_idx]];
    return plan;
}

}
