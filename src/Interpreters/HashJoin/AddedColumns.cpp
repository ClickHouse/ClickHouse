#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/fillRowStoreOutputColumns.h>
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
        if (!join_data_sorted && join_data_avg_perkey_rows < output_by_row_list_threshold)
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
    /// A join that emits no right column still records refs when `EXPLAIN ANALYZE matches = 1` asks
    /// for an exact match count, and then there is nothing to emit from them.
    if (columns.empty())
        return;

    chassert(!has_row_store || !join_data_sorted, "Row store should be disabled when join data rerange optimization is used.");

    /// The reranged build side is the one producer of the range shape.
    const RefWordShape shape = join_data_sorted ? RefWordShape::Ranges : RefWordShape::Lists;
    emitColumnarOutputs(
        columns,
        RefWordSelection{
            .begin = row_refs_begin,
            .end = row_refs_end,
            .rows = countRefWordRows({row_refs_begin, row_refs_end}, shape),
            .shape = shape});

    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
    {
        const auto & access_index = output_access_indexes[dst_idx];
        if (access_index.type != ColumnAccessIndex::Type::RowStore)
            continue;
        auto & col = columns[dst_idx];
        col->reserve(col->size() + size_to_reserve);
        col->fillFromRowRefsWithRowStore(type_name[dst_idx].type, access_index.field_offset, access_index.field_size, row_refs_begin, row_refs_end, block_row_stores);
    }
}

void LazyOutput::emitColumnarOutputs(MutableColumns & columns, const RefWordSelection & selection) const
{
    /// Header derivation (`JoiningTransform::transformHeader`) runs the join over an empty block, and
    /// reaches here with nothing recorded and nothing to append.
    if (selection.begin == selection.end)
        return;

    /// Empty for joinGet, which emits through `buildJoinGetOutput` instead.
    chassert(!emit_gather.empty());
    EmitScratch scratch;

    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
        if (output_access_indexes[dst_idx].type == ColumnAccessIndex::Type::Columns)
            gatherColumn(*columns[dst_idx], emit_gather[dst_idx], selection, scratch);
}

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const StoredBlock * block, size_t row_num, size_t column_index)
{
    if (const auto * replicated_column_from_block = block->replicated_columns[column_index])
        return {replicated_column_from_block->getNestedColumn().get(), replicated_column_from_block->getIndexes().getIndexAt(row_num)};
    return {block->columns[column_index].get(), row_num};
}

void LazyOutput::buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
    {
        const auto & access_index = output_access_indexes[dst_idx];
        chassert(access_index.type != ColumnAccessIndex::Type::RowStore);

        auto & col = columns[dst_idx];
        col->reserve(col->size() + size_to_reserve);
        for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
        {
            if (!*row_ref_i)
            {
                type_name[dst_idx].type->insertDefaultInto(*col);
                continue;
            }
            chassert(refWordIsInline(*row_ref_i));
            const auto * block = stored_columns[refWordBlockNo(*row_ref_i)];
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

    /// The words this walk selects, cut by the row and byte limits, are the emit input for every
    /// columnar column, so it always records them.
    [[maybe_unused]] PaddedPODArray<UInt64> selected_words;
    if constexpr (from_columns)
        selected_words.reserve(rows_limit);

    [[maybe_unused]] RowStorePointers row_store_ptrs;
    [[maybe_unused]] std::optional<size_t> row_store_batch_size;
    if constexpr (from_row_store)
        row_store_ptrs.ptrs.reserve(rows_limit);

    size_t added_rows = 0;
    size_t row_idx = 0;
    size_t total_byte_size = 0;
    size_t left_idx = 0; /// position in non-replicated left block
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
                    block = stored_columns[block_no];
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
                    selected_words.push_back(ref_word);
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
                selected_words.push_back(0);
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

    if constexpr (from_columns)
    {
        /// Every selected word is inline or zero by construction, which is the flat shape.
        const RefWordSelection selection{
            .begin = selected_words.data(),
            .end = selected_words.data() + selected_words.size(),
            .rows = selected_words.size(),
            .shape = RefWordShape::Flat};
        emitColumnarOutputs(columns, selection);
    }

    fillRowStoreOutputColumns(columns, output_access_indexes, row_store_ptrs, row_store_batch_size, type_name);
    return added_rows;
}

template<bool from_row_list, bool from_row_store, bool from_columns>
void LazyOutput::buildOutputFromBlocks(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if (columns.empty())
        return;

    constexpr RefWordShape shape = from_row_list ? RefWordShape::Lists : RefWordShape::Flat;
    const RefWordSelection selection{
        .begin = row_refs_begin,
        .end = row_refs_end,
        .rows = countRefWordRows({row_refs_begin, row_refs_end}, shape),
        .shape = shape};
    chassert(selection.rows <= size_to_reserve);

    if constexpr (from_columns)
        emitColumnarOutputs(columns, selection);

    if constexpr (from_row_store)
    {
        /// The row store is not addressed by ref words: it needs the resolved row pointer.
        RowStorePointers row_store_ptrs;
        std::optional<size_t> row_store_batch_size;
        row_store_ptrs.ptrs.reserve(selection.rows);

        auto collect = [&](const UInt64 ref_word)
        {
            const auto & row_store = block_row_stores[refWordBlockNo(ref_word)];
            row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(refWordRowNo(ref_word)));
            if (!row_store_batch_size)
                row_store_batch_size = row_store->getBatchSize();
        };

        for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
        {
            if (!*row_ref_i)
            {
                row_store_ptrs.ptrs.emplace_back(nullptr);
                row_store_ptrs.has_defaults = true;
            }
            else if constexpr (from_row_list)
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

        fillRowStoreOutputColumns(columns, output_access_indexes, row_store_ptrs, row_store_batch_size, type_name);
    }
}

void AddedColumns::appendFromBlock(UInt64 ref_word)
{
#ifndef NDEBUG
    /// `ref_word` may be an inline single ref or a list word (pointer + count); firstWord yields
    /// the head ref of either, whose block is valid for the column-structure assertion.
    checkColumns(*lazy_output.stored_columns[refWordBlockNo(RowRefList::fromWord(ref_word).firstWord())]);
#endif
    if (record_row_refs)
        lazy_output.addRef(ref_word);
}

}
