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

    /// Empty for joinGet, which emits through `buildJoinGetOutput` instead.
    chassert(!emit_gather.empty());
    gatherJoinOutputColumns(columns, emit_gather, selection, row_store_row_length);

    /// Without rows_limit, all possible rows are added and result value is not used.
    return 0;
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
                        const StoredBlock * block = stored_columns[refWordBlockNo(ref_word)];
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

    /// Every selected word is inline or zero by construction, which is the flat shape.
    const RefWordSelection selection{
        .begin = selected_words.data(),
        .end = selected_words.data() + selected_words.size(),
        .rows = selected_words.size(),
        .shape = RefWordShape::Flat};
    gatherJoinOutputColumns(columns, emit_gather, selection, row_store_row_length);
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
