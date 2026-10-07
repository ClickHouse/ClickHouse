#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/TableJoin.h>

#include <span>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class MatchedRowsStats;

struct JoinOnKeyColumns
{
    Names key_names;

    Columns materialized_keys_holder;
    ColumnRawPtrs key_columns;

    ConstNullMapPtr null_map;
    ColumnPtr null_map_holder;

    /// Only rows where mask == true can be joined
    JoinCommon::JoinMask join_mask_column;

    Sizes key_sizes;

    JoinOnKeyColumns(
        const ScatteredBlock & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_,
        bool keep_lowcardinality = false);

    bool isRowFiltered(size_t i) const
    {
        return join_mask_column.isRowFiltered(i);
    }

    /// A row with a NULL key and a row filtered out by the ON-section mask have the same
    /// effect - the row matches nothing - so the probe loop needs a single skip byte per
    /// row instead of two checks. Returns the skip bytes (indexed by source-block row,
    /// 1 = skip), prepared once per call: without an ON-section condition the null map is
    /// returned directly, no copy (nullptr when the keys are not nullable either = no row
    /// is skipped); with one, `buffer` is filled with the merged null-and-mask bytes.
    /// Only the positions the caller will probe are written (continuation chunks and
    /// index selectors visit a subset of the source block); the rest of `buffer` stays
    /// uninitialized and must not be read.
    const UInt8 * buildRowSkipData(IColumn::Filter & buffer, size_t range_begin, size_t range_size) const;
    /// `PartitionedHashJoin::joinRightColumns` calls this overload when its selector holds
    /// row indexes rather than one continuous range.
    const UInt8 * buildRowSkipData(IColumn::Filter & buffer, const ScatteredBlock::Indexes & indexes) const;
};

struct LazyOutput
{
    /// Entries share the encoding of the join hash map cell values (see RowRefs.h):
    ///   0 - default row; bit 63 set - inline encoded RowRef; otherwise - a RowRefList
    ///   list word (pointer to a Batch node in the low 48 bits + row count).
    /// ASOF matches are inline encoded RowRef words too (the leaf of the sorted lookup vector).
    PaddedPODArray<UInt64> row_refs;
    size_t row_count = 0;   /// Total number of rows in all refs and ref lists
    size_t hash_table_matches = 0; /// Total number of hash table matches

    /// Resolves RowRef::block_no at emit time; points into the join's StoredColumnsIndex,
    /// which is immutable once the build phase is finished. Kept beside the per-column emit table
    /// below for the two consumers that need a whole block rather than one resolved column:
    /// `byteSizeAt` accounting and `buildJoinGetOutput`'s nullable dispatch.
    const StoredBlock * const * stored_columns = nullptr;

    /// Per-block row store base pointers (block_no -> RowDataStore*), from StoredColumnsIndex::rowStoresData().
    /// Shared by all row-store columns of a block; a specific column is a field_offset/field_size slice.
    const RowDataStore * const * block_row_stores = nullptr;

    /// Per output column, the source descriptor `gatherColumn` reads. Resolved once per probe block,
    /// as it is a property of the join rather than of an output chunk. Empty for joinGet.
    std::vector<GatherColumn> emit_gather;

    NamesAndTypes type_name;

    bool join_data_sorted = false;
    bool output_by_row_list = false;
    size_t output_by_row_list_threshold = 0;
    size_t join_data_avg_perkey_rows = 0;

    ColumnAccessIndexes output_access_indexes;
    bool has_row_store = false;
    bool has_columns = false;

    const PaddedPODArray<UInt64> & getRowRefs() const { return row_refs; }
    size_t getRowCount() const { return row_count; }

    void reserve(size_t size) { row_refs.reserve(size); }

    /// `ref_word` is either an inline single ref or a RowRefList list word (pointer + count).
    void addRef(UInt64 ref_word)
    {
        chassert(ref_word != 0);
        row_refs.emplace_back(ref_word);
        const auto rows = refWordRows(ref_word);
        row_count += rows;
        hash_table_matches += rows;
    }

    void addDefault()
    {
        row_refs.emplace_back(0);
        ++row_count;
    }

    [[nodiscard]] size_t buildOutput(
        size_t size_to_reserve,
        const Block & left_block,
        const IColumn::Offsets & left_offsets,
        MutableColumns & columns,
        const UInt64 * row_refs_begin,
        const UInt64 * row_refs_end,
        size_t rows_offset,
        size_t rows_limit,
        size_t bytes_limit) const;

    void buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const;

    /// The columnar output columns read the recorded words in whatever shape they have.
    void emitColumnarOutputs(MutableColumns & columns, const RefWordSelection & selection) const;

    /// The row store is not addressed by ref words: it needs every ref resolved to a row pointer.
    /// This resolves them once, into one pointer array that every row-store column reads.
    void fillRowStoreOutputsByPointers(MutableColumns & columns, const RefWordSelection & selection) const;

    /// Each row-store column resolves the refs for itself, so no per-output-row array is kept. The
    /// choice for keys with many rows, where the output outgrows both inputs.
    void fillRowStoreOutputsByRefLists(
        size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const;

    template<bool from_row_store, bool from_columns>
    [[nodiscard]] size_t buildOutputFromBlocksLimitAndOffset(
        MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
        const PaddedPODArray<UInt64> & left_sizes, const IColumn::Offsets & left_offsets,
        size_t rows_offset, size_t rows_limit, size_t bytes_limit) const;

private:
    template<typename F>
    void dispatchOutputs(F && f) const;
};

/// What one emit's output columns read: the access index of each saved-block column in `positions`
/// and, for the columnar ones, the gather source, resolved once per probe. Only the requested
/// positions are built, so `StorageJoin` queries selecting different right-column subsets each get
/// their own columns built rather than reusing another query's table.
struct EmitPlan
{
    ColumnAccessIndexes access_indexes;
    /// Parallel to `access_indexes`; empty for a row-store column, and for every column when not resolved.
    std::vector<GatherColumn> gather;
    bool has_row_store = false;
    bool has_columns = false;
};

/// `type_name` is parallel to `positions`. `with_gather` is false for joinGet, whose output type may
/// wrap the stored one in `Nullable` and which emits row by row through `buildJoinGetOutput`.
EmitPlan
planJoinEmit(const HashJoin::RightTableData & data, std::span<const size_t> positions, const NamesAndTypes & type_name, bool with_gather);

/// Records the probe's matches as encoded ref words. Every strictness records rather than emits:
/// the output columns are built later, by the emit kernels, from the words this collects.
class AddedColumns
{
public:

    AddedColumns(
        const ScatteredBlock & left_block_,
        const Block & block_with_columns_to_add,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        ExpressionActionsPtr additional_filter_expression_,
        const std::vector<std::pair<size_t, size_t>> & additional_filter_required_rhs_pos_,
        bool is_asof_join,
        bool is_join_get_,
        bool record_refs_for_stats)
        : left_block(left_block_.getSourceBlock())
        , join_on_keys(join_on_keys_)
        , additional_filter_expression(additional_filter_expression_)
        , additional_filter_required_rhs_pos(additional_filter_required_rhs_pos_)
        , rows_to_add(left_block_.rows())
        , enable_prefetch(join.enableSoftwarePrefetch())
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        record_row_refs = num_columns_to_add > 0 || record_refs_for_stats;
        lazy_output.reserve(rows_to_add);

        columns.reserve(num_columns_to_add);
        lazy_output.type_name.reserve(num_columns_to_add);

        std::vector<size_t> right_indexes;
        right_indexes.reserve(num_columns_to_add);

        lazy_output.output_by_row_list_threshold = join.getTableJoin().outputByRowListPerkeyRowsThreshold();
        lazy_output.join_data_sorted = join.getJoinedData()->sorted;
        lazy_output.join_data_avg_perkey_rows = join.getJoinedData()->avgPerKeyRows();
        lazy_output.stored_columns = join.getJoinedData()->stored_columns_index->blocksData();
        lazy_output.block_row_stores = join.getJoinedData()->stored_columns_index->rowStoresData();

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!left_block.has(qualified_name))
                addColumn(src_column);
        }

        if (is_asof_join)
        {
            chassert(join_on_keys.size() == 1);
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column);
            left_asof_key = join_on_keys[0].key_columns.back();
        }

        for (auto & tn : lazy_output.type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));

        nullable_column_ptrs.resize(right_indexes.size(), nullptr);
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
            const auto & saved_column = saved_block_sample.getByPosition(right_indexes[j]).column;
            if (columns[j]->isNullable() && !saved_column->isNullable())
                nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
        }

        EmitPlan plan = planJoinEmit(*join.getJoinedData(), right_indexes, lazy_output.type_name, !is_join_get);
        lazy_output.output_access_indexes = std::move(plan.access_indexes);
        lazy_output.emit_gather = std::move(plan.gather);
        lazy_output.has_row_store = plan.has_row_store;
        lazy_output.has_columns = plan.has_columns;
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), lazy_output.type_name[i].type, lazy_output.type_name[i].name);
    }

    /// Encoded RowRef word (inline single ref, including an ASOF match) or a RowRefList
    /// list word (pointer + count).
    void appendFromBlock(UInt64 ref_word);

    void appendDefaultRow()
    {
        if (record_row_refs)
            lazy_output.addDefault();
    }

    const IColumn & leftAsofKey() const { return *left_asof_key; }

    /// `PreSelectedRows` returns false. It keeps one ref per right row for the additional filter.
    static constexpr bool appendsWholeKey() { return true; }

    Block left_block;
    std::vector<JoinOnKeyColumns> join_on_keys;
    ExpressionActionsPtr additional_filter_expression;
    const std::vector<std::pair<size_t, size_t>> & additional_filter_required_rhs_pos;

    size_t max_joined_block_rows = 0;
    size_t rows_to_add;
    bool need_filter = false;
    bool enable_prefetch = true;

    MutableColumns columns;
    IColumn::Offsets offsets_to_replicate;
    IColumn::Filter filter;
    /// For every row with a match, if we set filter[row] = 1, we also add this row to `matched_rows` for faster ScatteredBlock::filter().
    /// The per-row `push_back` reloads `c_end` and `c_end_of_storage` right after the previous row stored `c_end`. If the pair
    /// crosses a cache line, store-to-load forwarding fails. On the stack that depended on the callers' frames, and it made a
    /// 1-thread join 35% slower.
    alignas(64) IColumn::Offsets matched_rows;

    /// for lazy
    // The default row is represented by a zero ref word, so that fixed-size blocks can be generated sequentially,
    // default_count cannot represent the position of the row
    LazyOutput lazy_output;
    bool record_row_refs = false;

    /// Non-owning; set only under EXPLAIN ANALYZE
    MatchedRowsStats * match_stats = nullptr;

    size_t matched_left_rows = 0;

private:

    void checkColumns(const StoredBlock & to_check)
    {
        auto check = [&](size_t dst_idx, const IColumn * column_from_block)
        {
            const auto * dest_column = columns[dst_idx].get();
            if (auto * nullable_col = nullable_column_ptrs[dst_idx])
            {
                if (!is_join_get)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Columns {} and {} can have different nullability only in joinGetOrNull",
                                    dest_column->getName(), column_from_block->getName());
                dest_column = nullable_col->getNestedColumnPtr().get();
            }

            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(column_from_block))
                column_from_block = column_replicated->getNestedColumn().get();
            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(dest_column))
                dest_column = column_replicated->getNestedColumn().get();

            /** Using dest_column->structureEquals(*column_from_block) will not work for low cardinality columns,
              * because dictionaries can be different, while calling insertFrom on them is safe, for example:
              * ColumnLowCardinality(size = 0, UInt8(size = 0), ColumnUnique(size = 1, String(size = 1)))
              * and
              * ColumnLowCardinality(size = 0, UInt16(size = 0), ColumnUnique(size = 1, String(size = 1)))
              */
            if (typeid(*dest_column) != typeid(*column_from_block))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} have different types {} and {}",
                                dest_column->getName(), column_from_block->getName(),
                                demangle(typeid(*dest_column).name()), demangle(typeid(*column_from_block).name()));
        };

        for (size_t dst_idx = 0; dst_idx < lazy_output.output_access_indexes.size(); ++dst_idx)
        {
            const auto & output_access_index = lazy_output.output_access_indexes[dst_idx];
            if (output_access_index.type == ColumnAccessIndex::Type::RowStore)
            {
                if (to_check.hasRowStore())
                {
                    auto sample_col = to_check.row_store->getFieldLayout(output_access_index.index).type->createColumn();
                    check(dst_idx, sample_col.get());
                }
            }
            else
                check(dst_idx, to_check.columns.at(output_access_index.index).get());
        }
    }

    bool is_join_get;
    std::vector<ColumnNullable *> nullable_column_ptrs;

    /// for ASOF
    const IColumn * left_asof_key = nullptr;

    void addColumn(const ColumnWithTypeAndName & src_column)
    {
        /// Not reserved here: the emit kernels reserve when they know the exact row count.
        columns.push_back(src_column.column->cloneEmpty());
        lazy_output.type_name.emplace_back(src_column.name, src_column.type);
    }
};

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const StoredBlock * block, size_t row_num, size_t column_index);

}
