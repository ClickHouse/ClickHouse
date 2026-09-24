#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/fillRowStoreOutputColumns.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

#include <any>
#include <numeric>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/** RIGHT/FULL non-joined rows of the shared table. With per-offset
  * used flags, stream `i` of `n` owns cell positions `[i * cells / n, (i + 1) * cells / n)`. A
  * cell's used flag is its position plus one. Stream 0 also emits the zero-value cell and rows
  * whose keys were never inserted (saved null maps). With per-row used flags (`used_flags_per_row`:
  * several ON clauses, or a mixed ON condition on a RIGHT or FULL join) the stored blocks are walked
  * instead, row by row; the streams take the blocks round-robin by block number. A row nothing
  * marked is emitted whether or not its key ever entered a table, so no null maps are kept for that
  * shape.
  *
  * Fixed-width payload is in a row store; remaining columns stay columnar. Output is filled
  * through the join's access indexes, never by position.
  */
class NotJoinedPartitioned final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedPartitioned(const HashJoin & parent_, UInt64 max_block_size_, size_t stream_idx_, size_t num_streams_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , stream_idx(stream_idx_)
        , num_streams(num_streams_)
        , block_row_stores(parent.storedData().stored_columns_index->rowStoresData())
    {
        /// The output columns are `getEmptyBlock`'s, so they are positional with the saved sample.
        const Block & saved = parent.savedBlockSample();
        type_name.reserve(saved.columns());
        for (const auto & column : saved)
            type_name.emplace_back(column.name, column.type);

        std::vector<size_t> positions(saved.columns());
        std::iota(positions.begin(), positions.end(), 0);
        EmitPlan plan = planJoinEmit(parent.storedData(), positions, type_name, /*with_gather=*/true);
        output_access_indexes = std::move(plan.access_indexes);
        emit_gather = std::move(plan.gather);
        has_row_store = plan.has_row_store;
        has_columns = plan.has_columns;
    }

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        dispatchStorage(
            [&]<bool with_row_store, bool with_columns>()
            {
                if (parent.used_flags_per_row)
                {
                    rows_added = fillFromStoredBlocks<with_row_store, with_columns>(columns_right);
                    return;
                }

                const HashJoin::Type type = parent.storedData().type;
                rows_added = std::visit(
                    [&](const auto & shape)
                    {
                        switch (type)
                        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: return fillFromTable<with_row_store, with_columns>(columns_right, *shape.TYPE);
                            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
                        }
                    },
                    parent.clauses.front().tableMaps().maps);

                fillNullsFromBlocks<with_row_store, with_columns>(columns_right, rows_added);
            });
        if (auto * stats = parent.matched_rows_stats.get())
            stats->collectNonJoined(rows_added);
        return rows_added;
    }

private:
    const HashJoin & parent;
    const UInt64 max_block_size;
    const size_t stream_idx;
    const size_t num_streams;

    /// Per-block bases of the row stores; stable once the build is finished.
    const RowDataStore * const * block_row_stores;
    /// Where each saved-block column lives (row store field or columnar position), its type, and
    /// the gather source of the columnar ones.
    ColumnAccessIndexes output_access_indexes;
    NamesAndTypes type_name;
    std::vector<GatherColumn> emit_gather;
    bool has_row_store = false;
    bool has_columns = false;

    /// The shared-table cursor: the next cell position of this stream's stripe, and whether the zero cell
    /// has been considered (stream 0 only).
    size_t position = 0;
    bool positioned = false;
    bool zero_done = false;
    /// The fixed-map cursor, resumable across calls.
    std::any fixed_position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;
    /// The stored-block cursor of the per-row scan, resumable across calls.
    std::optional<HashJoin::StoredBlocksList::const_iterator> stored_position;

    /// The rows one call collected: encoded ref words for the columnar part, row pointers for the
    /// row store part.
    struct Collected
    {
        PaddedPODArray<UInt64> words;
        RowStorePointers row_store_ptrs;
        std::optional<size_t> row_store_batch_size;
        size_t rows = 0;

        template <bool with_row_store, bool with_columns>
        void reserve(size_t n)
        {
            if constexpr (with_columns)
                words.reserve(n);
            if constexpr (with_row_store)
                row_store_ptrs.ptrs.reserve(n);
        }
    };

    template <typename F>
    void dispatchStorage(F && f) const
    {
        if (has_row_store && has_columns)
            f.template operator()<true, true>();
        else if (has_row_store)
            f.template operator()<true, false>();
        else
            f.template operator()<false, true>();
    }

    template <bool with_row_store, bool with_columns>
    void collectRow(UInt32 block_no, UInt32 row_no, Collected & out) const
    {
        if constexpr (with_columns)
            out.words.push_back(RowRef(block_no, row_no).encode());
        if constexpr (with_row_store)
        {
            const RowDataStore * row_store = block_row_stores[block_no];
            out.row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row_no));
            if (!out.row_store_batch_size)
                out.row_store_batch_size = row_store->getBatchSize();
        }
        ++out.rows;
    }

    /// Flat: a not-joined row is always one inline ref, never a list and never a default.
    void fillOutput(MutableColumns & columns_right, const Collected & collected) const
    {
        if (has_columns)
        {
            const RefWordSelection selection{
                .begin = collected.words.data(),
                .end = collected.words.data() + collected.words.size(),
                .rows = collected.words.size(),
                .shape = RefWordShape::Flat};
            EmitScratch scratch;
            for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
                if (output_access_indexes[dst_idx].type == ColumnAccessIndex::Type::Columns)
                    gatherColumn(*columns_right[dst_idx], emit_gather[dst_idx], selection, scratch);
        }
        if (has_row_store)
            fillRowStoreOutputColumns(
                columns_right, output_access_indexes, collected.row_store_ptrs, collected.row_store_batch_size, type_name);
    }

    template <bool with_row_store, bool with_columns, typename Mapped>
    void collectMapped(const Mapped & mapped, Collected & out) const
    {
        /// ASOF never reaches here, being LEFT/INNER only.
        if constexpr (std::is_same_v<Mapped, RowRefList>)
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                const UInt64 ref_word = *it;
                collectRow<with_row_store, with_columns>(refWordBlockNo(ref_word), refWordRowNo(ref_word), out);
            }
        }
        else if constexpr (std::is_same_v<Mapped, RowRef>)
        {
            collectRow<with_row_store, with_columns>(mapped.blockNo(), mapped.rowNo(), out);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined rows are not supported for ASOF joins");
        }
    }

    template <bool with_row_store, bool with_columns, typename Table>
    size_t fillFromTable(MutableColumns & columns_right, const Table & table)
    {
        Collected collected;
        collected.reserve<with_row_store, with_columns>(max_block_size);

        if constexpr (is_hash_join_table<Table>)
        {
            const size_t cells = table.cellCount();
            const size_t end = (stream_idx + 1) * cells / num_streams;
            if (!positioned)
            {
                position = stream_idx * cells / num_streams;
                positioned = true;
            }
            if (stream_idx == 0 && !zero_done)
            {
                zero_done = true;
                if (table.hasZero() && !parent.isUsed(0))
                    collectMapped<with_row_store, with_columns>(table.zeroValue()->getMapped(), collected);
            }
            for (; position < end && collected.rows < max_block_size; ++position)
            {
                const auto * cell = table.cellAt(position);
                if (table.isEmptyCell(cell))
                    continue;
                if (parent.isUsed(position + 1))
                    continue;
                collectMapped<with_row_store, with_columns>(cell->getMapped(), collected);
            }
        }
        else if (stream_idx == 0)
        {
            /// The direct-index maps are at most 2^18 cells; one stream walks them whole.
            using Iterator = typename Table::const_iterator;
            if (!fixed_position.has_value())
                fixed_position = std::make_any<Iterator>(table.begin());
            Iterator & it = std::any_cast<Iterator &>(fixed_position);
            const auto end = table.end();
            for (; it != end && collected.rows < max_block_size; ++it)
            {
                if (parent.isUsed(table.offsetInternal(it.getPtr())))
                    continue;
                collectMapped<with_row_store, with_columns>(it->getMapped(), collected);
            }
        }

        fillOutput(columns_right, collected);
        return collected.rows;
    }

    /// Per-row used flags: every stored row nothing marked, whether it entered a table or not.
    /// The streams take the stored blocks round-robin by block number.
    template <bool with_row_store, bool with_columns>
    size_t fillFromStoredBlocks(MutableColumns & columns_right)
    {
        const auto & stored_blocks = parent.storedBlocks();
        if (!stored_position.has_value())
            stored_position = stored_blocks.begin();

        Collected collected;
        collected.reserve<with_row_store, with_columns>(max_block_size);

        const auto end = stored_blocks.end();
        for (auto & it = *stored_position; it != end && collected.rows < max_block_size; ++it)
        {
            if (it->block_no % num_streams != stream_idx)
                continue;
            const size_t rows = it->blockRows();
            for (size_t row = 0; row < rows; ++row)
                if (!parent.isUsed(it->block_no, row))
                    collectRow<with_row_store, with_columns>(it->block_no, static_cast<UInt32>(row), collected);
        }

        fillOutput(columns_right, collected);
        return collected.rows;
    }

    /// The rows that never entered the table, from the null maps saved when the build ended.
    /// Not partitioned, so exactly one stream emits them.
    template <bool with_row_store, bool with_columns>
    void fillNullsFromBlocks(MutableColumns & columns_right, size_t & rows_added)
    {
        if (stream_idx != 0)
            return;

        const auto & nullmaps = parent.storedNullmaps();
        if (!nulls_position.has_value())
            nulls_position = nullmaps.begin();

        auto end = nullmaps.end();

        Collected collected;
        collected.reserve<with_row_store, with_columns>(max_block_size);

        for (auto & it = *nulls_position; it != end && rows_added + collected.rows < max_block_size; ++it)
        {
            const StoredBlock * stored = it->columns;
            ConstNullMapPtr nullmap = nullptr;
            if (it->column)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->column).getData();

            for (size_t row : stored->selector)
                if (nullmap && (*nullmap)[row])
                    collectRow<with_row_store, with_columns>(stored->block_no, static_cast<UInt32>(row), collected);
        }

        fillOutput(columns_right, collected);
        rows_added += collected.rows;
    }
};

bool HashJoin::supportParallelNonJoinedBlocksProcessing() const
{
    /// Without equi keys nothing reaches a table, so no right row is ever marked used and the scan cannot
    /// be split.
    const bool any_clause_has_right_keys = std::ranges::any_of(
        table_join->getClauses(), [](const TableJoin::JoinOnClause & on_clause) { return !on_clause.key_names_right.empty(); });
    return parallel_non_joined_allowed && table_join->allowParallelNonJoinedRowsProcessing()
        && JoinCommon::hasNonJoinedBlocks(*table_join) && any_clause_has_right_keys;
}

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size, /*stream_idx=*/0, /*num_streams=*/1);
}

IBlocksStreamPtr HashJoin::getNonJoinedBlocks(
    const Block & left_sample_block,
    const Block & result_sample_block,
    UInt64 max_block_size,
    size_t stream_idx,
    size_t num_streams) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};

    /// Skipped with several clauses: every right key is then among the columns to add, so the invariant
    /// does not hold.
    size_t left_columns_count = left_sample_block.columns();
    if (canRemoveColumnsFromLeftBlock())
        left_columns_count = table_join->getOutputColumns(JoinTableSide::Left).size();

    const size_t expected_columns_count
        = left_columns_count + required_right_keys.columns() + sample_block_with_columns_to_add.columns();
    if (!used_flags_per_row && expected_columns_count != result_sample_block.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of columns in result sample block: {} expected {} ([{}] = [{}] + [{}] + [{}])",
            result_sample_block.columns(),
            expected_columns_count,
            result_sample_block.dumpNames(),
            left_sample_block.dumpNames(),
            required_right_keys.dumpNames(),
            sample_block_with_columns_to_add.dumpNames());

    auto non_joined = std::make_unique<NotJoinedPartitioned>(*this, max_block_size, stream_idx, num_streams);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

}
