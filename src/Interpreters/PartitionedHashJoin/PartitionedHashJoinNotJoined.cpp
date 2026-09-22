#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/fillRowStoreOutputColumns.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
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
extern const int UNSUPPORTED_JOIN_KEYS;
}

/** RIGHT/FULL non-joined rows of the shared table; counterpart of `NotJoinedHash` with per-offset
  * used flags. Stream `i` of `n` owns cell positions `[i * cells / n, (i + 1) * cells / n)`. A
  * cell's used flag is its position plus one. Stream 0 also emits the zero-value cell and rows
  * whose keys were never inserted (saved null maps). Several disjuncts run on the delegated
  * `HashJoin` and its `NotJoinedHash`.
  *
  * Fixed-width payload is in a row store; remaining columns stay columnar. Output is filled
  * through the join's access indexes, never by position.
  */
class NotJoinedPartitioned final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedPartitioned(const PartitionedHashJoin & parent_, UInt64 max_block_size_, size_t stream_idx_, size_t num_streams_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , stream_idx(stream_idx_)
        , num_streams(num_streams_)
        , block_row_stores(parent.storedData().stored_columns_index->rowStoresData())
    {
        /// The output columns are `getEmptyBlock`'s, so they are positional with the saved sample.
        const Block & saved = parent.hash_join->savedBlockSample();
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

    Block getEmptyBlock() override { return parent.hash_join->savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        dispatchStorage(
            [&]<bool with_row_store, bool with_columns>()
            {
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
                            default:
                                throw Exception(
                                    ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                                    "Unsupported JOIN keys for the partitioned join (type: {})",
                                    type);
                        }
                    },
                    parent.clause.tableMaps().maps);

                fillNullsFromBlocks<with_row_store, with_columns>(columns_right, rows_added);
            });
        if (auto * stats = parent.matched_rows_stats.get())
            stats->collectNonJoined(rows_added);
        return rows_added;
    }

private:
    const PartitionedHashJoin & parent;
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
        /// The same walk as `CollectorNonJoined`, which is file-local to `HashJoin.cpp`. ASOF never
        /// reaches here, being LEFT/INNER only.
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
                if (table.hasZero() && !parent.hash_join->isUsed(0))
                    collectMapped<with_row_store, with_columns>(table.zeroValue()->getMapped(), collected);
            }
            for (; position < end && collected.rows < max_block_size; ++position)
            {
                const auto * cell = table.cellAt(position);
                if (table.isEmptyCell(cell))
                    continue;
                if (parent.hash_join->isUsed(position + 1))
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
                if (parent.hash_join->isUsed(table.offsetInternal(it.getPtr())))
                    continue;
                collectMapped<with_row_store, with_columns>(it->getMapped(), collected);
            }
        }

        fillOutput(columns_right, collected);
        return collected.rows;
    }

    /// The rows that never entered the table, from the null maps saved when the build ended; as
    /// `NotJoinedHash::fillNullsFromBlocks` does. Not partitioned, so exactly one stream emits them.
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

bool PartitionedHashJoin::supportParallelNonJoinedBlocksProcessing() const
{
    return !delegate_mode && table_join->allowParallelNonJoinedRowsProcessing() && JoinCommon::hasNonJoinedBlocks(*table_join)
        && !table_join->getOnlyClause().key_names_right.empty();
}

IBlocksStreamPtr
PartitionedHashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size, /*stream_idx=*/0, /*num_streams=*/1);
}

IBlocksStreamPtr PartitionedHashJoin::getNonJoinedBlocks(
    const Block & left_sample_block,
    const Block & result_sample_block,
    UInt64 max_block_size,
    size_t stream_idx,
    size_t num_streams) const
{
    if (delegate_mode)
    {
        /// `supportParallelNonJoinedBlocksProcessing` keeps this path single-stream, so only the
        /// first stream has anything to emit.
        if (stream_idx != 0)
            return {};
        return hash_join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
    }

    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};

    /// The same check `HashJoin::getNonJoinedBlocks` makes; the shapes that would break the
    /// invariant took the delegated branch above.
    size_t left_columns_count = left_sample_block.columns();
    if (hash_join->canRemoveColumnsFromLeftBlock())
        left_columns_count = table_join->getOutputColumns(JoinTableSide::Left).size();

    const size_t expected_columns_count
        = left_columns_count + hash_join->required_right_keys.columns() + hash_join->sample_block_with_columns_to_add.columns();
    if (expected_columns_count != result_sample_block.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of columns in result sample block: {} expected {} ([{}] = [{}] + [{}] + [{}])",
            result_sample_block.columns(),
            expected_columns_count,
            result_sample_block.dumpNames(),
            left_sample_block.dumpNames(),
            hash_join->required_right_keys.dumpNames(),
            hash_join->sample_block_with_columns_to_add.dumpNames());

    auto non_joined = std::make_unique<NotJoinedPartitioned>(*this, max_block_size, stream_idx, num_streams);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

}
