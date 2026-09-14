#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/HashJoin/fillJoinOutputColumns.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

#include <any>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

/** The partitioned counterpart of `NotJoinedHash`'s per-offset regime, for RIGHT/FULL output. The one
  * table is walked by cell position: stream `i` of `n` owns positions `[i * cells / n, (i + 1) * cells / n)`,
  * so parallel fillers emit disjoint rows, and a cell's used flag is its position plus one - exactly where
  * the probe marked it, offset 0 being the zero-value cell, which stream 0 emits along with the rows whose
  * keys were never inserted (from the saved nullmap holders, as in the standard filler). Nothing here
  * handles the per-row-flags regime, whose shapes take the delegated path and `NotJoinedHash` itself.
  *
  * A stored block keeps its fixed-width payload in a row store and the rest columnar, so the output
  * columns are filled through the join's access indexes, as `NotJoinedHash` does, never by position.
  */
class NotJoinedPartitioned final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedPartitioned(const PartitionedHashJoin & parent_, UInt64 max_block_size_, size_t stream_idx_, size_t num_streams_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , stream_idx(stream_idx_)
        , num_streams(num_streams_)
        , stored_blocks(parent.storedData().stored_columns_index->blocksData())
        , block_row_stores(parent.storedData().stored_columns_index->rowStoresData())
    {
        /// `columns_keys_and_right` is built from `getEmptyBlock`, so it is positional with the saved sample.
        const auto & data = parent.storedData();
        const Block & saved = parent.leaf_join->savedBlockSample();
        type_name.reserve(saved.columns());
        for (const auto & column : saved)
            type_name.emplace_back(column.name, column.type);

        if (data.row_store_state == HashJoin::RowStoreState::Initialized)
        {
            output_access_indexes = data.column_access_indexes;
            with_row_store = true;
            for (const auto & access_index : output_access_indexes)
                with_columns = with_columns || access_index.type == ColumnAccessIndex::Type::Columns;
        }
        else
        {
            output_access_indexes.reserve(saved.columns());
            for (size_t j = 0; j < saved.columns(); ++j)
                output_access_indexes.push_back({ColumnAccessIndex::Type::Columns, j});
            with_columns = true;
        }
    }

    Block getEmptyBlock() override { return parent.leaf_join->savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        dispatchStorage(
            [&]<bool with_row_store_, bool with_columns_>()
            {
                const HashJoin::Type type = parent.storedData().type;
                rows_added = std::visit(
                    [&](const auto & shape)
                    {
                        switch (type)
                        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: return fillFromTable<with_row_store_, with_columns_>(columns_right, *shape.TYPE);
                            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                            default:
                                throw Exception(
                                    ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                                    "Unsupported JOIN keys for the partitioned join (type: {})",
                                    type);
                        }
                    },
                    parent.shared_maps->maps);

                fillNullsFromBlocks<with_row_store_, with_columns_>(columns_right, rows_added);
            });
        return rows_added;
    }

private:
    const PartitionedHashJoin & parent;
    const UInt64 max_block_size;
    const size_t stream_idx;
    const size_t num_streams;

    /// Per-block bases of the stored blocks and their row stores; stable once the build is finished.
    const StoredBlock * const * stored_blocks;
    const RowDataStore * const * block_row_stores;
    /// Where each saved-block column lives (row store field or columnar position) and its type.
    ColumnAccessIndexes output_access_indexes;
    NamesAndTypes type_name;
    bool with_row_store = false;
    bool with_columns = false;

    /// The rows one call collected: `(block, row)` pairs for the columnar part, row pointers for the
    /// row store part.
    struct Collected
    {
        ColumnsWithRowNumbers columns_with_row_numbers;
        RowStorePointers row_store_ptrs;
        std::optional<size_t> row_store_batch_size;
        size_t rows = 0;

        void reserve(size_t n)
        {
            columns_with_row_numbers.columns.reserve(n);
            columns_with_row_numbers.row_numbers.reserve(n);
            row_store_ptrs.ptrs.reserve(n);
        }
    };

    template <typename F>
    void dispatchStorage(F && f) const
    {
        if (with_row_store && with_columns)
            f.template operator()<true, true>();
        else if (with_row_store)
            f.template operator()<true, false>();
        else
            f.template operator()<false, true>();
    }

    template <bool with_row_store_, bool with_columns_>
    void collectRow(UInt32 block_no, UInt32 row_no, Collected & out) const
    {
        if constexpr (with_columns_)
        {
            out.columns_with_row_numbers.columns.push_back(stored_blocks[block_no]);
            out.columns_with_row_numbers.row_numbers.push_back(row_no);
        }
        if constexpr (with_row_store_)
        {
            const RowDataStore * row_store = block_row_stores[block_no];
            out.row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row_no));
            if (!out.row_store_batch_size)
                out.row_store_batch_size = row_store->getBatchSize();
        }
        ++out.rows;
    }

    void fillOutput(MutableColumns & columns_keys_and_right, const Collected & collected) const
    {
        fillJoinOutputColumns(
            columns_keys_and_right,
            output_access_indexes,
            collected.row_store_ptrs,
            collected.row_store_batch_size,
            collected.columns_with_row_numbers,
            type_name);
    }

    /// The shared-table cursor: the next cell position of this stream's stripe, and whether the zero cell
    /// has been considered (stream 0 only).
    size_t position = 0;
    bool positioned = false;
    bool zero_done = false;
    /// The fixed-map cursor, resumable across calls.
    std::any fixed_position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;

    template <bool with_row_store_, bool with_columns_, typename Mapped>
    void collectMapped(const Mapped & mapped, Collected & out) const
    {
        /// As `CollectorNonJoined` does. ASOF never reaches here, being LEFT/INNER only.
        if constexpr (std::is_same_v<Mapped, RowRefList>)
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                const UInt64 ref_word = *it;
                collectRow<with_row_store_, with_columns_>(refWordBlockNo(ref_word), refWordRowNo(ref_word), out);
            }
        }
        else if constexpr (std::is_same_v<Mapped, RowRef>)
        {
            collectRow<with_row_store_, with_columns_>(mapped.blockNo(), mapped.rowNo(), out);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined rows are not supported for ASOF joins");
        }
    }

    template <bool with_row_store_, bool with_columns_, typename Table>
    size_t fillFromTable(MutableColumns & columns_keys_and_right, const Table & table)
    {
        Collected collected;
        collected.reserve(max_block_size);

        if constexpr (is_shared_join_table<Table>)
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
                if (table.hasZero() && !parent.leaf_join->isUsed(0))
                    collectMapped<with_row_store_, with_columns_>(table.zeroValue()->getMapped(), collected);
            }
            for (; position < end && collected.rows < max_block_size; ++position)
            {
                const auto * cell = table.cellAt(position);
                if (table.isEmptyCell(cell))
                    continue;
                if (parent.leaf_join->isUsed(position + 1))
                    continue;
                collectMapped<with_row_store_, with_columns_>(cell->getMapped(), collected);
            }
        }
        else if (stream_idx == 0)
        {
            /// The direct-index maps are at most 65536 cells; one stream walks them whole.
            using Iterator = typename Table::const_iterator;
            if (!fixed_position.has_value())
                fixed_position = std::make_any<Iterator>(table.begin());
            Iterator & it = std::any_cast<Iterator &>(fixed_position);
            const auto end = table.end();
            for (; it != end && collected.rows < max_block_size; ++it)
            {
                if (parent.leaf_join->isUsed(table.offsetInternal(it.getPtr())))
                    continue;
                collectMapped<with_row_store_, with_columns_>(it->getMapped(), collected);
            }
        }

        fillOutput(columns_keys_and_right, collected);
        return collected.rows;
    }

    /// The rows that never entered the table, from the nullmap holders saved at the build barrier; as
    /// `NotJoinedHash::fillNullsFromBlocks` does. Not partitioned, so exactly one stream emits them.
    template <bool with_row_store_, bool with_columns_>
    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (stream_idx != 0)
            return;

        const auto & nullmaps = parent.storedData().nullmaps;
        if (!nulls_position.has_value())
            nulls_position = nullmaps.begin();

        auto end = nullmaps.end();

        Collected collected;
        collected.reserve(max_block_size);

        for (auto & it = *nulls_position; it != end && rows_added + collected.rows < max_block_size; ++it)
        {
            const StoredBlock * stored = it->columns;
            ConstNullMapPtr nullmap = nullptr;
            if (it->column)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->column).getData();

            for (size_t row : stored->selector)
                if (nullmap && (*nullmap)[row])
                    collectRow<with_row_store_, with_columns_>(stored->block_no, static_cast<UInt32>(row), collected);
        }

        fillOutput(columns_keys_and_right, collected);
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
        return leaf_join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
    }

    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};

    /// The same check `HashJoin::getNonJoinedBlocks` makes; the shapes that would break the
    /// invariant took the delegated branch above.
    size_t left_columns_count = left_sample_block.columns();
    if (leaf_join->canRemoveColumnsFromLeftBlock())
        left_columns_count = table_join->getOutputColumns(JoinTableSide::Left).size();

    const size_t expected_columns_count
        = left_columns_count + leaf_join->required_right_keys.columns() + leaf_join->sample_block_with_columns_to_add.columns();
    if (expected_columns_count != result_sample_block.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of columns in result sample block: {} expected {} ([{}] = [{}] + [{}] + [{}])",
            result_sample_block.columns(),
            expected_columns_count,
            result_sample_block.dumpNames(),
            left_sample_block.dumpNames(),
            leaf_join->required_right_keys.dumpNames(),
            leaf_join->sample_block_with_columns_to_add.dumpNames());

    auto non_joined = std::make_unique<NotJoinedPartitioned>(*this, max_block_size, stream_idx, num_streams);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

}
