#include <Columns/ColumnsNumber.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
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

/** The partitioned counterpart of `NotJoinedHash`'s per-offset regime, for RIGHT/FULL output. Leaf
  * maps are iterated in leaf order - one filler walks only its own stride of them, so that parallel
  * fillers emit disjoint rows - and a cell's used flag sits at its leaf's `flag_base` plus the
  * map-internal offset, exactly where the probe marked it. Rows whose keys were never inserted come
  * from the saved nullmap holders, as in the standard filler. Nothing here handles the per-row-flags
  * regime, whose shapes take the delegated path and `NotJoinedHash` itself.
  */
class NotJoinedPartitioned final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedPartitioned(const PartitionedHashJoin & parent_, UInt64 max_block_size_, size_t stream_idx_, size_t num_streams_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
        , current_leaf(stream_idx_)
        , owns_nulls(stream_idx_ == 0)
    {
    }

    Block getEmptyBlock() override { return parent.leaf_join->savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        const HashJoin::Type type = parent.storedData().type;

        size_t rows_added = std::visit(
            [&](const auto & first_leaf_shape)
            {
                using Shape = std::decay_t<decltype(first_leaf_shape)>;
                switch (type)
                {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return fillFromLeaves(columns_right, [this](size_t leaf) { return std::get<Shape>(parent.leaf_maps[leaf].maps).TYPE.get(); });
                    APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                    default:
                        throw Exception(
                            ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", type);
                }
            },
            parent.leaf_maps.front().maps);

        fillNullsFromBlocks(columns_right, rows_added);
        return rows_added;
    }

private:
    const PartitionedHashJoin & parent;
    const UInt64 max_block_size;
    const size_t num_streams;

    size_t current_leaf;
    /// Nullmap rows are not partitioned by leaf, so exactly one stream emits them.
    const bool owns_nulls;
    std::any position; /// iterator into the current leaf's map, resumable across calls
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;

    template <typename Mapped>
    static void collectMapped(
        const Mapped & mapped,
        const StoredBlock * const * stored_columns,
        VectorWithMemoryTracking<const StoredBlock *> & blocks,
        VectorWithMemoryTracking<UInt32> & row_numbers)
    {
        /// As `CollectorNonJoined` does. ASOF never reaches here, being LEFT/INNER only.
        if constexpr (std::is_same_v<Mapped, RowRefList>)
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                const UInt64 ref_word = *it;
                blocks.push_back(stored_columns[refWordBlockNo(ref_word)]);
                row_numbers.push_back(refWordRowNo(ref_word));
            }
        }
        else if constexpr (std::is_same_v<Mapped, RowRef>)
        {
            blocks.push_back(stored_columns[mapped.blockNo()]);
            row_numbers.push_back(mapped.rowNo());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined rows are not supported for ASOF joins");
        }
    }

    template <typename LeafMapGetter>
    size_t fillFromLeaves(MutableColumns & columns_keys_and_right, LeafMapGetter && leaf_map_of)
    {
        using Map = std::remove_pointer_t<std::invoke_result_t<LeafMapGetter, size_t>>;
        using Iterator = Map::const_iterator;

        ColumnsWithRowNumbers columns_with_row_numbers;
        auto & many_columns = columns_with_row_numbers.columns;
        auto & row_nums = columns_with_row_numbers.row_numbers;
        many_columns.reserve(max_block_size);
        row_nums.reserve(max_block_size);

        const StoredBlock * const * stored_columns = parent.storedData().stored_columns_index->blocksData();
        const size_t num_leaves = parent.leaf_maps.size();

        while (current_leaf < num_leaves && row_nums.size() < max_block_size)
        {
            const Map & map = *leaf_map_of(current_leaf);
            if (!position.has_value())
                position = std::make_any<Iterator>(map.begin());

            Iterator & it = std::any_cast<Iterator &>(position);
            const auto end = map.end();
            const UInt64 leaf_flag_base = parent.flag_base[current_leaf];

            for (; it != end && row_nums.size() < max_block_size; ++it)
            {
                const size_t offset = leaf_flag_base + map.offsetInternal(it.getPtr());
                if (parent.leaf_join->isUsed(offset))
                    continue;
                collectMapped(it->getMapped(), stored_columns, many_columns, row_nums);
            }

            if (it == end)
            {
                current_leaf += num_streams;
                position.reset();
            }
        }

        for (size_t j = 0; j < columns_keys_and_right.size(); ++j)
            columns_keys_and_right[j]->fillFromBlocksAndRowNumbers(j, columns_with_row_numbers);

        return row_nums.size();
    }

    /// The rows that never entered a map, from the nullmap holders saved at the build barrier; as
    /// `NotJoinedHash::fillNullsFromBlocks` does.
    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!owns_nulls)
            return;

        const auto & nullmaps = parent.storedData().nullmaps;
        if (!nulls_position.has_value())
            nulls_position = nullmaps.begin();

        auto end = nullmaps.end();

        ColumnsWithRowNumbers columns_with_row_numbers;
        auto & many_columns = columns_with_row_numbers.columns;
        auto & row_nums = columns_with_row_numbers.row_numbers;
        many_columns.reserve(max_block_size);
        row_nums.reserve(max_block_size);

        for (auto & it = *nulls_position; it != end && rows_added + row_nums.size() < max_block_size; ++it)
        {
            const auto * columns = it->columns;
            ConstNullMapPtr nullmap = nullptr;
            if (it->column)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->column).getData();

            for (size_t row : columns->selector)
            {
                if (nullmap && (*nullmap)[row])
                {
                    many_columns.push_back(columns);
                    row_nums.push_back(static_cast<UInt32>(row));
                }
            }
        }

        for (size_t j = 0; j < columns_keys_and_right.size(); ++j)
            columns_keys_and_right[j]->fillFromBlocksAndRowNumbers(j, columns_with_row_numbers);
        rows_added += row_nums.size();
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
