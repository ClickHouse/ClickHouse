#include  <Interpreters/NotJoinedSingleSlot.h>

#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/joinDispatch.h>

#include <Common/Exception.h>
#include <mutex>
#include <any>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block NotJoinedSingleSlot::getEmptyBlock()
{
    if (join.hash_joins.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No concurrency slots for NotJoinedSingleSlot");
    const auto & slot_ptr = join.hash_joins[slot_index];
    std::lock_guard lock(slot_ptr->mutex);
    return slot_ptr->data->savedBlockSample().cloneEmpty();
}

size_t NotJoinedSingleSlot::fillColumns(MutableColumns & columns_right)
{
    if (done)
        return 0;

    const auto & slot_ptr = join.hash_joins[slot_index];
    std::unique_lock<std::mutex> lock(slot_ptr->mutex);
    HashJoin & hash_join = *slot_ptr->data;
    const auto & right_data = hash_join.getJoinedData();

    if (right_data->type == HashJoin::Type::EMPTY || right_data->type == HashJoin::Type::CROSS)
    {
        done = true;
        return 0;
    }

    size_t rows_added = 0;
    const auto & blocks = right_data->blocks;

    if (right_data->type == HashJoin::Type::EMPTY)
    {
        rows_added = fillColumnsFromData(blocks, columns_right);
        if (rows_added == 0)
            done = true;
        return rows_added;
    }

    if (flag_per_row)
    {
        rows_added = fillUsedFlagsRowByRow(hash_join, columns_right);
    }
    else
    {

        // We'll do the same approach that NotJoinedHash does
        bool prefer_maps_all = hash_join.getTableJoin().getMixedJoinExpression() != nullptr;
        auto & map_variant = hash_join.getJoinedData()->maps[0];

        joinDispatch(
            hash_join.getKind(),
            hash_join.getStrictness(),
            map_variant,
            prefer_maps_all,
            [&](auto, auto, auto & maps_holder)
            {
                // We do a switch on data->type
                switch (hash_join.getJoinedData()->type)
                {
        #define M(NAME) \
                    case HashJoin::Type::NAME: \
                    { \
                        if (maps_holder.NAME) \
                            rows_added = fillFromMap(hash_join, *maps_holder.NAME, columns_right); \
                        break; \
                    }
                    APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
                    default:
                        break;
                }
            });
    }

    if (!flag_per_row && rows_added < max_block_size)
        rows_added += fillNullsFromBlocks(hash_join, columns_right, rows_added);

    // If we didn't add anything, consider we are done
    if (rows_added == 0)
        done = true;

    return rows_added;
}

size_t NotJoinedSingleSlot::fillColumnsFromData(const HashJoin::ScatteredBlocksList & blocks, MutableColumns & columns_right)
{
    if (!position.has_value())
        position = std::make_any<HashJoin::ScatteredBlocksList::const_iterator>(blocks.begin());

    size_t rows_added = 0;
    auto & block_it = std::any_cast<HashJoin::ScatteredBlocksList::const_iterator &>(position);
    auto end_it = blocks.end();

    for (; block_it != end_it && rows_added < max_block_size; ++block_it)
    {
        const auto & stored_block = *block_it;
        size_t block_rows = stored_block.rows();
        size_t to_read = std::min<size_t>(max_block_size - rows_added, block_rows - current_block_start);

        for (size_t col = 0; col < columns_right.size(); ++col)
        {
            const auto & src_col = stored_block.getByPosition(col).column;
            columns_right[col]->insertRangeFrom(*src_col, current_block_start, to_read);
        }
        rows_added += to_read;
        current_block_start += to_read;

        if (rows_added >= max_block_size)
        {
            if (current_block_start >= block_rows)
            {
                ++block_it;
                current_block_start = 0;
            }
            break;
        }
        current_block_start = 0;
    }
    return rows_added;
}

size_t NotJoinedSingleSlot::fillUsedFlagsRowByRow(const HashJoin & hash_join, MutableColumns & columns_right)
{
    auto & blocks = hash_join.getJoinedData()->blocks;
    if (!used_position.has_value())
        used_position = blocks.begin();

    size_t rows_added = 0;
    auto end_it = blocks.end();

    for (auto & it = *used_position; it != end_it && rows_added < max_block_size; ++it)
    {
        const auto & stored_block = *it;
        size_t block_rows = stored_block.rows();
        for (; current_block_start < block_rows && rows_added < max_block_size; ++current_block_start)
        {
            if (!hash_join.isUsed(&stored_block.getSourceBlock(), current_block_start))
            {
                for (size_t col = 0; col < columns_right.size(); ++col)
                {
                    columns_right[col]->insertFrom(*stored_block.getByPosition(col).column, current_block_start);
                }
                ++rows_added;
            }
        }
        if (rows_added >= max_block_size)
        {
            if (current_block_start >= block_rows)
            {
                ++it;
                current_block_start = 0;
            }
            break;
        }
        current_block_start = 0;
    }
    return rows_added;
}

size_t NotJoinedSingleSlot::fillNullsFromBlocks(
    const HashJoin & hash_join,
    MutableColumns & columns_right,
    size_t rows_already_added)
{
    size_t rows_added = 0;
    if (!nulls_position.has_value())
        nulls_position = hash_join.getJoinedData()->blocks_nullmaps.begin();

    auto & it = *nulls_position;
    auto end_it = hash_join.getJoinedData()->blocks_nullmaps.end();

    while (it != end_it && (rows_already_added + rows_added < max_block_size))
    {
        const auto * stored_block_ptr = it->block;
        if (!stored_block_ptr)
        {
            ++it;
            continue;
        }
        size_t block_rows = stored_block_ptr->rows();
        const auto & mask_col = assert_cast<const ColumnUInt8 &>(*it->column).getData();
        for (; current_block_start < block_rows && rows_already_added + rows_added < max_block_size; ++current_block_start)
        {
            if (mask_col[current_block_start])
            {
                for (size_t col = 0; col < columns_right.size(); ++col)
                {
                    columns_right[col]->insertFrom(*stored_block_ptr->getByPosition(col).column, current_block_start);
                }
                ++rows_added;
            }
        }
        if (rows_already_added + rows_added >= max_block_size)
        {
            if (current_block_start >= block_rows)
            {
                ++it;
                current_block_start = 0;
            }
            break;
        }
        current_block_start = 0;
        ++it;
    }
    return rows_added;
}


// a helper for "ANY" or "ALL" map
template <typename Mapped>
static void addUnmatchedMapped(
    const Mapped & mapped,
    size_t & rows_added,
    size_t max_block_size,
    MutableColumns & columns_right)
{
    if constexpr (std::is_same_v<Mapped, RowRef>)
    {
        // "ANY" single row
        if (rows_added < max_block_size)
        {
            for (size_t col = 0; col < columns_right.size(); ++col)
            {
                const auto & src_col = mapped.block->getByPosition(col).column;
                columns_right[col]->insertFrom(*src_col, mapped.row_num);
            }
            ++rows_added;
        }
    }
    else if constexpr (std::is_same_v<Mapped, RowRefList>)
    {
        // "ALL" multiple rows
        for (auto rr_it = mapped.begin(); rr_it.ok() && rows_added < max_block_size; ++rr_it)
        {
            for (size_t col = 0; col < columns_right.size(); ++col)
            {
                const auto & src_col = rr_it->block->getByPosition(col).column;
                columns_right[col]->insertFrom(*src_col, rr_it->row_num);
            }
            ++rows_added;
        }
    }
    else if constexpr (std::is_same_v<Mapped, AsofRowRefs>)
    {
        // Case for ASOF JOIN, throwing an exception here (ASOF is not used with anything except LEFT JOIN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "NotJoinedSingleSlot is not supported for ASOF JOIN");
    }
    else if constexpr (std::is_same_v<Mapped, std::unique_ptr<SortedLookupVectorBase>>)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SortedLookupVectorBase received in NotJoinedSingleSlot");
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown mapped type received in NotJoinedSingleSlot");
    }
}

template <typename MapType>
size_t fillFromMap(
    HashJoin & hash_join,
    MapType & map,
    std::any & position,
    size_t max_block_size,
    MutableColumns & columns_right)
{
    using Iterator = typename MapType::iterator;
    using CellType = typename MapType::cell_type;
    using Mapped   = typename CellType::mapped_type;

    size_t rows_added = 0;

    // if it's the first call, set position to map.begin()
    if (!position.has_value())
        position = std::make_any<Iterator>(map.begin());

    auto & it = std::any_cast<Iterator &>(position);
    auto end_it = map.end();

    while (it != end_it && rows_added < max_block_size)
    {
        size_t offset = map.offsetInternal(it.getPtr());
        if (!hash_join.isUsed(offset))
        {
            // We have an unmatched row => output
            const Mapped & mapped = it->getMapped();
            addUnmatchedMapped(mapped, rows_added, max_block_size, columns_right);
        }
        ++it;
    }

    return rows_added;
}

template <typename MapType>
size_t NotJoinedSingleSlot::fillFromMap(HashJoin & hash_join, MapType & map_type, MutableColumns & columns_right)
{
    return DB::fillFromMap(hash_join, map_type, position, max_block_size, columns_right);
}

}
