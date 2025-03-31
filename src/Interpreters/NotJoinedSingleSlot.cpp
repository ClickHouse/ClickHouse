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
        return 0; // no more data

    // We read from slot № slot_index only
    const auto & slot_ptr = join.hash_joins[slot_index];
    std::unique_lock<std::mutex> lock(slot_ptr->mutex);

    HashJoin & hash_join = *slot_ptr->data;
    if (hash_join.getJoinedData()->type == HashJoin::Type::EMPTY || hash_join.getJoinedData()->type == HashJoin::Type::CROSS)
    {
        done = true;
        return 0;
    }

    // We'll do the same approach that NotJoinedHash does
    size_t rows_added = 0;
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

    // If we didn't add anything, consider we are done
    if (rows_added == 0)
        done = true;

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
