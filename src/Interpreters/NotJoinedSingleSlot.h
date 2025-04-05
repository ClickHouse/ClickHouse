#pragma once

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Common/Arena.h>
#include <base/types.h>
#include <any>

namespace DB
{

class ConcurrentHashJoin;
class HashJoin;

/// NotJoinedConcurrentHash implements RightColumnsFiller for RIGHT JOINs.
/// It is similar to NotJoinedHash from HashJoin.cpp, but adapted for concurrency.
class NotJoinedSingleSlot final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedSingleSlot(size_t slot_index_, const ConcurrentHashJoin & join_, UInt64 max_block_size_, bool flag_per_row_)
        : slot_index(slot_index_), join(join_), max_block_size(max_block_size_), flag_per_row(flag_per_row_)
    {
    }

    Block getEmptyBlock() override;
    size_t fillColumns(MutableColumns & columns_right) override;

    template <typename MapType>
    size_t fillFromMap(DB::HashJoin & hash_join, MapType & map, MutableColumns & columns_right);

private:
    size_t slot_index;
    const ConcurrentHashJoin & join;
    UInt64 max_block_size;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;
    std::optional<HashJoin::ScatteredBlocksList::const_iterator> used_position;
    size_t current_block_start = 0; // For partial reading of a block
    bool flag_per_row;

    size_t fillColumnsFromData(const HashJoin::ScatteredBlocksList & blocks, MutableColumns & columns_right);
    size_t fillUsedFlagsRowByRow(const HashJoin & hash_join, MutableColumns & columns_right);
    size_t fillNullsFromBlocks(const HashJoin & hash_join, MutableColumns & columns_right, size_t rows_already_added);

    // We'll keep a position in the single final map
    // or we might need to store an iterator if we do "ALL" join
    std::any position;
    bool done = false;
};

}
