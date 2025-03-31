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
    bool flag_per_row;

    // We'll keep a position in the single final map
    // or we might need to store an iterator if we do "ALL" join
    std::any position;
    bool done = false;
};

}
