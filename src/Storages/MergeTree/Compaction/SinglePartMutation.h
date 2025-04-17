#pragma once
#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

struct SinglePartMutationChoise
{
    enum class Type
    {
        TTL_DROP,
        TTL_MATERIALIZE,
        APPLY_DELETED_MASK,
    };

    Type type;
    PartProperties part;
};

}
