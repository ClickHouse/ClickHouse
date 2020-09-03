#pragma once

#include <Core/Types.h>

namespace DB
{

enum class MergeType
{
    REGULAR,
    TTL_DELETE,
};

MergeType checkAndGetMergeType(UInt64 merge_type);

String toString(MergeType merge_type);

bool isTTLMergeType(MergeType merge_type);

}
