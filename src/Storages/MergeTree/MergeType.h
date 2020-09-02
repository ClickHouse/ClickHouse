#pragma once

#include <Core/Types.h>

namespace DB
{

enum class MergeType
{
    NORMAL,
    TTL_DELETE,
    TTL_RECOMPRESS,
};

String toString(MergeType merge_type);

bool isTTLMergeType(MergeType merge_type);

}
