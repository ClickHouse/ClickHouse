#pragma once

#include <Core/Types.h>

namespace DB
{

enum class MergeType
{
    NORMAL,
    FINAL,
    TTL_DELETE,
    TTL_RECOMPRESS,
};

String toString(MergeType merge_type);

}
