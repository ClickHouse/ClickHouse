#pragma once

namespace DB
{
    enum class MergeTreeDataPartType
    {
        WIDE,
        COMPACT,
        IN_MEMORY,
        UNKNOWN,
    };
}
