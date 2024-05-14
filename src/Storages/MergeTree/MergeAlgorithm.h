#pragma once

#include <base/types.h>

namespace DB
{
/// Algorithm of Merge.
enum class MergeAlgorithm : uint8_t
{
    Undecided, /// Not running yet
    Horizontal, /// per-row merge of all columns
    Vertical /// per-row merge of PK and secondary indices columns, per-column gather for non-PK columns
};

}
