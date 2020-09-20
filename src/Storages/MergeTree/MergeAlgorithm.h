#pragma once

#include <common/types.h>

namespace DB
{
/// Algorithm of Merge.
enum class MergeAlgorithm
{
    Undecided, /// Not running yet
    Horizontal, /// per-row merge of all columns
    Vertical /// per-row merge of PK and secondary indices columns, per-column gather for non-PK columns
};

String toString(MergeAlgorithm merge_algorithm);

}
