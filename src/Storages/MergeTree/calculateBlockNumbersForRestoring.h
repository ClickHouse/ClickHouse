#pragma once

#include <Core/Types.h>
#include <set>


namespace DB
{
struct MergeTreePartInfo;
struct MutationInfoFromBackup;
class MutationCommands;

using BlockNumbersForRestoringMergeTreeAllocator = std::function<Int64(size_t count)>;

/// Calculates new block numbers to restore a MergeTree table.
/// This function recalculates block numbers in `part_infos` and `mutations` using a passed function for allocating block numbers.
/// The input order of `part_infos` and `mutations` is arbitrary (`part_infos` and `mutations` don't have to be sorted),
/// and this functions preserves that order (i.e. the output order is the same as the input order).
void calculateBlockNumbersForRestoringMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers);

using BlockNumbers = std::vector<Int64>;
using MutationNumbers = std::vector<Int64>;

struct PartitionIDAndNumBlockNumbers
{
    String partition_id;
    size_t num_block_numbers = 0;
};

using BlockNumbersForRestoringReplicatedMergeTreeAllocator
    = std::function<std::vector<BlockNumbers>(const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers)>;

using MutationNumbersForRestoringReplicatedMergeTreeAllocator = std::function<MutationNumbers(size_t count)>;

using PartitionsAffectedByMutationGetter = std::function<std::set<String>(const MutationCommands & commands)>;

/// Calculates new block numbers and new mutation IDs to restore a ReplicatedMergeTree table.
/// This function recalculates block numbers in `part_infos` and `mutations` using passed functions for allocating block numbers and mutation numbers.
/// The input order of `part_infos` and `mutations` is arbitrary (`part_infos` and `mutations` don't have to be sorted),
/// and this functions preserves that order (i.e. the output order is the same as the input order).
void calculateBlockNumbersForRestoringReplicatedMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
    MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
    PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation);
}
