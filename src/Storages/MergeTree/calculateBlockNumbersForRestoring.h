#pragma once

#include <Core/Types.h>
#include <set>


namespace DB
{
struct MergeTreePartInfo;
struct MutationInfoFromBackup;
class MutationCommands;

using AllocateBlockNumbersToRestoreMergeTreeFunction = std::function<Int64(size_t count)>;

/// Calculates new block numbers to restore a MergeTree table.
/// The function also sorts parts and mutations.
void calculateBlockNumbersForRestoringMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<String> & part_names_in_backup,
    std::vector<MutationInfoFromBackup> & mutations,
    std::vector<String> & mutation_names_in_backup,
    AllocateBlockNumbersToRestoreMergeTreeFunction allocate_block_numbers);

using BlockNumbers = std::vector<Int64>;
using MutationNumbers = std::vector<Int64>;

struct PartitionIDAndNumBlockNumbers
{
    String partition_id;
    size_t num_block_numbers = 0;
};

using AllocateBlockNumbersToRestoreReplicatedMergeTreeFunction
    = std::function<std::vector<BlockNumbers>(const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers)>;

using AllocateMutationNumbersToRestoreReplicatedMergeTreeFunction = std::function<MutationNumbers(size_t count)>;

using GetPartitionIdsAffectedByCommandsFunction = std::function<std::set<String>(const MutationCommands & commands)>;

/// Calculates new block numbers and new mutation IDs to restore a ReplicatedMergeTree table.
/// The function also sorts parts and mutations.
void calculateBlockNumbersForRestoringReplicatedMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<String> & part_names_in_backup,
    std::vector<MutationInfoFromBackup> & mutations,
    std::vector<String> & mutation_names_in_backup,
    AllocateBlockNumbersToRestoreReplicatedMergeTreeFunction allocate_block_numbers,
    AllocateMutationNumbersToRestoreReplicatedMergeTreeFunction allocate_mutation_numbers,
    GetPartitionIdsAffectedByCommandsFunction get_partition_ids_affected_by_commands);
}
