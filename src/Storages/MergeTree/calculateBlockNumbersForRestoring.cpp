#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MutationInfoFromBackup.h>
#include <base/apply_permutation.h>

#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{
    /// Sort mutations by mutation number. A bigger mutation number means the mutation was created later.
    void sortMutations(std::vector<MutationInfoFromBackup> & mutations, std::vector<String> & mutation_names_in_backup)
    {
        size_t count = mutations.size();
        chassert(mutation_names_in_backup.size() == count);

        /// Prepare list of permutations so we can apply it two vectors `mutations` and `mutation_names_in_backup`.
        std::vector<size_t> permutation;
        permutation.resize(count);
        std::iota(permutation.begin(), permutation.end(), 0);

        std::sort(
            permutation.begin(),
            permutation.end(),
            [&](size_t left_index, size_t right_index)
            {
                const auto & left = mutations[left_index];
                const auto & right = mutations[right_index];
                return left.number < right.number;
            });

        apply_permutation(mutations, mutation_names_in_backup, std::move(permutation));
    }

    struct PartMutations
    {
        /// Index of the first mutation this part is applied to or should be applied to.
        ///
        /// For example, let's consider a MergeTree with six parts "all_1_2_1_8", "all_3_3_0", "all_4_4_0_5", "all_6_6_0_8", "all_7_7_0", "all_9_9_0"
        /// and two mutations "mutation_5.txt", "mutation_8.txt". For those parts `first_mutation` and `num_applied_mutations` will be calculated as:
        /// | part_name   | first_mutation | num_applied_mutations |
        /// | all_1_2_1_8 | 0 (mutation_5) |                     2 |
        /// | all_3_3_0   | 0              |                     0 |
        /// | all_4_4_0_5 | 0              |                     1 |
        /// | all_6_6_0_8 | 1 (mutation_8) |                     1 |
        /// | all_7_7_0   | 1              |                     0 |
        /// | all_9_9_0   | 2 (none)       |                     0 |
        ///
        size_t first_mutation = 0;

        /// Number of mutations this part is already applied to.
        size_t num_applied_mutations = 0;
    };

    /// Find corresponding mutations for each part.
    std::vector<PartMutations>
    findMutationsForEachPart(const std::vector<MergeTreePartInfo> & parts, const std::vector<MutationInfoFromBackup> & mutations)
    {
        std::vector<PartMutations> res;
        res.resize(parts.size());

        if (mutations.empty())
        {
            /// No mutations.
        }
        else if (mutations.front().block_number)
        {
            /// Mutations are specified with a single `block_number` per a mutation.

            /// Make a map of mutations where mutations are ordered by their block numbers.
            /// This map needs to be ordered because we use upper_bound() below.
            std::map<Int64, size_t> mutations_by_block_number;

            for (size_t i = 0; i != mutations.size(); ++i)
            {
                const auto & mutation = mutations[i];

                /// MergeTree and ReplicatedMergeTree mutations can't be mixed.
                if (!mutation.block_number || mutation.block_numbers)
                    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Mutations specify incorrect block numbers");
                Int64 block_number = *mutation.block_number;

                /// The block number specified in a mutation must increase with following mutations.
                if (i > 0)
                {
                    Int64 previous_block_number = *mutations[i - 1].block_number;
                    if (block_number <= previous_block_number)
                        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Mutations specify incorrect block numbers");
                }

                mutations_by_block_number[block_number] = i;
            }

            /// Use the map of mutations to calculate `first_mutation` and `num_applied_mutations` for each part.
            for (size_t i = 0; i != parts.size(); ++i)
            {
                const auto & part_info = parts[i];
                auto & first_mutation = res[i].first_mutation;
                auto & num_applied_mutations = res[i].num_applied_mutations;

                auto it = mutations_by_block_number.upper_bound(part_info.min_block);
                if (it == mutations_by_block_number.end())
                {
                    /// For all mutations `mutation_info.block_number < part_info.min_block`.
                    first_mutation = mutations.size();
                }
                else
                {
                    /// We've found the first mutation for which `mutation_info.block_number > part_info.min_block`.
                    first_mutation = it->second;
                    if (part_info.mutation > part_info.min_block)
                    {
                        it = mutations_by_block_number.upper_bound(part_info.mutation);
                        if (it == mutations_by_block_number.end())
                            num_applied_mutations = mutations.size() - first_mutation;
                        else
                            num_applied_mutations = it->second - first_mutation;
                    }
                }
            }
        }
        else
        {
            /// Mutations are specified with multiple block numbers - each partition has its own block number.

            /// Make a map of mutations where mutations are ordered by their block numbers.
            /// The nested map needs to be ordered because we use upper_bound() below.
            std::unordered_map<String, std::map<Int64, size_t>> mutation_by_partition_and_block_number;

            for (size_t i = 0; i != mutations.size(); ++i)
            {
                const auto & mutation = mutations[i];

                /// MergeTree and ReplicatedMergeTree mutations can't be mixed.
                if (!mutation.block_numbers || mutation.block_number)
                    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Mutations specify incorrect block numbers");
                const auto & block_numbers = *mutation.block_numbers;

                /// The block number specified in a mutation must increase with following mutations.
                if (i > 0)
                {
                    const auto & previos_block_numbers = *mutations[i - 1].block_numbers;
                    for (const auto & [partition_id, previous_block_number] : previos_block_numbers)
                    {
                        auto it = block_numbers.find(partition_id);
                        if (it != block_numbers.end())
                        {
                            Int64 block_number = it->second;
                            if (block_number <= previous_block_number)
                                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Mutations specify incorrect block numbers");
                        }
                    }
                }

                for (const auto & [partition_id, block_number] : block_numbers)
                    mutation_by_partition_and_block_number[partition_id][block_number] = i;
            }

            /// Use the map of mutations to calculate `first_mutation` and `num_applied_mutations` for each part.
            for (size_t i = 0; i != parts.size(); ++i)
            {
                const auto & part_info = parts[i];
                const auto & partition_id = part_info.partition_id;
                auto & first_mutation = res[i].first_mutation;
                auto & num_applied_mutations = res[i].num_applied_mutations;

                auto it_by_partition = mutation_by_partition_and_block_number.find(partition_id);
                if (it_by_partition == mutation_by_partition_and_block_number.end())
                {
                    /// The partition is not found in the map of mutation, that means the part was added after all the mutations.
                    first_mutation = mutations.size();
                }
                else
                {
                    const auto & mutations_by_block_number = it_by_partition->second;
                    auto it = mutations_by_block_number.upper_bound(part_info.min_block);
                    if (it == mutations_by_block_number.end())
                    {
                        /// For all mutations `mutation_info.block_number < part_info.min_block`.
                        first_mutation = mutations.size();
                    }
                    else
                    {
                        /// We've found the first mutation for which `mutation_info.block_number > part_info.min_block`.
                        first_mutation = it->second;
                        if (part_info.mutation > part_info.min_block)
                        {
                            it = mutations_by_block_number.upper_bound(part_info.mutation);
                            if (it == mutations_by_block_number.end())
                                num_applied_mutations = mutations.size() - first_mutation;
                            else
                                num_applied_mutations = it->second - first_mutation;
                        }
                    }
                }
            }
        }

        return res;
    }

    /// Sort parts by `first_mutation`, then by `min_block`.
    /// This order corresponds to the order in which mutations and parts could be attached.
    void sortParts(std::vector<MergeTreePartInfo> & part_infos, std::vector<PartMutations> & parts_mutations, std::vector<String> & part_names_in_backup)
    {
        size_t count = part_infos.size();
        chassert((parts_mutations.size() == count) && (part_names_in_backup.size() == count));

        /// Prepare list of permutations so we can apply it three vectors `part_infos` and `parts_mutations` and `part_names_in_backup`.
        std::vector<size_t> permutation;
        permutation.resize(count);
        std::iota(permutation.begin(), permutation.end(), 0);

        std::sort(
            permutation.begin(),
            permutation.end(),
            [&](size_t left_index, size_t right_index)
            {
                const auto & left_prm = parts_mutations[left_index];
                const auto & right_prm = parts_mutations[right_index];
                if (left_prm.first_mutation != right_prm.first_mutation)
                    return left_prm.first_mutation < right_prm.first_mutation;
                Int64 left_min_block = part_infos[left_index].min_block;
                Int64 right_min_block = part_infos[right_index].min_block;
                return left_min_block < right_min_block;
            });

        apply_permutation(part_infos, parts_mutations, part_names_in_backup, std::move(permutation));
    }

    /// Helper class designed to calculate new block numbers.
    class BlockNumbersCalculator
    {
    public:
        BlockNumbersCalculator(
            std::vector<MergeTreePartInfo> & part_infos_,
            std::vector<String> & part_names_in_backup_,
            std::vector<MutationInfoFromBackup> & mutations_,
            std::vector<String> & mutation_names_in_backup_);

        void calculateForMergeTree(AllocateBlockNumbersToRestoreMergeTreeFunction allocate_block_numbers);

        void calculateForReplicatedMergeTree(
            AllocateBlockNumbersToRestoreReplicatedMergeTreeFunction allocate_block_numbers,
            AllocateMutationNumbersToRestoreReplicatedMergeTreeFunction allocate_mutation_numbers,
            GetPartitionIdsAffectedByCommandsFunction get_partition_ids_affected_by_commands);

    private:
        std::vector<MergeTreePartInfo> & parts;
        std::vector<PartMutations> parts_mutations;
        std::vector<MutationInfoFromBackup> & mutations;
    };

    BlockNumbersCalculator::BlockNumbersCalculator(
        std::vector<MergeTreePartInfo> & part_infos_, std::vector<String> & part_names_in_backup_,
        std::vector<MutationInfoFromBackup> & mutations_, std::vector<String> & mutation_names_in_backup_)
        : parts(part_infos_), mutations(mutations_)
    {
        sortMutations(mutations, mutation_names_in_backup_);
        parts_mutations = findMutationsForEachPart(parts, mutations);
        sortParts(parts, parts_mutations, part_names_in_backup_);
    }

    /// Calculate new block numbers for a MergeTree table.
    void BlockNumbersCalculator::calculateForMergeTree(AllocateBlockNumbersToRestoreMergeTreeFunction allocate_block_numbers)
    {
        /// Allocate block numbers.
        /// Each part takes one block number and each mutation takes one block number.
        Int64 num_block_numbers = parts.size() + mutations.size();
        if (!num_block_numbers)
            return;
        Int64 start_block_number = std::move(allocate_block_numbers)(num_block_numbers);
        Int64 current_block_number = start_block_number;

        /// For each mutation first we handle parts this mutation should be applied to, then we handle the mutation itself.
        size_t part_index = 0;
        for (size_t mutation_index = 0; mutation_index != mutations.size(); ++mutation_index)
        {
            /// Parts are ordered by `first_mutation`, see the constructor of `BlockNumbersCalculator`.
            for (; (part_index != parts.size()) && (parts_mutations[part_index].first_mutation <= mutation_index); ++part_index)
            {
                auto & part_info = parts[part_index];
                part_info.min_block = current_block_number++;
                part_info.max_block = part_info.min_block;
                part_info.level = 0;
                part_info.mutation = 0;
            }
            auto & mutation = mutations[mutation_index];
            mutation.number = current_block_number++;
            mutation.name = fmt::format("mutation_{}.txt", mutation.number);
            mutation.block_number = mutation.number;
            mutation.block_numbers.reset();
        }

        /// All mutations were handled, now we have to handle parts added after the last mutation.
        for (; part_index != parts.size(); ++part_index)
        {
            auto & part_info = parts[part_index];
            part_info.min_block = current_block_number++;
            part_info.max_block = part_info.min_block;
            part_info.level = 0;
            part_info.mutation = 0;
        }

        /// All allocated block numbers must be used by now.
        chassert (start_block_number + num_block_numbers == current_block_number);

        /// Adjust `part_info.mutation` by means of `num_mutations_applied`.
        for (part_index = 0; part_index != parts.size(); ++part_index)
        {
            if (size_t num_applied_mutations = parts_mutations[part_index].num_applied_mutations)
            {
                auto & part_info = parts[part_index];
                size_t first_mutation = parts_mutations[part_index].first_mutation;
                part_info.mutation = *mutations[first_mutation + num_applied_mutations - 1].block_number;
            }
        }
    }

    /// Calculate new block numbers and new mutation IDs for a ReplicatedMergeTree table.
    void BlockNumbersCalculator::calculateForReplicatedMergeTree(
        AllocateBlockNumbersToRestoreReplicatedMergeTreeFunction allocate_block_numbers,
        AllocateMutationNumbersToRestoreReplicatedMergeTreeFunction allocate_mutation_numbers,
        GetPartitionIdsAffectedByCommandsFunction get_partition_ids_affected_by_commands)
    {
        /// Find affected partitions for each mutation.
        /// Some mutations affect all existing partitions, some mutations affect only a subset of partitions.
        /// In order to allocate block numbers we have to find out first which partitions each mutation affects.
        std::vector<std::set<String>> partitions_by_mutation;
        partitions_by_mutation.reserve(mutations.size());
        for (const auto & mutation : mutations)
            partitions_by_mutation.emplace_back(get_partition_ids_affected_by_commands(mutation.commands));

        /// For each mutation we find parts this mutation should be applied to, in order to collect partition ids.
        std::set<String> partitions;
        size_t part_index = 0;
        for (size_t mutation_index = 0; mutation_index != mutations.size(); ++mutation_index)
        {
            /// Parts are ordered by `first_mutation`, see the constructor of `BlockNumbersCalculator`.
            for (; (part_index != parts.size()) && (parts_mutations[part_index].first_mutation <= mutation_index); ++part_index)
            {
                const auto & part_info = parts[part_index];
                partitions.emplace(part_info.partition_id);
            }

            if (partitions_by_mutation[mutation_index].empty())
            {
                partitions_by_mutation[mutation_index] = partitions;
            }
            else
            {
                std::copy(
                    partitions_by_mutation[mutation_index].begin(),
                    partitions_by_mutation[mutation_index].end(),
                    std::inserter(partitions, partitions.end()));
            }
        }

        struct PartitionBlocks
        {
            size_t num_block_numbers = 0;
            std::vector<Int64> block_numbers;
            size_t current_block_number = 0;
        };

        std::unordered_map<String, PartitionBlocks> blocks_by_partition;

        /// Calculate how many block numbers we need for each partition.
        for (const auto & part_info : parts)
            ++blocks_by_partition[part_info.partition_id].num_block_numbers;

        for (const auto & partition_ids : partitions_by_mutation)
            for (const auto & partition_id : partition_ids)
                ++blocks_by_partition[partition_id].num_block_numbers;

        /// Allocate block numbers.
        {
            std::vector<PartitionIDAndNumBlockNumbers> partitions_and_num_block_numbers;
            partitions_and_num_block_numbers.reserve(blocks_by_partition.size());
            for (auto & [partition_id, blocks] : blocks_by_partition)
                partitions_and_num_block_numbers.emplace_back(PartitionIDAndNumBlockNumbers{partition_id, blocks.num_block_numbers});
            auto allocation_result = allocate_block_numbers(partitions_and_num_block_numbers);
            size_t i = 0;
            for (auto & [partition_id, blocks] : blocks_by_partition)
                blocks.block_numbers = std::move(allocation_result[i++]);
        }

        /// Allocate mutation numbers.
        std::vector<Int64> mutation_numbers = allocate_mutation_numbers(mutations.size());
        size_t current_mutation_number = 0;

        /// For each mutation first we handle parts this mutation should be applied to, then we handle the mutation itself.
        part_index = 0;
        for (size_t mutation_index = 0; mutation_index != mutations.size(); ++mutation_index)
        {
            /// Parts are ordered by `first_mutation`, see the constructor of `BlockNumbersCalculator`.
            for (; (part_index != parts.size()) && (parts_mutations[part_index].first_mutation <= mutation_index); ++part_index)
            {
                auto & part_info = parts[part_index];
                auto & blocks = blocks_by_partition.at(part_info.partition_id);
                part_info.min_block = blocks.block_numbers[blocks.current_block_number++];
                part_info.max_block = part_info.min_block;
                part_info.level = 0;
                part_info.mutation = 0;
            }

            auto & mutation = mutations[mutation_index];
            mutation.number = mutation_numbers[current_mutation_number++];
            mutation.name = fmt::format("{:010}", mutation.number);
            mutation.block_number.reset();
            auto & block_numbers = mutation.block_numbers.emplace();
            for (const auto & partition_id : partitions_by_mutation[mutation_index])
            {
                auto & blocks = blocks_by_partition.at(partition_id);
                block_numbers[partition_id] = blocks.block_numbers[blocks.current_block_number++];
            }
        }

        /// All mutations were handled, now we have to handle parts added after the last mutation.
        for (; part_index != parts.size(); ++part_index)
        {
            auto & part_info = parts[part_index];
            auto & blocks = blocks_by_partition.at(part_info.partition_id);
            part_info.min_block = blocks.block_numbers[blocks.current_block_number++];
            part_info.max_block = part_info.min_block;
            part_info.level = 0;
            part_info.mutation = 0;
        }

        /// All allocated block numbers must be used by now.
#if defined(ABORT_ON_LOGICAL_ERROR)
        chassert(current_mutation_number == mutations.size());
        for (auto & [_, blocks] : blocks_by_partition)
            chassert(blocks.current_block_number == blocks.num_block_numbers);
#endif

        /// Adjust `part_info.mutation` by means of `num_mutations_applied`.
        for (part_index = 0; part_index != parts.size(); ++part_index)
        {
            if (size_t num_applied_mutations = parts_mutations[part_index].num_applied_mutations)
            {
                size_t first_mutation = parts_mutations[part_index].first_mutation;
                auto & part_info = parts[part_index];
                const auto & block_numbers = *mutations[first_mutation + num_applied_mutations - 1].block_numbers;
                auto it = block_numbers.find(part_info.partition_id);
                if (it != block_numbers.end())
                    part_info.mutation = it->second;
            }
        }
    }
}

void calculateBlockNumbersForRestoringMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    Strings & part_names_in_backup,
    std::vector<MutationInfoFromBackup> & mutations,
    Strings & mutation_names_in_backup,
    AllocateBlockNumbersToRestoreMergeTreeFunction allocate_block_numbers)
{
    BlockNumbersCalculator{part_infos, part_names_in_backup, mutations, mutation_names_in_backup}.calculateForMergeTree(allocate_block_numbers);
}

void calculateBlockNumbersForRestoringReplicatedMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    Strings & part_names_in_backup,
    std::vector<MutationInfoFromBackup> & mutations,
    Strings & mutation_names_in_backup,
    AllocateBlockNumbersToRestoreReplicatedMergeTreeFunction allocate_block_numbers,
    AllocateMutationNumbersToRestoreReplicatedMergeTreeFunction allocate_mutation_numbers,
    GetPartitionIdsAffectedByCommandsFunction get_partition_ids_affected_by_commands)
{
    BlockNumbersCalculator{part_infos, part_names_in_backup, mutations, mutation_names_in_backup}.calculateForReplicatedMergeTree(
        allocate_block_numbers, allocate_mutation_numbers, get_partition_ids_affected_by_commands);
}
}
