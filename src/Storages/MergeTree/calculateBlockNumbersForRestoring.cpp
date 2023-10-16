#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MutationInfoFromBackup.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{
    /// Sort mutations by mutation number. A bigger mutation number means the mutation was created later.
    void sortMutations(std::vector<MutationInfoFromBackup *> & mutations)
    {
        std::sort(
            mutations.begin(),
            mutations.end(),
            [&](MutationInfoFromBackup * left, MutationInfoFromBackup * right) { return left->number < right->number; });
    }

    struct PartMutations
    {
        MergeTreePartInfo * part_info = nullptr;

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

    /// Find corresponding mutations for each part - in case the mutations are specified with single block numbers.
    /// It means the mutations are in the MergeTree format.
    void findMutationsForEachPart_MergeTree(std::vector<PartMutations> & parts, const std::vector<MutationInfoFromBackup *> & mutations)
    {
        /// Make a map of mutations where mutations are ordered by their block numbers.
        std::vector<std::pair<Int64, size_t>> mutations_by_block_number;
        mutations_by_block_number.reserve(mutations.size());

        for (size_t i = 0; i != mutations.size(); ++i)
        {
            const auto & mutation = *mutations[i];

            /// MergeTree and ReplicatedMergeTree mutations can't be mixed.
            if (mutation.block_numbers)
            {
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                "Read incorrect mutations from the backup: {} has MergeTree format and {} has ReplicatedMergeTree format",
                                mutations.front()->name, mutation.name);
            }

            if (!mutation.block_number)
            {
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                "Read incorrect mutations from the backup: {} is in an unknown format without a block number",
                                mutation.name);
            }

            Int64 block_number = *mutation.block_number;

            /// The block number specified in a mutation must increase with following mutations.
            if (!mutations_by_block_number.empty())
            {
                Int64 previous_block_number = mutations_by_block_number.back().first;
                if (block_number <= previous_block_number)
                {
                    const auto & previous_mutation = *mutations[mutations_by_block_number.back().second];
                    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                    "Read incorrect mutations from the backup: "
                                    "Next mutation {} has a non-increased block number ({}) compared with a previous one {} ({})",
                                    mutation.name, block_number, previous_mutation.name, previous_block_number);
                }
            }

            mutations_by_block_number.emplace_back(block_number, i);
        }

        /// Returns the index of the first mutation with the block number greater than a specified value.
        auto find_mutation_by_block_number = [&](Int64 block_number)
        {
            auto it = std::upper_bound(
                mutations_by_block_number.begin(),
                mutations_by_block_number.end(),
                block_number,
                [](Int64 x, const std::pair<Int64, size_t> & y) { return x < y.first; });
            if (it == mutations_by_block_number.end())
                return mutations.size();
            return it->second;
        };

        /// Use the map of mutations to calculate `first_mutation` and `num_applied_mutations` for each part.
        for (auto & part : parts)
        {
            const auto & part_info = *part.part_info;
            auto & first_mutation = part.first_mutation;
            auto & num_applied_mutations = part.num_applied_mutations;

            first_mutation = find_mutation_by_block_number(part_info.min_block);

            if ((part_info.mutation > part_info.min_block) && (first_mutation < mutations.size()))
                num_applied_mutations = find_mutation_by_block_number(part_info.mutation) - first_mutation;
        }
    }

    /// Find corresponding mutations for each part - in case the mutations are specified with multiple block numbers for each partitions.
    /// It means the mutations are in the ReplicatedMergeTree format.
    void findMutationsForEachPart_ReplicatedMergeTree(std::vector<PartMutations> & parts, const std::vector<MutationInfoFromBackup *> & mutations)
    {
        /// Make a map of mutations where mutations are ordered by their block numbers.
        std::unordered_map<String, std::vector<std::pair<Int64, size_t>>> mutations_by_partition_and_block_number;

        for (size_t i = 0; i != mutations.size(); ++i)
        {
            const auto & mutation = *mutations[i];

            /// MergeTree and ReplicatedMergeTree mutations can't be mixed.
            if (mutation.block_number)
            {
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                "Read incorrect mutations from the backup: {} has MergeTree format and {} has ReplicatedMergeTree format",
                                mutation.name, mutations.front()->name);
            }

            if (!mutation.block_numbers)
            {
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                "Read incorrect mutations from the backup: {} is in an unknown format without a block number",
                                mutation.name);
            }

            const auto & block_numbers = *mutation.block_numbers;

            for (const auto & [partition_id, block_number] : block_numbers)
            {
                auto & mutations_by_block_number = mutations_by_partition_and_block_number[partition_id];
                mutations_by_block_number.reserve(mutations.size());

                /// The block number specified in a mutation must increase with following mutations.
                if (!mutations_by_block_number.empty())
                {
                    Int64 previous_block_number = mutations_by_block_number.back().first;
                    if (block_number <= previous_block_number)
                    {
                        const auto & previous_mutation = *mutations[mutations_by_block_number.back().second];
                        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                                        "Read incorrect mutations from the backup: "
                                        "Next mutation {} has a non-increased block number ({}) compared with a previous one {} ({}) in partition {}",
                                        mutation.name, block_number, previous_mutation.name, previous_block_number, partition_id);
                    }
                }

                mutations_by_block_number.emplace_back(block_number, i);
            }
        }

        /// Use the map of mutations to calculate `first_mutation` and `num_applied_mutations` for each part.
        for (auto & part : parts)
        {
            const auto & part_info = *part.part_info;
            const auto & partition_id = part_info.partition_id;
            auto & first_mutation = part.first_mutation;
            auto & num_applied_mutations = part.num_applied_mutations;

            auto it_by_partition = mutations_by_partition_and_block_number.find(partition_id);
            if (it_by_partition == mutations_by_partition_and_block_number.end())
            {
                /// The partition is not found in the map of mutation, that means the part was added after all the mutations.
                first_mutation = mutations.size();
            }
            else
            {
                const auto & mutations_by_block_number = it_by_partition->second;

                /// Returns the index of the first mutation with the block number greater than a specified value.
                auto find_mutation_by_block_number = [&](Int64 block_number)
                {
                    auto it = std::upper_bound(
                        mutations_by_block_number.begin(),
                        mutations_by_block_number.end(),
                        block_number,
                        [](Int64 x, const std::pair<Int64, size_t> & y) { return x < y.first; });
                    if (it == mutations_by_block_number.end())
                        return mutations.size();
                    return it->second;
                };

                first_mutation = find_mutation_by_block_number(part_info.min_block);

                if ((part_info.mutation > part_info.min_block) && (first_mutation < mutations.size()))
                    num_applied_mutations = find_mutation_by_block_number(part_info.mutation) - first_mutation;
            }
        }
    }

    /// Find corresponding mutations for each part.
    void findMutationsForEachPart(std::vector<PartMutations> & parts, const std::vector<MutationInfoFromBackup *> & mutations)
    {
        if (mutations.empty())
            return;

        /// Single block number corresponds to a MergeTree's mutations and multiple block numbers correspond to a ReplicatedMergeTree's mutations.
        const auto & single_block_number = mutations.front()->block_number.has_value();
        if (single_block_number)
            findMutationsForEachPart_MergeTree(parts, mutations);
        else
            findMutationsForEachPart_ReplicatedMergeTree(parts, mutations);
    }

    /// Sort parts by `first_mutation`, then by `min_block`.
    /// This order corresponds to the order in which mutations and parts could be attached.
    void sortParts(std::vector<PartMutations> & parts)
    {
        std::sort(
            parts.begin(),
            parts.end(),
            [&](const PartMutations & left, const PartMutations & right)
            {
                if (left.first_mutation != right.first_mutation)
                    return left.first_mutation < right.first_mutation;
                const auto & left_part_info = *left.part_info;
                const auto & right_part_info = *right.part_info;
                if (left_part_info.min_block != right_part_info.min_block)
                    return left_part_info.min_block < right_part_info.min_block;
                return left_part_info < right_part_info;
            });
    }

    /// Helper class designed to calculate new block numbers.
    class BlockNumbersCalculator
    {
    public:
        BlockNumbersCalculator(std::vector<MergeTreePartInfo> & part_infos_, std::vector<MutationInfoFromBackup> & mutations_);

        void calculateForMergeTree(BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers);

        void calculateForReplicatedMergeTree(
            BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
            MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
            PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation);

    private:
        std::vector<PartMutations> parts;
        std::vector<MutationInfoFromBackup *> mutations;
    };

    BlockNumbersCalculator::BlockNumbersCalculator(
        std::vector<MergeTreePartInfo> & part_infos_, std::vector<MutationInfoFromBackup> & mutations_)
    {
        parts.resize(part_infos_.size());
        for (size_t i = 0; i != part_infos_.size(); ++i)
            parts[i].part_info = &part_infos_[i];

        mutations.resize(mutations_.size());
        for (size_t i = 0; i != mutations_.size(); ++i)
            mutations[i] = &mutations_[i];

        sortMutations(mutations);
        findMutationsForEachPart(parts, mutations);
        sortParts(parts);
    }

    /// Calculate new block numbers for a MergeTree table.
    void BlockNumbersCalculator::calculateForMergeTree(BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers)
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
            for (; (part_index != parts.size()) && (parts[part_index].first_mutation <= mutation_index); ++part_index)
            {
                auto & part_info = *parts[part_index].part_info;
                part_info.min_block = current_block_number++;
                part_info.max_block = part_info.min_block;
                part_info.level = 0;
                part_info.mutation = 0;
            }
            auto & mutation = *mutations[mutation_index];
            mutation.number = current_block_number++;
            mutation.name = fmt::format("mutation_{}.txt", mutation.number);
            mutation.block_number = mutation.number;
            mutation.block_numbers.reset();
        }

        /// All mutations were handled, now we have to handle parts added after the last mutation.
        for (; part_index != parts.size(); ++part_index)
        {
            auto & part_info = *parts[part_index].part_info;
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
            if (size_t num_applied_mutations = parts[part_index].num_applied_mutations)
            {
                auto & part_info = *parts[part_index].part_info;
                size_t first_mutation = parts[part_index].first_mutation;
                part_info.mutation = *mutations[first_mutation + num_applied_mutations - 1]->block_number;
            }
        }
    }

    /// Calculate new block numbers and new mutation IDs for a ReplicatedMergeTree table.
    void BlockNumbersCalculator::calculateForReplicatedMergeTree(
        BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
        MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
        PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation)
    {
        /// Find affected partitions for each mutation.
        /// Some mutations affect all existing partitions, some mutations affect only a subset of partitions.
        /// In order to allocate block numbers we have to find out first which partitions each mutation affects.
        std::vector<std::set<String>> partitions_by_mutation;
        partitions_by_mutation.reserve(mutations.size());
        for (const auto & mutation : mutations)
            partitions_by_mutation.emplace_back(get_partitions_affected_by_mutation(mutation->commands));

        /// For each mutation we find parts this mutation should be applied to, in order to collect partition ids.
        std::set<String> partitions;
        size_t part_index = 0;
        for (size_t mutation_index = 0; mutation_index != mutations.size(); ++mutation_index)
        {
            /// Parts are ordered by `first_mutation`, see the constructor of `BlockNumbersCalculator`.
            for (; (part_index != parts.size()) && (parts[part_index].first_mutation <= mutation_index); ++part_index)
            {
                const auto & part_info = *parts[part_index].part_info;
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
        for (const auto & part : parts)
        {
            const auto & part_info = *part.part_info;
            ++blocks_by_partition[part_info.partition_id].num_block_numbers;
        }

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
            for (; (part_index != parts.size()) && (parts[part_index].first_mutation <= mutation_index); ++part_index)
            {
                auto & part_info = *parts[part_index].part_info;
                auto & blocks = blocks_by_partition.at(part_info.partition_id);
                part_info.min_block = blocks.block_numbers[blocks.current_block_number++];
                part_info.max_block = part_info.min_block;
                part_info.level = 0;
                part_info.mutation = 0;
            }

            auto & mutation = *mutations[mutation_index];
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
            auto & part_info = *parts[part_index].part_info;
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
            if (size_t num_applied_mutations = parts[part_index].num_applied_mutations)
            {
                size_t first_mutation = parts[part_index].first_mutation;
                auto & part_info = *parts[part_index].part_info;
                const auto & block_numbers = *mutations[first_mutation + num_applied_mutations - 1]->block_numbers;
                auto it = block_numbers.find(part_info.partition_id);
                if (it != block_numbers.end())
                    part_info.mutation = it->second;
            }
        }
    }
}

void calculateBlockNumbersForRestoringMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers)
{
    BlockNumbersCalculator{part_infos, mutations}.calculateForMergeTree(allocate_block_numbers);
}

void calculateBlockNumbersForRestoringReplicatedMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
    MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
    PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation)
{
    BlockNumbersCalculator{part_infos, mutations}.calculateForReplicatedMergeTree(
        allocate_block_numbers, allocate_mutation_numbers, get_partitions_affected_by_mutation);
}
}
