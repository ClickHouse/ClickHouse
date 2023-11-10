#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MutationInfoFromBackup.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{
    /// Contains either a part or a mutation. This structure is to make possible to add both parts and mutations to the same vector
    /// and sort them all together (see the implementation of calculateBlockNumbersForRestoringMergeTree()).
    struct PartOrMutation
    {
        /// An instance of `PartOrMutation` contains either a part or a mutation, but never both.
        MergeTreePartInfo * part = nullptr;
        MutationInfoFromBackup * mutation = nullptr;

        /// The sorting key is used to compare two instances of `PartOrMutation` against each other.
        /// It can be either `part->min_block` or `mutation->number` or any block number from the mapping `mutation->block_numbers`.
        Int64 sorting_key = 0;

        explicit PartOrMutation(MergeTreePartInfo & part_info) : part(&part_info), sorting_key(part->min_block) { }

        explicit PartOrMutation(MutationInfoFromBackup & mutation_info) : mutation(&mutation_info), sorting_key(mutation->number) { }

        PartOrMutation(MutationInfoFromBackup & mutation_info, Int64 block_number) : mutation(&mutation_info), sorting_key(block_number) { }

        /// First we compare by block number than by other members of MergeTreePartInfo.
        friend bool operator<(const PartOrMutation & left, const PartOrMutation & right) { return left.sorting_key < right.sorting_key; }

        void setBlockNumber(Int64 block_number) const
        {
            chassert(part);
            part->min_block = block_number;
            part->max_block = block_number;
            part->level = 0;
        }
    };

    /// Checks that the sorting keys used in `left` and `right` are different.
    /// Normally it should be so, because a part cannot reuse a block number used by another part,
    /// and also a mutation cannot reuse a block number used by other mutation or part.
    /// For a ReplicatedMergeTree that's true within any single partition.
    /// The function is used to check parts of a MergeTree or parts within a partition of a ReplicatedMergeTree.
    void checkSortingKeysAreUnique(const PartOrMutation & left, const PartOrMutation & right)
    {
        if (left.sorting_key != right.sorting_key)
            return;

        /// Generate a descriptive error message.
        if (left.part && right.part)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "Read incorrect parts from backup: Two parts share the same min_block: {} and {}",
                            left.part->getPartNameForLogs(), right.part->getPartNameForLogs());
        }
        else if (left.mutation && right.mutation)
        {
            if (left.mutation->number == right.mutation->number)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Read incorrect mutations from backup: Two mutations share the same number: {} ({}) and {} ({})",
                                left.mutation->name, left.mutation->toString(), right.mutation->name, right.mutation->toString());
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Read incorrect mutations from backup: Two mutations share the same block number: {} ({}) and {} ({})",
                                left.mutation->name, left.mutation->toString(), right.mutation->name, right.mutation->toString());
            }
        }
        else if (left.part && right.mutation)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "Read incorrect information from backup: A part and a mutation share the same block number: {} and {} ({})",
                            left.part->getPartNameForLogs(), right.mutation->name, right.mutation->toString());
        }
        else
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "Read incorrect information from backup: A part and a mutation share the same block number: {} and {} ({})",
                            right.part->getPartNameForLogs(), left.mutation->name, left.mutation->toString());
        }
    }

    /// Sorts parts or mutations and also checks that the sorting keys (i.e. block numbers) are unique.
    void sortPartsOrMutations(std::vector<PartOrMutation> & parts_or_mutations)
    {
        std::sort(parts_or_mutations.begin(), parts_or_mutations.end());

        if (parts_or_mutations.size() >= 2)
        {
            for (size_t i = 0; i != parts_or_mutations.size() - 1; ++i)
                checkSortingKeysAreUnique(parts_or_mutations[i], parts_or_mutations[i + 1]);
        }
    }

    /// Old block number and new block number paired together.
    struct OldAndNewBlockNumbers
    {
        Int64 old_block_number;
        Int64 new_block_number;

        friend bool operator<(Int64 left, const OldAndNewBlockNumbers & right)
        {
            return left < right.old_block_number;
        }

#if defined(ABORT_ON_LOGICAL_ERROR) /// The following operator is used only to check sorting.
        friend bool operator<(const OldAndNewBlockNumbers & left, const OldAndNewBlockNumbers & right)
        {
            return left.old_block_number < right.old_block_number;
        }
#endif
    };

    /// Converts an old block number to a new one, this function is used to assign the mutation version number.
    /// If the specified old block number is not found, the function returns a new block number corresponding to the previous old block number,
    /// or zero (if there is no previous old block number).
    /// `old_and_new_block_numbers` must be sorted before calling this function.
    Int64 getNewMutationVersion(const std::vector<OldAndNewBlockNumbers> & old_and_new_block_numbers, Int64 old_mutation_version)
    {
        auto it = std::upper_bound(old_and_new_block_numbers.begin(), old_and_new_block_numbers.end(), old_mutation_version);
        if (it != old_and_new_block_numbers.begin())
        {
            /// Return the new block number corresponding to an equal or lesser `old_block_number`.
            --it;
            return it->new_block_number;
        }

        /// `old_mutation_version` < (every `old_block_number` in `old_and_new_block_numbers`)
        /// That means this mutations was added before any part we're going to restore. So we just return zero here.
        return 0;
    }

    /// Checks that mutations look good for restoring a MergeTree.
    void checkMutationsAreMergeTreeMutations(const std::vector<MutationInfoFromBackup> & mutations)
    {
        for (const auto & mutation : mutations)
        {
            if (mutation.block_numbers)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Read incorrect mutations from the backup: {} ({}) has ReplicatedMergeTree format while the table expect mutations in MergeTree format",
                                mutation.name, mutation.toString());
            }

            if (!mutation.block_number)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Read incorrect mutations from the backup: {} ({}) is in an unknown format without a block number",
                                mutation.name, mutation.toString());
            }

            if (mutation.block_number.value() != mutation.number)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Read incorrect mutations from the backup: {} ({}) has unexpected block number != {}",
                                mutation.name, mutation.toString(), mutation.number);
            }
        }
    }

    /// Formats a mutation name as the name of a corresponding file "mutation_*.txt"
    String formatMutationNameForMergeTree(Int64 mutation_number)
    {
        return fmt::format("mutation_{}.txt", mutation_number);
    }
}


/// Calculates new block numbers to restore a MergeTree table.
///
/// The algorithm is the following:
/// First we add parts and mutations to the same vector and sort them all together.
/// Then we allocate block numbers and assign block numbers to the parts.
/// And finally we assign mutation versions to the parts.
///
/// For example, let's consider a table containing parts "all_7_7_0", "all_9_9_0", "all_1_2_1_8", "all_3_3_0", "all_6_6_0_8", "all_4_4_0_5",
/// and mutations "mutation_8.txt", "mutation_5.txt".
///
/// After the sorting we'll get the following list:
/// "all_1_2_1_8", "all_3_3_0", "all_4_4_0_5", "mutation_5.txt", "all_6_6_0_8", "all_7_7_0", "mutation_8.txt", "all_9_9_0".
///
/// Now we can allocate block numbers and assign them sequentially (the merging level is always set to zero):
/// "all_1_2_1_8"    -> 1 -> "all_1_1_0_8"
/// "all_3_3_0"      -> 2 -> "all_2_2_0"
/// "all_4_4_0_5"    -> 3 -> "all_3_3_0_5"
/// "mutation_5.txt" -> 4 -> "mutation_4.txt"
/// "all_6_6_0_8"    -> 5 -> "all_5_5_0_8"
/// "all_7_7_0"      -> 6 -> "all_6_6_0"
/// "mutation_8.txt" -> 7 -> "mutation_7.txt"
/// "all_9_9_0"      -> 8 -> "all_8_8_0"
///
/// Finally we assign the mutation version for parts "all_1_1_0_8", "all_3_3_0_5" and "all_5_5_0_8".
/// We know at that point that we've calculated block number 4 for "mutation_5.txt" and block number 7 for "mutation_8.txt", so we
/// can assign them to those parts: "all_1_1_0_8" -> "all_1_1_0_7", "all_3_3_0_5" -> "all_3_3_0_4", "all_5_5_0_8" -> "all_5_5_0_7".
///
/// The final result will be:
/// "all_1_2_1_8"    -> 1 -> "all_1_1_0_8" -> "all_1_1_0_7"
/// "all_3_3_0"      -> 2 -> "all_2_2_0"
/// "all_4_4_0_5"    -> 3 -> "all_3_3_0_5" -> "all_3_3_0_4"
/// "mutation_5.txt" -> 4 -> "mutation_4.txt"
/// "all_6_6_0_8"    -> 5 -> "all_5_5_0_8" -> "all_5_5_0_7"
/// "all_7_7_0"      -> 6 -> "all_6_6_0"
/// "mutation_8.txt" -> 7 -> "mutation_7.txt"
/// "all_9_9_0"      -> 8 -> "all_8_8_0"
///
/// Because of any sorting operations are performed on pointers to part infos and mutation infos, not on part infos and mutation infos themselves,
/// the output order will be the same as the input order, i.e.
/// "all_6_6_0", "all_8_8_0", "all_1_1_0_7", "all_2_2_0", "all_5_5_0_7", "all_3_3_0_4", and "mutation_7.txt", "mutation_4.txt".
void calculateBlockNumbersForRestoringMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers)
{
    if (part_infos.empty() && mutations.empty())
        return;

    /// Check that specified mutations are proper MergeTree mutations, i.e. each of them uses only one block number.
    checkMutationsAreMergeTreeMutations(mutations);

    /// Add parts and mutations to a vector to sort them all together.
    std::vector<PartOrMutation> parts_or_mutations;
    parts_or_mutations.reserve(part_infos.size() + mutations.size());

    for (auto & part : part_infos)
        parts_or_mutations.emplace_back(part);

    for (auto & mutation : mutations)
        parts_or_mutations.emplace_back(mutation);

    /// Sort parts and mutations together by block number.
    sortPartsOrMutations(parts_or_mutations);

    /// Allocate block numbers.
    /// Each part takes one block number and each mutation takes one block number.
    Int64 current_block_number = std::move(allocate_block_numbers)(parts_or_mutations.size());

    /// The mapping `old_and_new_mutation_versions` is used to assign the mutation version number for each part (see below).
    std::vector<OldAndNewBlockNumbers> old_and_new_mutation_versions;
    old_and_new_mutation_versions.reserve(mutations.size());

    /// Assign block numbers for each part and each mutation.
    for (auto & part_or_mutation : parts_or_mutations)
    {
        auto new_block_number = current_block_number++;

        if (part_or_mutation.part)
        {
            part_or_mutation.setBlockNumber(new_block_number);
        }
        else
        {
            auto & mutation = *part_or_mutation.mutation;
            Int64 old_block_number = *mutation.block_number;
            mutation.block_number = new_block_number;
            mutation.number = new_block_number;
            mutation.name = formatMutationNameForMergeTree(mutation.number);
            old_and_new_mutation_versions.emplace_back(old_block_number, new_block_number);
        }
    }

    /// Assign the mutation version number for each part using the mapping between old and new block numbers.
    chassert(std::is_sorted(old_and_new_mutation_versions.begin(), old_and_new_mutation_versions.end()));
    for (auto & part : part_infos)
    {
        if (part.mutation)
            part.mutation = getNewMutationVersion(old_and_new_mutation_versions, part.mutation);
    }
}


namespace
{
    /// Formats a mutation name as the name of a corresponding node in ZooKeeper in the "mutations/" subfolder.
    String formatMutationNameForReplicatedMergeTree(Int64 mutation_number)
    {
        return fmt::format("{:010}", mutation_number);
    }

    /// Converts mutations to the ReplicatedMergeTree format, i.e. a block number per each partition.
    /// This conversion is used to restore a MergeTree storage as a ReplicatedMergeTree storage.
    /// For an ordinary MergeTree a single block number is used as a mutation version for each partition, so all we need
    /// to do that conversion is to repeat the single block number for each partition.
    void convertMutationsBlockNumbersToReplicated(
        std::vector<MutationInfoFromBackup> & mutations,
        PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation,
        const std::vector<MergeTreePartInfo> & parts_to_collect_partitions)
    {
        if (mutations.empty())
            return;

        /// Some mutations affect all partitions, whileas some affect only a few partitions.
        bool has_mutations_affecting_all_partitions = false;

        for (auto & mutation : mutations)
        {
            if (mutation.block_number && !mutation.block_numbers)
            {
                auto partitions = get_partitions_affected_by_mutation(mutation.commands);
                if (partitions.empty())
                {
                    has_mutations_affecting_all_partitions = true;
                }
                else
                {
                    /// Set mutation's block numbers.
                    auto & block_numbers = mutation.block_numbers.emplace();
                    for (const auto & partition : partitions)
                        block_numbers[partition] = mutation.block_number.value();
                    mutation.block_number.reset();
                }
            }
        }

        if (!has_mutations_affecting_all_partitions)
            return;

        /// Some mutations affect all the partitions, so we need to collect all partitions in use before updating mutations.
        std::set<String> all_partitions;
        for (const auto & part : parts_to_collect_partitions)
            all_partitions.emplace(part.partition_id);

        for (const auto & mutation : mutations)
        {
            if (mutation.block_numbers)
            {
                for (const auto & [partition, _] : mutation.block_numbers.value())
                    all_partitions.emplace(partition);
            }
        }

        /// Finally we can set block numbers for mutations affecting all the partitions.
        for (auto & mutation : mutations)
        {
            if (mutation.block_number && !mutation.block_numbers)
            {
                /// Set mutation's block numbers.
                auto & block_numbers = mutation.block_numbers.emplace();
                for (const auto & partition : all_partitions)
                    block_numbers[partition] = mutation.block_number.value();
                mutation.block_number.reset();
            }
        }
    }

    /// Calculates new mutation numbers for restoring a ReplicatedMergeTree storage.
    /// For ReplicatedMergeTree mutation numbers and block numbers are allocated separaterely.
    void calculateNewMutationNumbersForReplicated(
        std::vector<MutationInfoFromBackup> & mutations,
        MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers)
    {
        if (mutations.empty())
            return;

        /// Sort mutations by mutation number.
        std::vector<PartOrMutation> sorted_mutations;
        sorted_mutations.reserve(mutations.size());
        for (auto & mutation : mutations)
            sorted_mutations.emplace_back(mutation);

        sortPartsOrMutations(sorted_mutations);

        auto new_mutation_numbers = std::move(allocate_mutation_numbers)(mutations.size());

        /// Assign the new allocated mutation numbers to mutations.
        for (size_t i = 0; i != sorted_mutations.size(); ++i)
        {
            auto & mutation = *sorted_mutations[i].mutation;
            mutation.number = new_mutation_numbers[i];
            mutation.name = formatMutationNameForReplicatedMergeTree(mutation.number);
        }
    }
}


/// Calculates new block numbers and new mutation IDs to restore a ReplicatedMergeTree table.
/// The algorithm is pretty much the same as for an ordinary MergeTree, but with some differences:
/// 1. Mutation numbers are not block numbers anymore, they're calculated separately -
///    we sort mutations by their numbers and then assign new mutation numbers sequentially.
/// 2. Block numbers are assigned for each partition separately.
void calculateBlockNumbersForRestoringReplicatedMergeTree(
    std::vector<MergeTreePartInfo> & part_infos,
    std::vector<MutationInfoFromBackup> & mutations,
    BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
    MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
    PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation)
{
    if (part_infos.empty() && mutations.empty())
        return;

    /// Convert block numbers used in the mutations to the Replicated format, i.e. a block number per each partition.
    convertMutationsBlockNumbersToReplicated(mutations, std::move(get_partitions_affected_by_mutation), part_infos);

    /// Calculate new mutation numbers.
    calculateNewMutationNumbersForReplicated(mutations, std::move(allocate_mutation_numbers));

    /// In the following map we collect parts and mutation for each partitions.
    /// Mutations are collected here multiple times if they affect multiple partitions.
    std::unordered_map<String, std::vector<PartOrMutation>> parts_or_mutations_by_partitions;

    for (auto & part : part_infos)
    {
        const auto & partition = part.partition_id;
        auto & parts_or_mutations = parts_or_mutations_by_partitions[partition];
        parts_or_mutations.emplace_back(part);
    }

    for (auto & mutation : mutations)
    {
        if (mutation.block_numbers)
        {
            for (const auto & [partition, block_number] : mutation.block_numbers.value())
                parts_or_mutations_by_partitions[partition].emplace_back(mutation, block_number);
        }
    }

    /// Allocate block numbers.
    std::vector<PartitionIDAndNumBlockNumbers> block_numbers_alloc_info;
    block_numbers_alloc_info.reserve(parts_or_mutations_by_partitions.size());
    for (const auto & [partition, parts_or_mutations] : parts_or_mutations_by_partitions)
        block_numbers_alloc_info.emplace_back(partition, parts_or_mutations.size());

    auto new_block_numbers_for_all_partitions = std::move(allocate_block_numbers)(block_numbers_alloc_info);
    size_t current_partition_index = 0;

    std::vector<OldAndNewBlockNumbers> old_and_new_mutation_versions;

    /// Assign block numbers to parts and mutations.
    for (auto & [partition, parts_or_mutations] : parts_or_mutations_by_partitions)
    {
        const auto & new_block_numbers = new_block_numbers_for_all_partitions[current_partition_index++];

        /// Sort parts and mutations together for this partition by block number.
        sortPartsOrMutations(parts_or_mutations);

        /// The mapping `old_and_new_mutation_versions` is used to assign the mutation version number for each part (see below).
        old_and_new_mutation_versions.clear();
        old_and_new_mutation_versions.reserve(mutations.size());

        /// Assign block numbers for each part and each mutations.
        for (size_t i = 0; i != parts_or_mutations.size(); ++i)
        {
            auto & part_or_mutation = parts_or_mutations[i];
            auto new_block_number = new_block_numbers[i];

            if (part_or_mutation.part)
            {
                part_or_mutation.setBlockNumber(new_block_number);
            }
            else
            {
                auto & mutation = *part_or_mutation.mutation;
                auto & ref_block_number = mutation.block_numbers->at(partition);
                Int64 old_block_number = ref_block_number;
                ref_block_number = new_block_number;
                old_and_new_mutation_versions.emplace_back(old_block_number, new_block_number);
            }
        }

        /// Assign the mutation version number for each part using the mapping between old and new mutation numbers.
        chassert(std::is_sorted(old_and_new_mutation_versions.begin(), old_and_new_mutation_versions.end()));
        for (auto & part_or_mutation : parts_or_mutations)
        {
            if (part_or_mutation.part)
            {
                auto & part = *part_or_mutation.part;
                if (part.mutation)
                    part.mutation = getNewMutationVersion(old_and_new_mutation_versions, part.mutation);
            }
        }
    }
}

}
