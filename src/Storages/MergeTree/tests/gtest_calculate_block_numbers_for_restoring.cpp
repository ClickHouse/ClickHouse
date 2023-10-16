#include <gtest/gtest.h>

#include <Core/Types.h>
#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MutationInfoFromBackup.h>

using namespace DB;


/// Helper functions

namespace
{
    struct ListOfPartsAndMutations
    {
        std::vector<MergeTreePartInfo> part_infos;
        std::vector<MutationInfoFromBackup> mutation_infos;
        std::vector<String> original_part_names;
        std::vector<String> original_mutation_names;
    };

    using StringPairs = std::vector<std::pair<String, String>>;

    ListOfPartsAndMutations prepareListOfPartsAndMutations(const Strings & original_part_names, const StringPairs & original_mutations)
    {
        ListOfPartsAndMutations lst;

        size_t num_parts = original_part_names.size();
        lst.part_infos.reserve(num_parts);
        lst.original_part_names = original_part_names;
        for (const auto & part_name : original_part_names)
            lst.part_infos.emplace_back(MergeTreePartInfo::fromPartName(part_name, ::DB::MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING));

        size_t num_mutations = original_mutations.size();
        lst.mutation_infos.reserve(num_mutations);
        lst.original_mutation_names.reserve(num_mutations);
        for (const auto & [mutation_name, mutation_as_string] : original_mutations)
        {
            lst.original_mutation_names.emplace_back(mutation_name);
            lst.mutation_infos.emplace_back(MutationInfoFromBackup::parseFromString(mutation_as_string, mutation_name));
        }

        return lst;
    }

    Strings toLines(const ListOfPartsAndMutations & lst)
    {
        Strings lines;

        if (lst.part_infos.size() != lst.original_part_names.size())
        {
            lines.push_back("number of parts mismatch");
            return lines;
        }

        if (lst.mutation_infos.size() != lst.original_mutation_names.size())
        {
            lines.push_back("number of mutations mismatch");
            return lines;
        }

        for (size_t i = 0; i != lst.part_infos.size(); ++i)
            lines.push_back(fmt::format("Part #{}: {} -> {}", i + 1, lst.original_part_names[i], lst.part_infos[i].getPartNameForLogs()));

        for (size_t i = 0; i != lst.mutation_infos.size(); ++i)
            lines.push_back(fmt::format("Mutation #{}: {} -> {} ({})", i + 1, lst.original_mutation_names[i],
                            lst.mutation_infos[i].name, lst.mutation_infos[i].toString(/* one_line= */ true)));
        return lines;
    }

    void outputLines(const Strings & lines)
    {
        for (const auto & line : lines)
            std::cout << line << std::endl;
    }

    void checkLines(const Strings & result, const Strings & expected_result)
    {
        EXPECT_EQ(result.size(), expected_result.size());
        for (size_t i = 0; i != result.size(); ++i)
            EXPECT_EQ(result[i], expected_result[i]);
    }


    void checkCalculationForMergeTree(
        const Strings & original_part_names,
        const StringPairs & original_mutations,
        BlockNumbersForRestoringMergeTreeAllocator allocate_block_numbers,
        const Strings & expected_result)
    {
        ListOfPartsAndMutations lst = prepareListOfPartsAndMutations(original_part_names, original_mutations);

        ::DB::calculateBlockNumbersForRestoringMergeTree(lst.part_infos, lst.mutation_infos, allocate_block_numbers);

        Strings result = toLines(lst);

        outputLines(result);
        checkLines(result, expected_result);
    }

    void checkCalculationForReplicatedMergeTree(
        const Strings & original_part_names,
        const StringPairs & original_mutations,
        BlockNumbersForRestoringReplicatedMergeTreeAllocator allocate_block_numbers,
        MutationNumbersForRestoringReplicatedMergeTreeAllocator allocate_mutation_numbers,
        PartitionsAffectedByMutationGetter get_partitions_affected_by_mutation,
        const Strings & expected_result)
    {
        ListOfPartsAndMutations lst = prepareListOfPartsAndMutations(original_part_names, original_mutations);

        ::DB::calculateBlockNumbersForRestoringReplicatedMergeTree(
            lst.part_infos,
            lst.mutation_infos,
            allocate_block_numbers,
            allocate_mutation_numbers,
            get_partitions_affected_by_mutation);

        Strings result = toLines(lst);

        outputLines(result);
        checkLines(result, expected_result);
    }

    Int64 allocateBlockNumbersForMergeTree(size_t)
    {
        return 1;
    }

    std::vector<BlockNumbers> allocateBlockNumbersForReplicatedMergeTree(const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers)
    {
        size_t num_partitions = partitions_and_num_block_numbers.size();
        std::vector<BlockNumbers> res;
        res.resize(num_partitions);
        for (size_t i = 0; i != num_partitions; ++i)
        {
            size_t num_block_numbers = partitions_and_num_block_numbers[i].num_block_numbers;
            res[i].resize(num_block_numbers);
            for (size_t j = 0; j != num_block_numbers; ++j)
                res[i][j] = j;
        }
        return res;
    }

    MutationNumbers allocateMutationNumbers(size_t count)
    {
        MutationNumbers res;
        res.resize(count);
        for (size_t i = 0; i != count; ++i)
            res[i] = i;
        return res;
    }

    std::set<String> getPartitionsAffectedByMutation(const MutationCommands &)
    {
        return {};
    }

    Int64 allocateBlockNumbersForMergeTree_2(size_t count)
    {
        return 100 * count;
    }

    std::vector<BlockNumbers> allocateBlockNumbersForReplicatedMergeTree_2(const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers)
    {
        size_t num_partitions = partitions_and_num_block_numbers.size();
        std::vector<BlockNumbers> res;
        res.resize(num_partitions);
        for (size_t i = 0; i != num_partitions; ++i)
        {
            size_t num_block_numbers = partitions_and_num_block_numbers[i].num_block_numbers;
            res[i].resize(num_block_numbers);
            for (size_t j = 0; j != num_block_numbers; ++j)
                res[i][j] = 1000 * i + 100 * num_block_numbers + j * 3;
        }
        return res;
    }

    MutationNumbers allocateMutationNumbers_2(size_t count)
    {
        MutationNumbers res;
        res.resize(count);
        for (size_t i = 0; i != count; ++i)
            res[i] = 2000 + i * 5;
        return res;
    }
}


/// Actual tests


TEST(CalculateBlockNumbersForRestoring, MergeTree)
{
    auto parts = Strings{
        "1_2_2_0_7",
        "2_4_4_0_9",
        "0_6_6_0_11",
        "1_8_8_0_13",
        "2_10_10_0_11",
        "0_12_12_0_13",
        "1_14_14_0_15",
        "2_16_16_0_17",
        "0_18_18_0",
        "1_20_20_0",
        "2_22_22_0",
    };

    auto mutations = StringPairs{
        { "0000000009.txt", "block number: 9 commands: UPDATE b = concat(b, \'E\', space(sleep(2))) WHERE 1" },
        { "0000000011.txt", "block number: 11 commands: UPDATE b = concat(b, \'F\', space(sleep(2))) WHERE 1" }, 
        { "0000000013.txt", "block number: 13 commands: UPDATE b = concat(b, \'G\', space(sleep(2))) WHERE 1" }, 
        { "0000000015.txt", "block number: 15 commands: UPDATE b = concat(b, \'H\', space(sleep(2))) WHERE 1" }, 
        { "0000000017.txt", "block number: 17 commands: UPDATE b = concat(b, \'I\', space(sleep(2))) WHERE 1" }, 
        { "0000000019.txt", "block number: 19 commands: UPDATE b = concat(b, \'J\', space(sleep(2))) WHERE 1" }, 
        { "0000000021.txt", "block number: 21 commands: UPDATE b = concat(b, \'K\', space(sleep(2))) WHERE 1" },
    };

    auto expected_result = Strings{
        "Part #1: 1_2_2_0_7 -> 1_1_1_0",
        "Part #2: 2_4_4_0_9 -> 2_2_2_0_5",
        "Part #3: 0_6_6_0_11 -> 0_3_3_0_7",
        "Part #4: 1_8_8_0_13 -> 1_4_4_0_9",
        "Part #5: 2_10_10_0_11 -> 2_6_6_0_7",
        "Part #6: 0_12_12_0_13 -> 0_8_8_0_9",
        "Part #7: 1_14_14_0_15 -> 1_10_10_0_11",
        "Part #8: 2_16_16_0_17 -> 2_12_12_0_13",
        "Part #9: 0_18_18_0 -> 0_14_14_0",
        "Part #10: 1_20_20_0 -> 1_16_16_0",
        "Part #11: 2_22_22_0 -> 2_18_18_0",
        "Mutation #1: 0000000009.txt -> mutation_5.txt (block number: 5 commands: UPDATE b = concat(b, \\'E\\', space(sleep(2))) WHERE 1 )",
        "Mutation #2: 0000000011.txt -> mutation_7.txt (block number: 7 commands: UPDATE b = concat(b, \\'F\\', space(sleep(2))) WHERE 1 )",
        "Mutation #3: 0000000013.txt -> mutation_9.txt (block number: 9 commands: UPDATE b = concat(b, \\'G\\', space(sleep(2))) WHERE 1 )",
        "Mutation #4: 0000000015.txt -> mutation_11.txt (block number: 11 commands: UPDATE b = concat(b, \\'H\\', space(sleep(2))) WHERE 1 )",
        "Mutation #5: 0000000017.txt -> mutation_13.txt (block number: 13 commands: UPDATE b = concat(b, \\'I\\', space(sleep(2))) WHERE 1 )",
        "Mutation #6: 0000000019.txt -> mutation_15.txt (block number: 15 commands: UPDATE b = concat(b, \\'J\\', space(sleep(2))) WHERE 1 )",
        "Mutation #7: 0000000021.txt -> mutation_17.txt (block number: 17 commands: UPDATE b = concat(b, \\'K\\', space(sleep(2))) WHERE 1 )",
    };

    checkCalculationForMergeTree(parts, mutations, allocateBlockNumbersForMergeTree, expected_result);

    /// The same parts, a different order.
    auto parts_2 = Strings{
        "1_14_14_0_15",
        "0_18_18_0",
        "0_6_6_0_11",
        "1_2_2_0_7",
        "2_4_4_0_9",
        "1_8_8_0_13",
        "0_12_12_0_13",
        "2_10_10_0_11",
        "2_22_22_0",
        "2_16_16_0_17",
        "1_20_20_0",
    };

    /// The same mutations, a different order.
    auto mutations_2 = StringPairs{
        { "0000000017.txt", "block number: 17 commands: UPDATE b = concat(b, \'I\', space(sleep(2))) WHERE 1" }, 
        { "0000000009.txt", "block number: 9 commands: UPDATE b = concat(b, \'E\', space(sleep(2))) WHERE 1" },
        { "0000000013.txt", "block number: 13 commands: UPDATE b = concat(b, \'G\', space(sleep(2))) WHERE 1" }, 
        { "0000000011.txt", "block number: 11 commands: UPDATE b = concat(b, \'F\', space(sleep(2))) WHERE 1" }, 
        { "0000000015.txt", "block number: 15 commands: UPDATE b = concat(b, \'H\', space(sleep(2))) WHERE 1" }, 
        { "0000000021.txt", "block number: 21 commands: UPDATE b = concat(b, \'K\', space(sleep(2))) WHERE 1" },
        { "0000000019.txt", "block number: 19 commands: UPDATE b = concat(b, \'J\', space(sleep(2))) WHERE 1" }, 
    };

    /// The input order of parts and mutations must be preserved, but block numbers must be calculated in the correct order.
    auto expected_result_2 = Strings{
        "Part #1: 1_14_14_0_15 -> 1_1809_1809_0_1810",
        "Part #2: 0_18_18_0 -> 0_1813_1813_0",
        "Part #3: 0_6_6_0_11 -> 0_1802_1802_0_1806",
        "Part #4: 1_2_2_0_7 -> 1_1800_1800_0",
        "Part #5: 2_4_4_0_9 -> 2_1801_1801_0_1804",
        "Part #6: 1_8_8_0_13 -> 1_1803_1803_0_1808",
        "Part #7: 0_12_12_0_13 -> 0_1807_1807_0_1808",
        "Part #8: 2_10_10_0_11 -> 2_1805_1805_0_1806",
        "Part #9: 2_22_22_0 -> 2_1817_1817_0",
        "Part #10: 2_16_16_0_17 -> 2_1811_1811_0_1812",
        "Part #11: 1_20_20_0 -> 1_1815_1815_0",
        "Mutation #1: 0000000017.txt -> mutation_1812.txt (block number: 1812 commands: UPDATE b = concat(b, \\'I\\', space(sleep(2))) WHERE 1 )",
        "Mutation #2: 0000000009.txt -> mutation_1804.txt (block number: 1804 commands: UPDATE b = concat(b, \\'E\\', space(sleep(2))) WHERE 1 )",
        "Mutation #3: 0000000013.txt -> mutation_1808.txt (block number: 1808 commands: UPDATE b = concat(b, \\'G\\', space(sleep(2))) WHERE 1 )",
        "Mutation #4: 0000000011.txt -> mutation_1806.txt (block number: 1806 commands: UPDATE b = concat(b, \\'F\\', space(sleep(2))) WHERE 1 )",
        "Mutation #5: 0000000015.txt -> mutation_1810.txt (block number: 1810 commands: UPDATE b = concat(b, \\'H\\', space(sleep(2))) WHERE 1 )",
        "Mutation #6: 0000000021.txt -> mutation_1816.txt (block number: 1816 commands: UPDATE b = concat(b, \\'K\\', space(sleep(2))) WHERE 1 )",
        "Mutation #7: 0000000019.txt -> mutation_1814.txt (block number: 1814 commands: UPDATE b = concat(b, \\'J\\', space(sleep(2))) WHERE 1 )",
    };

    /// A different function to allocate block number.
    checkCalculationForMergeTree(parts_2, mutations_2, allocateBlockNumbersForMergeTree_2, expected_result_2);
}


TEST(CalculateBlockNumbersForRestoring, ReplicatedMergeTree)
{
    auto parts = Strings{
        "1_0_0_0_3",
        "1_4_4_0_7",
        "2_0_0_0_3",
        "2_4_4_0_6",
        "0_0_0_0_3",
        "0_4_4_0_6",
        "1_8_8_0_10",
        "2_8_8_0_9",
        "0_8_8_0_9",
        "1_12_12_0",
        "2_12_12_0",
    };

    auto mutations = StringPairs{
        { "0000000004.txt", "block numbers count: 1 1 5 commands: UPDATE b = concat(b, \'E\', space(sleep(2))) WHERE 1" },
        { "0000000005.txt", "block numbers count: 2 1 6 2 5 commands: UPDATE b = concat(b, \'F\', space(sleep(2))) WHERE 1" },
        { "0000000006.txt", "block numbers count: 3 0 5 1 7 2 6 commands: UPDATE b = concat(b, \'G\', space(sleep(2))) WHERE 1" },
        { "0000000007.txt", "block numbers count: 3 0 6 1 9 2 7 commands: UPDATE b = concat(b, \'H\', space(sleep(2))) WHERE 1" },
        { "0000000008.txt", "block numbers count: 3 0 7 1 10 2 9 commands: UPDATE b = concat(b, \'I\', space(sleep(2))) WHERE 1" },
        { "0000000009.txt", "block numbers count: 3 0 9 1 11 2 10 commands: UPDATE b = concat(b, \'J\', space(sleep(2))) WHERE 1" },
        { "0000000010.txt", "block numbers count: 3 0 10 1 13 2 11 commands: UPDATE b = concat(b, \'K\', space(sleep(2))) WHERE 1" },
    };

    auto expected_result = Strings{
        "Part #1: 1_0_0_0_3 -> 1_0_0_0",
        "Part #2: 1_4_4_0_7 -> 1_1_1_0_4",
        "Part #3: 2_0_0_0_3 -> 2_0_0_0",
        "Part #4: 2_4_4_0_6 -> 2_1_1_0_3",
        "Part #5: 0_0_0_0_3 -> 0_0_0_0",
        "Part #6: 0_4_4_0_6 -> 0_1_1_0_3",
        "Part #7: 1_8_8_0_10 -> 1_5_5_0_7",
        "Part #8: 2_8_8_0_9 -> 2_5_5_0_6",
        "Part #9: 0_8_8_0_9 -> 0_5_5_0_6",
        "Part #10: 1_12_12_0 -> 1_9_9_0",
        "Part #11: 2_12_12_0 -> 2_9_9_0",
        "Mutation #1: 0000000004.txt -> 0000000000 (block numbers count: 1 1 2 commands: UPDATE b = concat(b, \\'E\\', space(sleep(2))) WHERE 1 )",
        "Mutation #2: 0000000005.txt -> 0000000001 (block numbers count: 2 1 3 2 2 commands: UPDATE b = concat(b, \\'F\\', space(sleep(2))) WHERE 1 )",
        "Mutation #3: 0000000006.txt -> 0000000002 (block numbers count: 3 0 2 1 4 2 3 commands: UPDATE b = concat(b, \\'G\\', space(sleep(2))) WHERE 1 )",
        "Mutation #4: 0000000007.txt -> 0000000003 (block numbers count: 3 0 3 1 6 2 4 commands: UPDATE b = concat(b, \\'H\\', space(sleep(2))) WHERE 1 )",
        "Mutation #5: 0000000008.txt -> 0000000004 (block numbers count: 3 0 4 1 7 2 6 commands: UPDATE b = concat(b, \\'I\\', space(sleep(2))) WHERE 1 )",
        "Mutation #6: 0000000009.txt -> 0000000005 (block numbers count: 3 0 6 1 8 2 7 commands: UPDATE b = concat(b, \\'J\\', space(sleep(2))) WHERE 1 )",
        "Mutation #7: 0000000010.txt -> 0000000006 (block numbers count: 3 0 7 1 10 2 8 commands: UPDATE b = concat(b, \\'K\\', space(sleep(2))) WHERE 1 )",
    };

    checkCalculationForReplicatedMergeTree(
        parts,
        mutations,
        allocateBlockNumbersForReplicatedMergeTree,
        allocateMutationNumbers,
        getPartitionsAffectedByMutation,
        expected_result);

    auto expected_result_2 = Strings{
        "Part #1: 1_0_0_0_3 -> 1_3100_3100_0",
        "Part #2: 1_4_4_0_7 -> 1_3103_3103_0_3112",
        "Part #3: 2_0_0_0_3 -> 2_2000_2000_0",
        "Part #4: 2_4_4_0_6 -> 2_2003_2003_0_2009",
        "Part #5: 0_0_0_0_3 -> 0_800_800_0",
        "Part #6: 0_4_4_0_6 -> 0_803_803_0_809",
        "Part #7: 1_8_8_0_10 -> 1_3115_3115_0_3121",
        "Part #8: 2_8_8_0_9 -> 2_2015_2015_0_2018",
        "Part #9: 0_8_8_0_9 -> 0_815_815_0_818",
        "Part #10: 1_12_12_0 -> 1_3127_3127_0",
        "Part #11: 2_12_12_0 -> 2_2027_2027_0",
        "Mutation #1: 0000000004.txt -> 0000002000 (block numbers count: 1 1 3106 commands: UPDATE b = concat(b, \\'E\\', space(sleep(2))) WHERE 1 )",
        "Mutation #2: 0000000005.txt -> 0000002005 (block numbers count: 2 1 3109 2 2006 commands: UPDATE b = concat(b, \\'F\\', space(sleep(2))) WHERE 1 )",
        "Mutation #3: 0000000006.txt -> 0000002010 (block numbers count: 3 0 806 1 3112 2 2009 commands: UPDATE b = concat(b, \\'G\\', space(sleep(2))) WHERE 1 )",
        "Mutation #4: 0000000007.txt -> 0000002015 (block numbers count: 3 0 809 1 3118 2 2012 commands: UPDATE b = concat(b, \\'H\\', space(sleep(2))) WHERE 1 )",
        "Mutation #5: 0000000008.txt -> 0000002020 (block numbers count: 3 0 812 1 3121 2 2018 commands: UPDATE b = concat(b, \\'I\\', space(sleep(2))) WHERE 1 )",
        "Mutation #6: 0000000009.txt -> 0000002025 (block numbers count: 3 0 818 1 3124 2 2021 commands: UPDATE b = concat(b, \\'J\\', space(sleep(2))) WHERE 1 )",
        "Mutation #7: 0000000010.txt -> 0000002030 (block numbers count: 3 0 821 1 3130 2 2024 commands: UPDATE b = concat(b, \\'K\\', space(sleep(2))) WHERE 1 )",
    };
    
    checkCalculationForReplicatedMergeTree(
        parts,
        mutations,
        allocateBlockNumbersForReplicatedMergeTree_2,
        allocateMutationNumbers_2,
        getPartitionsAffectedByMutation,
        expected_result_2);
}
