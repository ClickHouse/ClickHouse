#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>

#include <iomanip>
#include <iostream>

/** This program tests merge-selecting algorithm.
  * Usage:
  * ./merge_selector <<< "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"
  * clickhouse-client --query="SELECT 100 + round(10 * rand() / 0xFFFFFFFF) FROM system.numbers LIMIT 105" | tr "\n" ' ' | ./merge_selector
  */

int main(int, char **)
{
    using namespace DB;

    PartsRanges ranges(1);
    PartsRange & parts = ranges.back();

    // SimpleMergeSelector::Settings settings;
    // settings.base = 2;
    // settings.max_parts_to_merge_at_once = 10;
    // SimpleMergeSelector selector(settings);

    TrivialMergeSelector selector;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    size_t sum_parts_size = 0;

    while (!in.eof())
    {
        size_t size = 0;
        readText(size, in);
        skipWhitespaceIfAny(in);

        auto part_info = MergeTreePartInfo::fromPartName(fmt::format("all_{}_{}_0", parts.size(), parts.size()), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        parts.emplace_back(PartProperties
        {
            .name = part_info.getPartNameV1(),
            .info = part_info,
            .size = size,
            .age = 0,
        });

        sum_parts_size += size;
    }

    size_t sum_size_written = sum_parts_size;
    size_t num_merges = 1;

    while (parts.size() > 1)
    {
        PartsRange selected_parts = selector.select(ranges, 0, nullptr);

        if (selected_parts.empty())
        {
            // std::cout << '.';
            // for (auto & part : parts)
            //     ++part.age;
            // continue;*/

            break;
        }

        size_t sum_merged_size = 0;
        int64_t min_block = 0;
        int64_t max_block = 0;
        uint32_t max_level = 0;
        bool in_range = false;

        PartsRange next_range;
        for (const auto & part : parts)
        {
            if (part.name == selected_parts.front().name)
            {
                std::cout << "\033[1;31m";

                in_range = true;
                min_block = part.info.min_block;
            }

            std::cout << part.size;
            if (in_range)
            {
                sum_merged_size += part.size;
                max_level = std::max(max_level, part.info.level);
            }
            else
            {
                next_range.push_back(part);
            }

            if (part.name == selected_parts.back().name)
            {
                in_range = false;
                max_block = part.info.max_block;

                auto part_info = MergeTreePartInfo::fromPartName(fmt::format("all_{}_{}_{}", min_block, max_block, max_level + 1), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
                next_range.push_back(PartProperties
                {
                    .name = part_info.getPartNameV1(),
                    .info = part_info,
                    .size = sum_merged_size,
                    .age = 0,
                });

                std::cout << "\033[0m";
            }

            std::cout << " ";
        }
        std::cout << "\n";

        parts.swap(next_range);

        sum_size_written += sum_merged_size;
        ++num_merges;
    }

    std::cout << "\n";
    std::cout << std::fixed << std::setprecision(2)
        << "Write amplification: " << static_cast<double>(sum_size_written) / sum_parts_size << "\n"
        << "Num merges: " << num_merges << "\n";

    for (const auto & part : parts)
        std::cout << part.name << ", size: " << part.size << "\n";

    return 0;
}
