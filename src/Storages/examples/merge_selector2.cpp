#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/Operators.h>

#include <Common/formatReadable.h>

#include <iomanip>
#include <iostream>
#include <ostream>

/** This program tests merge-selecting algorithm.
 * Usage:
clickhouse-client --query="
    SELECT name, bytes, now() - modification_time
    FROM system.parts
    WHERE table = 'visits' AND active AND partition = '201610'" | ./merge_selector2
  */

int main(int, char **)
{
    using namespace DB;

    PartsRanges ranges(1);
    PartsRange & parts = ranges.back();

    SimpleMergeSelector::Settings settings;
    SimpleMergeSelector selector(settings);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    size_t sum_parts_size = 0;
    size_t num_parts = 0;

    while (!in.eof())
    {
        size_t size;
        time_t age;
        std::string name;

        in >> name >> "\t" >> size >> "\t" >> age >> "\n";

        auto part_info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        parts.push_back(PartProperties
        {
            .name = name,
            .info = part_info,
            .size = size,
            .age = age,
        });

        sum_parts_size += size;
        ++num_parts;
    }

    size_t total_size_merged = 0;
    size_t sum_size_written = sum_parts_size;
    size_t num_merges = 1;
    size_t age_passed = 0;

    while (parts.size() > 1)
    {
        PartsRange selected_parts = selector.select(ranges, 100ULL * 1024 * 1024 * 1024, nullptr);

        if (selected_parts.empty())
        {
            ++age_passed;

            if (age_passed > 60 * 86400)
                break;

            if (age_passed % 86400 == 0)
            {
                std::cout << ".";
                std::cout.flush();
            }

            PartsRange next_range;
            for (auto & part : parts)
            {
                next_range.push_back(PartProperties
                {
                    .name = part.name,
                    .info = part.info,
                    .size = part.size,
                    .age = part.age + 1,
                });
            }
            parts.swap(next_range);

            continue;
        }
        std::cout << "Time passed: " << age_passed << "\n";

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

            std::cout << part.name << "___" << (part.size / 1024);
            if (in_range)
            {
                sum_merged_size += part.size;
                max_level = std::max(part.info.level, max_level);
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
        total_size_merged += sum_merged_size;
        ++num_merges;

        double time_to_merge = sum_merged_size / (1048576 * 10.0);
        age_passed = static_cast<size_t>(age_passed + time_to_merge);

        {
            next_range.clear();
            for (auto & part : parts)
            {
                next_range.push_back(PartProperties
                {
                    .name = part.name,
                    .info = part.info,
                    .size = part.size,
                    .age = static_cast<time_t>(part.age + time_to_merge),
                });
            }
            parts.swap(next_range);
        }

        std::cout
            << "Time passed: " << age_passed << ", "
            << "num parts: " << parts.size() << ", "
            << "merged " << selected_parts.size() << " parts, " << formatReadableSizeWithBinarySuffix(sum_merged_size) << ", "
            << "total written: " << formatReadableSizeWithBinarySuffix(total_size_merged) << "\n";
    }

    std::cout << "\n";
    std::cout << std::fixed << std::setprecision(2)
        << "Write amplification: " << static_cast<double>(sum_size_written) / sum_parts_size << "\n"
        << "Num parts: " << num_parts << "\n"
        << "Num merges: " << num_merges << "\n";

    for (const auto & part : parts)
        std::cout << part.name << ", size: " << part.size << "\n";

    return 0;
}
