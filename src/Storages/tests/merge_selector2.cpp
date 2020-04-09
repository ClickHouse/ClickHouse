#include <list>
#include <iostream>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/LevelMergeSelector.h>
#include <Common/formatReadable.h>


/** This program tests merge-selecting algorithm.
 * Usage:
clickhouse-client --query="
    SELECT bytes, now() - modification_time, level, name
    FROM system.parts
    WHERE table = 'visits' AND active AND partition = '201610'" | ./merge_selector2
  */

int main(int, char **)
{
    using namespace DB;

    IMergeSelector::Partitions partitions(1);
    IMergeSelector::PartsInPartition & parts = partitions.back();

/*    SimpleMergeSelector::Settings settings;
    SimpleMergeSelector selector(settings);*/

    LevelMergeSelector::Settings settings;
    LevelMergeSelector selector(settings);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    size_t sum_parts_size = 0;

    std::list<std::string> part_names;

    while (!in.eof())
    {
        part_names.emplace_back();
        IMergeSelector::Part part;
        in >> part.size >> "\t" >> part.age >> "\t" >> part.level >> "\t" >> part_names.back() >> "\n";
        part.data = part_names.back().data();
//        part.level = 0;
        parts.emplace_back(part);
        sum_parts_size += part.size;
    }

    size_t total_size_merged = 0;
    size_t sum_size_written = sum_parts_size;
    size_t num_merges = 1;
    size_t age_passed = 0;

    while (parts.size() > 1)
    {
        IMergeSelector::PartsInPartition selected_parts = selector.select(partitions, 100ULL * 1024 * 1024 * 1024);

        if (selected_parts.empty())
        {
            ++age_passed;
            for (auto & part : parts)
                ++part.age;

            if (age_passed > 60 * 86400)
                break;

            if (age_passed % 86400 == 0)
                std::cout << ".";

            continue;
        }
        std::cout << "Time passed: " << age_passed << '\n';

        size_t sum_merged_size = 0;
        size_t start_index = 0;
        size_t max_level = 0;
        bool in_range = false;

        for (size_t i = 0, size = parts.size(); i < size; ++i)
        {
            if (parts[i].data == selected_parts.front().data)
            {
                std::cout << "\033[1;31m";
                in_range = true;
                start_index = i;
            }

            std::cout << (parts[i].size / 1024) << "_" << parts[i].level;
            if (in_range)
            {
                sum_merged_size += parts[i].size;
                if (parts[i].level > max_level)
                    max_level = parts[i].level;
            }

            if (parts[i].data == selected_parts.back().data)
            {
                in_range = false;
                std::cout << "\033[0m";
            }

            std::cout << " ";
        }

        parts[start_index].size = sum_merged_size;
        parts[start_index].level = max_level + 1;
        parts[start_index].age = 0;
        parts.erase(parts.begin() + start_index + 1, parts.begin() + start_index + selected_parts.size());

        std::cout << '\n';

        sum_size_written += sum_merged_size;
        total_size_merged += sum_merged_size;

        ++num_merges;

        double time_to_merge = sum_merged_size / (1048576 * 10.0);

        age_passed += time_to_merge;
        for (auto & part : parts)
            part.age += time_to_merge;

        std::cout << "Time passed: " << age_passed << ", num parts: " << parts.size()
            << ", merged " << selected_parts.size() << " parts, " << formatReadableSizeWithBinarySuffix(sum_merged_size)
            << ", total written: " << formatReadableSizeWithBinarySuffix(total_size_merged) << '\n';
    }

    std::cout << std::fixed << std::setprecision(2)
        << "Write amplification: " << static_cast<double>(sum_size_written) / sum_parts_size << "\n"
        << "Num parts: " << part_names.size() << "\n"
        << "Num merges: " << num_merges << "\n"
        << "Tree depth: " << parts.front().level << "\n"
        ;

    return 0;
}
