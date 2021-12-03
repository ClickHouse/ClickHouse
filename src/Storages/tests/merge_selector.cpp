#include <iostream>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>


/** This program tests merge-selecting algorithm.
  * Usage:
  * ./merge_selector <<< "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"
  * clickhouse-client --query="SELECT 100 + round(10 * rand() / 0xFFFFFFFF) FROM system.numbers LIMIT 105" | tr '\n' ' ' | ./merge_selector
  */

int main(int, char **)
{
    using namespace DB;

    IMergeSelector::PartsRanges partitions(1);
    IMergeSelector::PartsRange & parts = partitions.back();

    SimpleMergeSelector::Settings settings;
//    settings.base = 2;
//    settings.max_parts_to_merge_at_once = 10;
    SimpleMergeSelector selector(settings);

/*    LevelMergeSelector::Settings settings;
    settings.min_parts_to_merge = 8;
    settings.max_parts_to_merge = 16;
    LevelMergeSelector selector(settings);*/

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    size_t sum_parts_size = 0;

    while (!in.eof())
    {
        size_t size = 0;
        readText(size, in);
        skipWhitespaceIfAny(in);

        IMergeSelector::Part part;
        part.size = size;
        part.age = 0;
        part.level = 0;
        part.data = reinterpret_cast<const void *>(parts.size());

        parts.emplace_back(part);

        sum_parts_size += size;
    }

    size_t sum_size_written = sum_parts_size;
    size_t num_merges = 1;

    while (parts.size() > 1)
    {
        IMergeSelector::PartsRange selected_parts = selector.select(partitions, 0);

        if (selected_parts.empty())
        {
            std::cout << '.';
            for (auto & part : parts)
                ++part.age;
            continue;
        }
        std::cout << '\n';

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

            std::cout << parts[i].size;
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
        ++num_merges;
    }

    std::cout << std::fixed << std::setprecision(2)
        << "Write amplification: " << static_cast<double>(sum_size_written) / sum_parts_size << "\n"
        << "Num merges: " << num_merges << "\n"
        << "Tree depth: " << parts.front().level << "\n"
        ;

    return 0;
}
