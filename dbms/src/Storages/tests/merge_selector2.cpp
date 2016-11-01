#include <list>
#include <iostream>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/Operators.h>
#include <DB/Storages/MergeTree/SimpleMergeSelector.h>
#include <DB/Storages/MergeTree/LevelMergeSelector.h>


/** This program tests merge-selecting algorithm.
clickhouse-client --query="
 SELECT bytes, now() - modification_time, level, name
 FROM system.parts
 WHERE table = 'visits' AND active AND partition = '201610'" | ./merge_selector
  */

int main(int argc, char ** argv)
{
	using namespace DB;

	IMergeSelector::Partitions partitions(1);
	IMergeSelector::PartsInPartition & parts = partitions.back();

	SimpleMergeSelector::Settings settings;
//	settings.base = 2;
//	settings.max_parts_to_merge_at_once = 10;
	settings.lower_base_after = 30;
	SimpleMergeSelector selector(settings);

/*	LevelMergeSelector::Settings settings;
	settings.min_parts_to_merge = 8;
	settings.max_parts_to_merge = 16;
	LevelMergeSelector selector(settings);*/

	ReadBufferFromFileDescriptor in(STDIN_FILENO);

	size_t sum_parts_size = 0;

	std::list<std::string> part_names;

	while (!in.eof())
	{
		part_names.emplace_back();
		IMergeSelector::Part part;
		in >> part.size >> "\t" >> part.age >> "\t" >> part.level >> "\t" >> part_names.back() >> "\n";
		part.data = part_names.back().data();
		parts.emplace_back(part);
		sum_parts_size += part.size;
	}

	size_t sum_size_written = sum_parts_size;
	size_t num_merges = 1;

	while (parts.size() > 1)
	{
		IMergeSelector::PartsInPartition selected_parts = selector.select(partitions, 0);

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
		<< "Num parts: " << part_names.size() << "\n"
		<< "Num merges: " << num_merges << "\n"
		<< "Tree depth: " << parts.front().level << "\n"
		;

	return 0;
}
