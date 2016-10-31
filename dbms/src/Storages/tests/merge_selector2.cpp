#include <list>
#include <iostream>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/Operators.h>
#include <DB/Storages/MergeTree/SimpleMergeSelector.h>
#include <DB/Storages/MergeTree/LevelMergeSelector.h>


/** This program tests merge-selecting algorithm.
  * Pass parts properties to stdin: (size, age, level, name) in tab separated format,
  *  and it will select and print parts to merge.
  */

int main(int argc, char ** argv)
{
	using namespace DB;

	IMergeSelector::Partitions partitions(1);
	IMergeSelector::PartsInPartition & parts = partitions.back();

	LevelMergeSelector::Settings settings;
	LevelMergeSelector selector(settings);

	ReadBufferFromFileDescriptor in(STDIN_FILENO);

	std::list<std::string> part_names;

	while (!in.eof())
	{
		part_names.emplace_back();
		IMergeSelector::Part part;
		in >> part.size >> "\t" >> part.age >> "\t" >> part.level >> "\t" >> part_names.back() >> "\n";
		part.data = part_names.back().data();
		parts.emplace_back(part);
	}

	IMergeSelector::PartsInPartition selected_parts = selector.select(partitions, 0);

	for (const auto & part : selected_parts)
		std::cout << static_cast<const char *>(part.data) << ' ';
	std::cout << '\n';

	return 0;
}
