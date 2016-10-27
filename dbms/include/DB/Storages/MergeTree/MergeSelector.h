#pragma once

#include <cstddef>
#include <vector>
#include <functional>


namespace DB
{

class IMergeSelector
{
public:
	struct Part
	{
		size_t size;
		time_t age;
		unsigned level;

		const void * data;
	};

	using PartsInPartition = std::vector<Part>;
	using Partitions = std::vector<PartsInPartition>;

	using CanMergePart = std::function<bool(const Part &)>;
	using CanMergeAdjacent = std::function<bool(const Part &, const Part &)>;

	virtual PartsInPartition select(
		const Partitions & partitions,
		CanMergePart can_merge_part,
		CanMergeAdjacent can_merge_adjacent,
		const size_t max_total_size_to_merge,
		bool aggressive_mode) = 0;

	virtual ~IMergeSelector() {}
};

}
