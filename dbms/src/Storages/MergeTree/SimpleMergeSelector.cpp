#include <DB/Storages/MergeTree/SimpleMergeSelector.h>

#include <iostream>


namespace DB
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
	using Iterator = SimpleMergeSelector::PartsInPartition::const_iterator;

	void consider(Iterator begin, Iterator end, size_t sum_size)
	{
		double current_score = score(end - begin, sum_size);
		if (!min_score || current_score < min_score)
		{
			min_score = current_score;
			best_begin = begin;
			best_end = end;
		}
	}

	SimpleMergeSelector::PartsInPartition getBest()
	{
		return SimpleMergeSelector::PartsInPartition(best_begin, best_end);
	}

	static double score(double count, double sum_size)
	{
		/** Consider we have two alternative ranges of data parts to merge.
		  * Assume time to merge a range is proportional to sum size of its parts.
		  *
		  * Cost of query execution is proportional to total number of data parts in a moment of time.
		  * Let define our target: to minimize average (in time) total number of data parts.
		  *
		  * Let calculate integral of total number of parts, if we are going to do merge of one or another range.
		  * It must be lower, and thus we decide, what range is better to merge.
		  *
		  * The integral is lower iff the following formula is lower:
		  */
		return sum_size / (count - 1);
	}

	double min_score = 0;
	Iterator best_begin;
	Iterator best_end;
};


SimpleMergeSelector::PartsInPartition SimpleMergeSelector::select(
	const Partitions & partitions,
	const size_t max_total_size_to_merge)
{
	Estimator estimator;

	for (const auto & partition : partitions)
		selectWithinPartition(partition, max_total_size_to_merge, estimator);

	return estimator.getBest();
}


void SimpleMergeSelector::selectWithinPartition(
	const PartsInPartition & parts,
	const size_t max_total_size_to_merge,
	Estimator & estimator)
{
	if (parts.size() <= 1)
		return;

	double actual_base = settings.base;

	if (parts.back().age > settings.lower_base_after)
		actual_base = 1;

	if (parts.size() < actual_base)
		return;

	std::cerr << "parts.size(): " << parts.size()
		<< ", actual_base: " << actual_base
		<< ", max_total_size_to_merge: " << max_total_size_to_merge
		<< "\n";

	size_t parts_count = parts.size();
	size_t prefix_sum = 0;
	std::vector<size_t> prefix_sums(parts.size() + 1);

	for (size_t i = 0; i < parts_count; ++i)
	{
		prefix_sum += parts[i].size;
		prefix_sums[i + 1] = prefix_sum;
	}

	for (size_t begin = 0; begin < parts_count; ++begin)
	{
		for (size_t end = begin + 1 + actual_base; end <= parts_count; ++end)
		{
			if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)
				break;

			size_t sum_size = prefix_sums[end] - prefix_sums[begin];

			if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
				break;

			estimator.consider(parts.begin() + begin, parts.begin() + end, sum_size);
		}
	}
}

}
