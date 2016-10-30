#include <DB/Storages/MergeTree/SimpleMergeSelector.h>

#include <iostream>


namespace DB
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
	void consider(SimpleMergeSelector::PartsInPartition && parts, size_t sum_size)
	{
		double current_score = score(parts.size(), sum_size);
		if (!min_score || current_score < min_score)
		{
			min_score = current_score;
			best = std::move(parts);
		}
	}

	SimpleMergeSelector::PartsInPartition getBest()
	{
		return std::move(best);
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
	SimpleMergeSelector::PartsInPartition best;
};


SimpleMergeSelector::PartsInPartition SimpleMergeSelector::select(
	const Partitions & partitions,
	CanMergeAdjacent can_merge_adjacent,
	const size_t max_total_size_to_merge)
{
	Estimator estimator;

	for (const auto & partition : partitions)
		selectWithinPartition(partition, can_merge_adjacent, max_total_size_to_merge, estimator);

	return estimator.getBest();
}


void SimpleMergeSelector::selectWithinPartition(
	const PartsInPartition & parts,
	CanMergeAdjacent can_merge_adjacent,
	const size_t max_total_size_to_merge,
	Estimator & estimator)
{
	if (parts.size() <= 1)
		return;

	double actual_base = settings.base;

	if (parts.back().age > settings.lower_base_after)
		actual_base = 1;

	std::cerr << "parts.size(): " << parts.size()
		<< ", actual_base: " << actual_base
		<< ", max_total_size_to_merge: " << max_total_size_to_merge
		<< "\n";

	auto prev_right_it = parts.begin();
	for (auto left_it = parts.begin(); left_it != parts.end(); ++left_it)
	{
		auto right_it = left_it;
		auto right_it_minus_one = right_it;
		++right_it;

		size_t sum_size = left_it->size;
		size_t max_size = left_it->size;
		PartsInPartition candidate;
		candidate.push_back(*left_it);

		for (; right_it != parts.end(); ++right_it)
		{
			sum_size += right_it->size;
			if (right_it->size > max_size)
				max_size = right_it->size;

			if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
				break;

			if (settings.max_parts_to_merge_at_once && candidate.size() >= settings.max_parts_to_merge_at_once)
				break;

			if (!can_merge_adjacent(*right_it_minus_one, *right_it))
				break;

			candidate.push_back(*right_it);

			right_it_minus_one = right_it;
		}

		if (candidate.size() <= 1)
			continue;

		if (static_cast<double>(sum_size) / max_size < actual_base)
			continue;

		/// Do not select subrange of previously considered range.
		if (right_it_minus_one <= prev_right_it)
			continue;

		estimator.consider(std::move(candidate), sum_size);

		prev_right_it = right_it_minus_one;
	}
}

}
