#include <DB/Storages/MergeTree/SimpleMergeSelector.h>

#include <cmath>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
	using Iterator = SimpleMergeSelector::PartsInPartition::const_iterator;

	void consider(Iterator begin, Iterator end, size_t sum_size, size_t size_next_at_left, size_t size_next_at_right)
	{
		double current_score = score(end - begin, sum_size);

		if (size_next_at_left > sum_size * 0.9)
		{
			double difference = std::abs(log2(static_cast<double>(sum_size) / size_next_at_left));
			if (difference < 0.5)
				current_score *= 0.75 + difference * 0.5;
		}

		if (size_next_at_right == 0)
			current_score *= 0.9;

		if (size_next_at_right > sum_size * 0.9)
		{
			double difference = std::abs(log2(static_cast<double>(sum_size) / size_next_at_right));
			if (difference < 0.5)
				current_score *= 0.75 + difference * 0.5;
		}

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


void selectWithinPartition(
	const SimpleMergeSelector::PartsInPartition & parts,
	const size_t max_total_size_to_merge,
	const time_t current_min_part_age,
	Estimator & estimator,
	const SimpleMergeSelector::Settings & settings)
{
	if (parts.size() <= 1)
		return;

	double actual_base = settings.base;

	if (current_min_part_age > settings.lower_base_after)
	{
		actual_base -= log2(current_min_part_age - settings.lower_base_after);
		if (actual_base < 1)
			actual_base = 1;
	}

	if (parts.size() <= actual_base)
		return;

	size_t parts_count = parts.size();
	size_t prefix_sum = 0;
	std::vector<size_t> prefix_sums(parts.size() + 1);

	for (size_t i = 0; i < parts_count; ++i)
	{
		prefix_sum += parts[i].size;
		prefix_sums[i + 1] = prefix_sum;
	}

	Estimator local_estimator;

	for (size_t begin = 0; begin < parts_count; ++begin)
	{
		for (size_t end = begin + 1 + actual_base; end <= parts_count; ++end)
		{
			if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)
				break;

			size_t sum_size = prefix_sums[end] - prefix_sums[begin];

			if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
				break;

			local_estimator.consider(
				parts.begin() + begin,
				parts.begin() + end,
				sum_size,
				begin == 0 ? 0 : parts[begin - 1].size,
				end == parts_count ? 0 : parts[end].size);
		}
	}

	{
		size_t sum_size = 0;
		size_t max_size = 0;

		for (auto it = local_estimator.best_begin; it != local_estimator.best_end; ++it)
		{
			sum_size += it->size;
			if (it->size > max_size)
				max_size = it->size;
		}

		if (static_cast<double>(sum_size) / max_size >= actual_base)
			estimator.consider(local_estimator.best_begin, local_estimator.best_end, sum_size, 0, 0);
	}
}

}


SimpleMergeSelector::PartsInPartition SimpleMergeSelector::select(
	const Partitions & partitions,
	const size_t max_total_size_to_merge)
{
	time_t min_age = -1;
	for (const auto & partition : partitions)
		for (const auto & part : partition)
			if (min_age == -1 || part.age < min_age)
				min_age = part.age;

	Estimator estimator;

	for (const auto & partition : partitions)
		selectWithinPartition(partition, max_total_size_to_merge, min_age, estimator, settings);

	return estimator.getBest();
}

}
