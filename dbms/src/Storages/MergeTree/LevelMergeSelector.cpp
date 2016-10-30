#include <DB/Storages/MergeTree/LevelMergeSelector.h>

#include <cmath>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
	using Iterator = LevelMergeSelector::PartsInPartition::const_iterator;

	void consider(Iterator begin, Iterator end, size_t sum_size)
	{
		double current_score = sum_size;

		if (!min_score || current_score < min_score)
		{
			min_score = current_score;
			best_begin = begin;
			best_end = end;
		}
	}

	LevelMergeSelector::PartsInPartition getBest()
	{
		return LevelMergeSelector::PartsInPartition(best_begin, best_end);
	}

	double min_score = 0;
	Iterator best_begin;
	Iterator best_end;
};


void selectWithinPartition(
	const LevelMergeSelector::PartsInPartition & parts,
	const size_t max_total_size_to_merge,
	Estimator & estimator,
	const LevelMergeSelector::Settings & settings)
{
	size_t parts_size = parts.size();
	if (parts_size <= 1)
		return;

	double actual_base = settings.min_parts_to_merge;

	if (parts.back().age > settings.lower_base_after)
	{
		actual_base -= log2(parts.back().age - settings.lower_base_after);
		if (actual_base < 2)
			actual_base = 2;
	}

	if (parts.size() < actual_base)
		return;

	size_t parts_count = parts.size();
	size_t prefix_sum = 0;
	std::vector<size_t> prefix_sums(parts.size() + 1);

	for (size_t i = 0; i < parts_count; ++i)
	{
		prefix_sum += parts[i].size;
		prefix_sums[i + 1] = prefix_sum;
	}

	size_t max_level = 0;
	size_t prev_level = -1;
	bool has_range_of_same_level = false;

	for (const auto & part : parts)
	{
		if (part.level > max_level)
			max_level = part.level;

		if (part.level == prev_level)
			has_range_of_same_level = true;

		prev_level = part.level;
	}

	for (size_t level = 0; level <= max_level; ++level)
	{
		bool in_range = false;
		size_t range_begin = 0;
		size_t range_end = 0;

		for (size_t i = 0; i <= parts_size; ++i)
		{
			if (i < parts_size && (parts[i].level == level || !has_range_of_same_level))
			{
				if (!in_range)
				{
					in_range = true;
					range_begin = i;
				}
			}
			else
			{
				if (in_range)
				{
					in_range = false;
					range_end = i;

					size_t range_size = range_end - range_begin;

					if (range_size >= actual_base)
					{
						size_t num_subranges = (range_size + settings.max_parts_to_merge - 1) / settings.max_parts_to_merge;

						for (size_t subrange_index = 0; subrange_index < num_subranges; ++subrange_index)
						{
							size_t subrange_begin = range_begin + subrange_index * range_size / num_subranges;
							size_t subrange_end = range_begin + (subrange_index + 1) * range_size / num_subranges;

							size_t size_of_subrange = prefix_sums[subrange_end] - prefix_sums[subrange_begin];

							if (size_of_subrange <= max_total_size_to_merge)
								estimator.consider(parts.begin() + subrange_begin, parts.begin() + subrange_end, size_of_subrange);
						}
					}
				}
			}
		}

		if (!has_range_of_same_level)
			break;
	}
}

}


LevelMergeSelector::PartsInPartition LevelMergeSelector::select(
	const Partitions & partitions,
	const size_t max_total_size_to_merge)
{
	Estimator estimator;

	for (const auto & partition : partitions)
		selectWithinPartition(partition, max_total_size_to_merge, estimator, settings);

	return estimator.getBest();
}

}
