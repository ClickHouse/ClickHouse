#include <DB/Storages/MergeTree/LevelMergeSelector.h>

#include <cmath>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  * It is selected simply: by minimal size.
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
	const time_t current_min_part_age,
	Estimator & estimator,
	const LevelMergeSelector::Settings & settings)
{
	size_t parts_size = parts.size();
	if (parts_size <= 1)
		return;

	/// Will lower 'min_parts_to_merge' if all parts are old enough.
	/// NOTE It is called base, because it is a base of logarithm, that determines merge tree depth.
	double actual_base = settings.min_parts_to_merge;

	if (current_min_part_age > settings.lower_base_after)
	{
		actual_base -= log2(current_min_part_age - settings.lower_base_after);
		if (actual_base < 2)
			actual_base = 2;
	}

	if (parts.size() > settings.fallback_after_num_parts)
		actual_base = 2;

	/// Not enough parts to merge.
	if (parts.size() < actual_base)
		return;

	/// To easily calculate sum size in any range.
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

	/// If no ranges of same level - then nothing to merge
	///  except case when parts are old (much time has passed) and 'base' was lowered to minimum.
	if (!has_range_of_same_level && actual_base > 2)
		return;

	/// For each level, try to select range of parts with that level.
	for (size_t level = 0; level <= max_level; ++level)
	{
		bool in_range = false;
		size_t range_begin = 0;
		size_t range_end = 0;

		for (size_t i = 0; i <= parts_size; ++i)
		{
			/// But if !has_range_of_same_level - it is allowed to select parts with any different levels.
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

					size_t range_length = range_end - range_begin;

					/// Length of range is enough.
					if (range_length >= actual_base)
					{
						/// If length of range is larger than 'max_parts_to_merge' - split it to subranges of almost equal lengths.
						/// For example, if 'max_parts_to_merge' == 100 and 'range_length' = 101, split it to subranges of lengths 50 and 51.
						size_t num_subranges = (range_length + settings.max_parts_to_merge - 1) / settings.max_parts_to_merge;

						for (size_t subrange_index = 0; subrange_index < num_subranges; ++subrange_index)
						{
							size_t subrange_begin = range_begin + subrange_index * range_length / num_subranges;
							size_t subrange_end = range_begin + (subrange_index + 1) * range_length / num_subranges;

							size_t size_of_subrange = prefix_sums[subrange_end] - prefix_sums[subrange_begin];

							/// Don't consider this range if its size is too large.
							if (!max_total_size_to_merge || size_of_subrange <= max_total_size_to_merge)
								estimator.consider(parts.begin() + subrange_begin, parts.begin() + subrange_end, size_of_subrange);
						}
					}
				}
			}
		}

		/// If we don't care of levels, first iteration was enough.
		if (!has_range_of_same_level)
			break;
	}
}

}


LevelMergeSelector::PartsInPartition LevelMergeSelector::select(
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
