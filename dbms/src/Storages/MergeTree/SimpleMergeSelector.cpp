#include <DB/Storages/MergeTree/SimpleMergeSelector.h>

#include <cmath>


namespace DB
{

struct Estimator
{
	void consider(SimpleMergeSelector::PartsInPartition && parts, size_t sum_size)
	{
		if (!min_size || sum_size < min_size)
		{
			min_size = sum_size;
			best = std::move(parts);
		}
	}

	SimpleMergeSelector::PartsInPartition getBest()
	{
		return std::move(best);
	}

	size_t min_size = 0;
	SimpleMergeSelector::PartsInPartition best;
};


SimpleMergeSelector::PartsInPartition SimpleMergeSelector::select(
	const Partitions & partitions,
	CanMergePart can_merge_part,
	CanMergeAdjacent can_merge_adjacent,
	const size_t max_total_size_to_merge,
	bool aggressive_mode)
{
	Estimator estimator;

	for (const auto & partition : partitions)
		selectWithinPartition(partition, can_merge_part, can_merge_adjacent, max_total_size_to_merge, aggressive_mode, estimator);

	return estimator.getBest();
}


void SimpleMergeSelector::selectWithinPartition(
	const PartsInPartition & parts,
	CanMergePart can_merge_part,
	CanMergeAdjacent can_merge_adjacent,
	const size_t max_total_size_to_merge,
	bool aggressive_mode,
	Estimator & estimator)
{
	if (parts.size() <= 1)
		return;

	for (auto left_it = parts.begin(); left_it != parts.end(); ++left_it)
	{
		if (!can_merge_part(*left_it))
			continue;

		auto right_it = left_it;
		auto prev_right_it = right_it;
		++right_it;

		size_t count = 1;
		size_t sum_size = left_it->size;
		size_t max_size = left_it->size;
		time_t min_age = left_it->age;
		PartsInPartition candidate;
		candidate.push_back(*left_it);

		for (; right_it != parts.end(); ++right_it)
		{
			++count;
			sum_size += right_it->size;
			if (right_it->size > max_size)
				max_size = right_it->size;
			if (right_it->age < min_age)
				min_age = right_it->age;

			double non_uniformity = static_cast<double>(max_size * count) / sum_size;

			if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
				break;

			if (count > settings.max_parts_to_merge_at_once)
				break;

			if (!aggressive_mode && non_uniformity > settings.max_nonuniformity_of_sizes_to_merge)
				break;

			if (!can_merge_part(*right_it))
				break;

			if (!can_merge_adjacent(*prev_right_it, *right_it))
				break;

			candidate.push_back(*right_it);

			prev_right_it = right_it;
		}

		if (count <= 1)
			continue;

		size_t min_parts_to_merge_at_once_with_correction_by_age = settings.min_parts_to_merge_at_once;

		if (min_age >= settings.lower_min_parts_to_merge_at_once_starting_at_time)
		{
			size_t min_parts_to_merge_at_once_correction = 1
				+ std::log(static_cast<double>(min_age) / settings.lower_min_parts_to_merge_at_once_starting_at_time)
					/ std::log(settings.lower_min_parts_to_merge_at_once_base_of_exponent);

			if (min_parts_to_merge_at_once_with_correction_by_age >= min_parts_to_merge_at_once_correction + 2)
				min_parts_to_merge_at_once_with_correction_by_age -= min_parts_to_merge_at_once_correction;
		}

		if (count < min_parts_to_merge_at_once_with_correction_by_age)
			continue;

		estimator.consider(std::move(candidate), sum_size);
	}
}

}
