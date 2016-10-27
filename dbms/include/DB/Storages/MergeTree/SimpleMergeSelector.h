#pragma once

#include <DB/Storages/MergeTree/MergeSelector.h>


namespace DB
{

struct Estimator;

class SimpleMergeSelector : public IMergeSelector
{
public:
	struct Settings
	{
		size_t min_parts_to_merge_at_once = 8;
		size_t max_parts_to_merge_at_once = 100;

		double max_nonuniformity_of_sizes_to_merge = 2;

		time_t lower_min_parts_to_merge_at_once_starting_at_time = 300;
		size_t lower_min_parts_to_merge_at_once_base_of_exponent = 4;
	};

	SimpleMergeSelector(const Settings & settings) : settings(settings) {}

	PartsInPartition select(
		const Partitions & partitions,
		CanMergePart can_merge_part,
		CanMergeAdjacent can_merge_adjacent,
		const size_t max_total_size_to_merge,
		bool aggressive_mode) override;

private:
	const Settings settings;

	void selectWithinPartition(
		const PartsInPartition & parts,
		CanMergePart can_merge_part,
		CanMergeAdjacent can_merge_adjacent,
		const size_t max_total_size_to_merge,
		bool aggressive_mode,
		Estimator & estimator);
};

}
