#pragma once

#include <DB/Storages/MergeTree/MergeSelector.h>


namespace DB
{

class LevelMergeSelector : public IMergeSelector
{
public:
	struct Settings
	{
		size_t min_parts_to_merge = 8;
		size_t max_parts_to_merge = 100;

		time_t lower_base_after = 300;
	};

	LevelMergeSelector(const Settings & settings) : settings(settings) {}

	PartsInPartition select(
		const Partitions & partitions,
		const size_t max_total_size_to_merge) override;

private:
	const Settings settings;
};

}
