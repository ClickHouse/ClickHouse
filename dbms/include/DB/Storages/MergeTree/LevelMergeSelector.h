#pragma once

#include <DB/Storages/MergeTree/MergeSelector.h>


namespace DB
{

/** Select parts to merge based on its level.
 *
  * Select first range of parts at least min_parts_to_merge length with minimum level.
  *
  * If enough time has passed, lower min_parts_to_merge.
  * And if no ranges of consecutive parts with same level, and much time has passed,
  *  allow to select parts of different level.
  * This is done to allow further merging when table is not updated.
  */
class LevelMergeSelector : public IMergeSelector
{
public:
	struct Settings
	{
		size_t min_parts_to_merge = 8;
		size_t max_parts_to_merge = 100;

		/** min_parts_to_merge will be lowered by 1 after that time.
		  * It will be lowered by 2 after that time * 2^1,
		  * It will be lowered by 3 after that time * 2^2,
		  *  and so on, exponentially.
		  */
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
