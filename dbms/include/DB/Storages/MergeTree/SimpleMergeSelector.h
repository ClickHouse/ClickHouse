#pragma once

#include <DB/Storages/MergeTree/MergeSelector.h>


namespace DB
{

class SimpleMergeSelector : public IMergeSelector
{
public:
	struct Settings
	{
		/** Minimum ratio of size of one part to all parts in set of parts to merge (for usual cases).
		  * For example, if all parts have equal size, it means, that at least 'base' number of parts should be merged.
		  * If parts has non-uniform sizes, then minumum number of parts to merge is effectively increased.
		  * This behaviour balances merge-tree workload.
		  * It called 'base', because merge-tree depth could be estimated as logarithm with that base.
		  */
		double base = 8;

		time_t lower_base_after = 300;

		/// Zero means unlimited.
		size_t max_parts_to_merge_at_once = 100;
	};

	SimpleMergeSelector(const Settings & settings) : settings(settings) {}

	PartsInPartition select(
		const Partitions & partitions,
		const size_t max_total_size_to_merge) override;

private:
	const Settings settings;
};

}
