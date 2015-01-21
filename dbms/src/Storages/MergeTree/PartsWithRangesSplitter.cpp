#include <DB/Storages/MergeTree/PartsWithRangesSplitter.h>

namespace DB
{

	PartsWithRangesSplitter::PartsWithRangesSplitter(const Cluster & input_cluster_, 
							size_t total_size_, size_t min_cluster_size_, size_t max_clusters_count_) 
	: input_cluster(input_cluster_),
	total_size(total_size_),
	remaining_size(total_size_),
	min_cluster_size(min_cluster_size_),
	max_clusters_count(max_clusters_count_)
	{
	}

	std::vector<PartsWithRangesSplitter::Cluster> PartsWithRangesSplitter::perform()
	{
		init();
		while (emit()) {}
		return output_clusters;
	}

	void PartsWithRangesSplitter::init()
	{
		size_t clusters_count = max_clusters_count;
		while ((clusters_count > 0) && (total_size < (min_cluster_size * clusters_count)))
			--clusters_count;

		cluster_size = total_size / clusters_count;

		output_clusters.resize(clusters_count);

		// Initialize range reader.
		input_part = input_cluster.begin();
		input_range = input_part->ranges.begin();
		initRangeInfo();

		// Initialize output writer.
		current_output_cluster = output_clusters.begin();
		addPart();
		initClusterInfo();
	}

	bool PartsWithRangesSplitter::emit()
	{
		size_t new_size = std::min(range_end - range_begin, cluster_end - cluster_begin);
		current_output_part->ranges.push_back(MarkRange(range_begin, range_begin + new_size));

		range_begin += new_size;
		cluster_begin += new_size;

		if (isClusterConsumed())
			return updateCluster();
		else if (isRangeConsumed())
			return updateRange(true);
		else
			return false;
	}

	bool PartsWithRangesSplitter::updateCluster()
	{
		++current_output_cluster;
		if (current_output_cluster == output_clusters.end())
			return false;

		if (isRangeConsumed())
			if (!updateRange(false))
				return false;

		addPart();
		initClusterInfo();
		return true;
	}

	bool PartsWithRangesSplitter::updateRange(bool add_part)
	{
		++input_range;
		if (input_range == input_part->ranges.end())
		{
			++input_part;
			if (input_part == input_cluster.end())
				return false;

			input_range = input_part->ranges.begin();

			if (add_part)
				addPart();
		}

		initRangeInfo();
		return true;
	}

	void PartsWithRangesSplitter::addPart()
	{
		MergeTreeDataSelectExecutor::RangesInDataPart new_part;
		new_part.data_part = input_part->data_part;
		current_output_cluster->push_back(new_part);
		current_output_part = &(current_output_cluster->back());
	}

	void PartsWithRangesSplitter::initRangeInfo()
	{
		range_begin = 0;
		range_end = input_range->end - input_range->begin;
	}

	void PartsWithRangesSplitter::initClusterInfo()
	{
		cluster_begin = 0;
		cluster_end = cluster_size;

		remaining_size -= cluster_size;
		if (remaining_size < cluster_size)
			cluster_end += remaining_size;
	}

}
