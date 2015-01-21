#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

	/// Разбиваем parts_with_ranges на n частей.
	/// Следующие условия должны быть выполнены:
	/// - 1 <= n <= settings.max_clusters_count;
	/// - каждая из n частей имеет >= min_cluster_size записей.
	/// 3 levels: cluster / part / range

	class PartsWithRangesSplitter
	{
	public:
		typedef MergeTreeDataSelectExecutor::RangesInDataParts Cluster;

	public:
		PartsWithRangesSplitter(const Cluster & input_cluster_, 
								size_t total_size_, size_t min_cluster_size_, size_t max_clusters_count_);

		~PartsWithRangesSplitter() = default;
		PartsWithRangesSplitter(const PartsWithRangesSplitter &) = delete;
		PartsWithRangesSplitter & operator=(const PartsWithRangesSplitter &) = delete;

		std::vector<Cluster> perform();

	private:
		void init();
		bool emit();
		bool updateCluster();
		bool updateRange(bool add_part);
		void addPart();
		void initRangeInfo();
		void initClusterInfo();
		bool isRangeConsumed() const { return range_begin == range_end; }
		bool isClusterConsumed() const { return cluster_begin == cluster_end; }

 	private:
		// Input data.
		const Cluster & input_cluster;
		Cluster::const_iterator input_part;
		std::vector<MarkRange>::const_iterator input_range;

		// Output data.
		std::vector<Cluster> output_clusters;
		std::vector<Cluster>::iterator current_output_cluster;
		MergeTreeDataSelectExecutor::RangesInDataPart * current_output_part;

		size_t total_size;
		size_t remaining_size;
		size_t min_cluster_size;
		size_t max_clusters_count;
		size_t cluster_size;

		size_t range_begin;
		size_t range_end;

		size_t cluster_begin;
		size_t cluster_end;
	};
}