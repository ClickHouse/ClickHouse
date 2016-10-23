#include <DB/Interpreters/ActiveMetrics.h>
#include <DB/Common/Exception.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Storages/MarkCache.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/UncompressedCache.h>
#include <DB/Databases/IDatabase.h>
#include <chrono>


namespace DB
{

ActiveMetrics::~ActiveMetrics()
{
	try
	{
		{
			std::lock_guard<std::mutex> lock{mutex};
			quit = true;
		}

		cond.notify_one();
		thread.join();
	}
	catch (...)
	{
		DB::tryLogCurrentException(__FUNCTION__);
	}
}


void ActiveMetrics::run()
{
	setThreadName("ActiveMetrics");

	std::unique_lock<std::mutex> lock{mutex};

	/// To be distant with moment of transmission of metrics. It is not strictly necessary.
	cond.wait_until(lock, std::chrono::system_clock::now() + std::chrono::seconds(30), [this] { return quit; });

	while (true)
	{
		if (cond.wait_until(lock, std::chrono::system_clock::now() + std::chrono::seconds(60), [this] { return quit; }))
			break;

		try
		{
			update();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
}


template <typename Max, typename T>
static void calculateMax(Max & max, T x)
{
	if (Max(x) > max)
		max = x;
}

template <typename Max, typename Sum, typename T>
static void calculateMaxAndSum(Max & max, Sum & sum, T x)
{
	sum += x;
	if (Max(x) > max)
		max = x;
}


void ActiveMetrics::update()
{
	{
		if (auto mark_cache = context.getMarkCache())
		{
			CurrentMetrics::set(CurrentMetrics::MarkCacheBytes, mark_cache->weight());
			CurrentMetrics::set(CurrentMetrics::MarkCacheFiles, mark_cache->count());
		}
	}

	{
		if (auto uncompressed_cache = context.getUncompressedCache())
		{
			CurrentMetrics::set(CurrentMetrics::UncompressedCacheBytes, uncompressed_cache->weight());
			CurrentMetrics::set(CurrentMetrics::UncompressedCacheCells, uncompressed_cache->count());
		}
	}

	{
		auto databases = context.getDatabases();

		size_t max_queue_size = 0;
		size_t max_inserts_in_queue = 0;
		size_t max_merges_in_queue = 0;

		size_t sum_queue_size = 0;
		size_t sum_inserts_in_queue = 0;
		size_t sum_merges_in_queue = 0;

		size_t max_absolute_delay = 0;
		size_t max_relative_delay = 0;

		size_t max_part_count_for_partition = 0;

		for (const auto & db : databases)
		{
			for (auto iterator = db.second->getIterator(); iterator->isValid(); iterator->next())
			{
				auto & table = iterator->table();
				StorageMergeTree * table_merge_tree = typeid_cast<StorageMergeTree *>(table.get());
				StorageReplicatedMergeTree * table_replicated_merge_tree = typeid_cast<StorageReplicatedMergeTree *>(table.get());

				if (table_replicated_merge_tree)
				{
					StorageReplicatedMergeTree::Status status;
					table_replicated_merge_tree->getStatus(status, false);

					calculateMaxAndSum(max_queue_size, sum_queue_size, status.queue.queue_size);
					calculateMaxAndSum(max_inserts_in_queue, sum_inserts_in_queue, status.queue.inserts_in_queue);
					calculateMaxAndSum(max_merges_in_queue, sum_merges_in_queue, status.queue.merges_in_queue);

					try
					{
						time_t absolute_delay = 0;
						time_t relative_delay = 0;
						table_replicated_merge_tree->getReplicaDelays(absolute_delay, relative_delay);

						calculateMax(max_absolute_delay, absolute_delay);
						calculateMax(max_relative_delay, relative_delay);
					}
					catch (...)
					{
						tryLogCurrentException(__PRETTY_FUNCTION__,
							"Cannot get replica delay for table: " + backQuoteIfNeed(db.first) + "." + backQuoteIfNeed(iterator->name()));
					}

					calculateMax(max_part_count_for_partition, table_replicated_merge_tree->getData().getMaxPartsCountForMonth());
					if (auto unreplicated_data = table_replicated_merge_tree->getUnreplicatedData())
						calculateMax(max_part_count_for_partition, unreplicated_data->getMaxPartsCountForMonth());
				}

				if (table_merge_tree)
				{
					calculateMax(max_part_count_for_partition, table_merge_tree->getData().getMaxPartsCountForMonth());
				}
			}
		}

		CurrentMetrics::set(CurrentMetrics::ReplicasMaxQueueSize, max_queue_size);
		CurrentMetrics::set(CurrentMetrics::ReplicasMaxInsertsInQueue, max_inserts_in_queue);
		CurrentMetrics::set(CurrentMetrics::ReplicasMaxMergesInQueue, max_merges_in_queue);

		CurrentMetrics::set(CurrentMetrics::ReplicasSumQueueSize, sum_queue_size);
		CurrentMetrics::set(CurrentMetrics::ReplicasSumInsertsInQueue, sum_inserts_in_queue);
		CurrentMetrics::set(CurrentMetrics::ReplicasSumMergesInQueue, sum_merges_in_queue);

		CurrentMetrics::set(CurrentMetrics::ReplicasMaxAbsoluteDelay, max_absolute_delay);
		CurrentMetrics::set(CurrentMetrics::ReplicasMaxRelativeDelay, max_relative_delay);

		CurrentMetrics::set(CurrentMetrics::MaxPartCountForPartition, max_part_count_for_partition);
	}

	/// Add more metrics as you wish.
}


}
