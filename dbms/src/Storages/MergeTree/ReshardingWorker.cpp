#include <DB/Storages/MergeTree/ReshardingWorker.h>
#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Storages/MergeTree/MergeTreeSharder.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Common/getFQDNOrHostName.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Context.h>
#include <common/threadpool.hpp>
#include <zkutil/ZooKeeper.h>
#include <Poco/Event.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <future>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int ABORTED;
	extern const int UNEXPECTED_ZOOKEEPER_ERROR;
	extern const int PARTITION_COPY_FAILED;
	extern const int PARTITION_ATTACH_FAILED;
	extern const int RESHARDING_CLEANUP_FAILED;
}

namespace
{

std::string createMergedPartName(const MergeTreeData::DataPartsVector & parts)
{
	DayNum_t left_date = DayNum_t(std::numeric_limits<UInt16>::max());
	DayNum_t right_date = DayNum_t(std::numeric_limits<UInt16>::min());
	UInt32 level = 0;

	for (const MergeTreeData::DataPartPtr & part : parts)
	{
		level = std::max(level, part->level);
		left_date = std::min(left_date, part->left_date);
		right_date = std::max(right_date, part->right_date);
	}

	return ActiveDataPartSet::getPartName(left_date, right_date, parts.front()->left, parts.back()->right, level + 1);
}

}

ReshardingWorker::ReshardingWorker(Context & context_)
	: context(context_), log(&Logger::get("ReshardingWorker"))
{
	auto zookeeper = context.getZooKeeper();

	host_task_queue_path = "/clickhouse";
	zookeeper->createIfNotExists(host_task_queue_path, "");

	host_task_queue_path += "/" + zookeeper->getTaskQueuePath();
	zookeeper->createIfNotExists(host_task_queue_path, "");

	host_task_queue_path += "/resharding";
	zookeeper->createIfNotExists(host_task_queue_path, "");

	host_task_queue_path += "/" + getFQDNOrHostName();
	zookeeper->createIfNotExists(host_task_queue_path, "");
}

ReshardingWorker::~ReshardingWorker()
{
	must_stop = true;
	if (polling_thread.joinable())
		polling_thread.join();
}

void ReshardingWorker::start()
{
	polling_thread = std::thread(&ReshardingWorker::pollAndExecute, this);
}

void ReshardingWorker::submitJob(const std::string & database_name,
	const std::string & table_name,
	const std::string & partition,
	const WeightedZooKeeperPaths & weighted_zookeeper_paths,
	const std::string & sharding_key)
{
	auto str = ReshardingJob(database_name, table_name, partition, weighted_zookeeper_paths, sharding_key).toString();
	submitJobImpl(str);
}

void ReshardingWorker::submitJob(const ReshardingJob & job)
{
	auto str = job.toString();
	submitJobImpl(str);
}

bool ReshardingWorker::isStarted() const
{
	return is_started;
}

void ReshardingWorker::submitJobImpl(const std::string & serialized_job)
{
	auto zookeeper = context.getZooKeeper();
	(void) zookeeper->create(host_task_queue_path + "/task-", serialized_job,
		zkutil::CreateMode::PersistentSequential);
}

void ReshardingWorker::pollAndExecute()
{
	try
	{
		bool old_val = false;
		if (!is_started.compare_exchange_strong(old_val, true, std::memory_order_seq_cst,
			std::memory_order_relaxed))
			throw Exception("Resharding worker thread already started", ErrorCodes::LOGICAL_ERROR);

		LOG_DEBUG(log, "Started resharding thread.");

		try
		{
			performPendingJobs();
		}
		catch (const Exception & ex)
		{
			if ((ex.code() == ErrorCodes::RESHARDING_CLEANUP_FAILED) || hasAborted(ex))
				throw;
			else
				LOG_INFO(log, ex.message());
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		while (true)
		{
			try
			{
				Strings children;

				while (true)
				{
					zkutil::EventPtr event = new Poco::Event;
					auto zookeeper = context.getZooKeeper();
					children = zookeeper->getChildren(host_task_queue_path, nullptr, event);

					if (!children.empty())
						break;

					do
					{
						abortIfRequested();
					}
					while (!event->tryWait(1000));
				}

				std::sort(children.begin(), children.end());
				perform(children);
			}
			catch (const Exception & ex)
			{
				if ((ex.code() == ErrorCodes::RESHARDING_CLEANUP_FAILED) || hasAborted(ex))
					throw;
				else
					LOG_INFO(log, ex.message());
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}
		}
	}
	catch (const Exception & ex)
	{
		if (!hasAborted(ex))
			throw;
	}

	LOG_DEBUG(log, "Resharding thread terminated.");
}

void ReshardingWorker::performPendingJobs()
{
	auto zookeeper = context.getZooKeeper();

	Strings children = zookeeper->getChildren(host_task_queue_path);
	std::sort(children.begin(), children.end());
	perform(children);
}

void ReshardingWorker::perform(const Strings & job_nodes)
{
	auto zookeeper = context.getZooKeeper();

	for (const auto & child : job_nodes)
	{
		std::string  child_full_path = host_task_queue_path + "/" + child;
		auto job_descriptor = zookeeper->get(child_full_path);
		ReshardingJob job(job_descriptor);
		zookeeper->remove(child_full_path);
		perform(job);
	}
}

void ReshardingWorker::perform(const ReshardingJob & job)
{
	LOG_DEBUG(log, "Performing resharding job.");

	StoragePtr generic_storage = context.getTable(job.database_name, job.table_name);
	auto & storage = typeid_cast<StorageReplicatedMergeTree &>(*(generic_storage.get()));

	/// Защитить перешардируемую партицию от задачи слияния.
	ScopedPartitionMergeLock partition_merge_lock(storage, job.partition);

	try
	{
		createShardedPartitions(storage, job);
		publishShardedPartitions(storage, job);
		applyChanges(storage, job);
	}
	catch (const Exception & ex)
	{
		cleanup(storage, job);

		if (hasAborted(ex))
		{
			/// Поток завершается. Сохраняем сведения о прерванной задаче.
			submitJob(job);
			LOG_DEBUG(log, "Resharding job cancelled then re-submitted for later processing.");
		}

		throw;
	}
	catch (...)
	{
		cleanup(storage, job);
		throw;
	}

	cleanup(storage, job);
	LOG_DEBUG(log, "Resharding job successfully completed.");
}

void ReshardingWorker::createShardedPartitions(StorageReplicatedMergeTree & storage, const ReshardingJob & job)
{
	abortIfRequested();

	LOG_DEBUG(log, "Splitting partition shard-wise.");

	/// Куски одношо шарда, которые должы быть слиты.
	struct PartsToBeMerged
	{
		void add(MergeTreeData::MutableDataPartPtr & part)
		{
			parts.insert(part);
			total_size += part->size_in_bytes;
		}

		void clear()
		{
			parts.clear();
			total_size = 0;
		}

		MergeTreeData::MutableDataParts parts;
		size_t total_size = 0;
	};

	/// Для каждого шарда, куски, которые должны быть слиты.
	std::unordered_map<size_t, PartsToBeMerged> to_merge;

	MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;

	auto zookeeper = storage.getZooKeeper();
	const auto & settings = context.getSettingsRef();
	(void) settings;

	DayNum_t month = MergeTreeData::getMonthFromName(job.partition);

	auto parts_from_partition = storage.merger.selectAllPartsFromPartition(month);

	for (const auto & part : parts_from_partition)
	{
		MarkRanges ranges(1, MarkRange(0, part->size));

		MergeTreeBlockInputStream source(
			storage.data.getFullPath() + part->name + '/',
			DEFAULT_MERGE_BLOCK_SIZE,
			part->columns.getNames(),
			storage.data,
			part,
			ranges,
			false,
			nullptr,
			"",
			true,
			settings.min_bytes_to_use_direct_io,
			DBMS_DEFAULT_BUFFER_SIZE,
			true);

		MergeTreeSharder sharder(storage.data, job);

		Block block;
		while (block = source.read())
		{
			/// Разбить куски на несколько, согласно ключу шардирования.
			ShardedBlocksWithDateIntervals blocks = sharder.shardBlock(block);

			for (ShardedBlockWithDateInterval & block_with_dates : blocks)
			{
				abortIfRequested();

				/// Создать новый кусок соответствующий новому блоку.
				std::string month_name = toString(DateLUT::instance().toNumYYYYMMDD(DayNum_t(block_with_dates.min_date)) / 100);
				AbandonableLockInZooKeeper block_number_lock = storage.allocateBlockNumber(month_name);
				Int64 part_number = block_number_lock.getNumber();
				MergeTreeData::MutableDataPartPtr block_part = sharder.writeTempPart(block_with_dates, part_number);

				/// Добавить в БД ZooKeeper информацию о новом блоке.
				SipHash hash;
				block_part->checksums.summaryDataChecksum(hash);
				union
				{
					char bytes[16];
					UInt64 lo;
					UInt64 hi;
				} hash_value;
				hash.get128(hash_value.bytes);

				std::string checksum(hash_value.bytes, 16);

				std::string block_id = toString(hash_value.lo) + "_" + toString(hash_value.hi);

				zkutil::Ops ops;
				auto acl = zookeeper->getDefaultACL();

				std::string to_path = job.paths[block_with_dates.shard_no].first;

				ops.push_back(
					new zkutil::Op::Create(
						to_path + "/detached_sharded_blocks/" + block_id,
						"",
						acl,
						zkutil::CreateMode::Persistent));
				ops.push_back(
					new zkutil::Op::Create(
						to_path + "/detached_sharded_blocks/" + block_id + "/checksum",
						checksum,
						acl,
						zkutil::CreateMode::Persistent));
				ops.push_back(
					new zkutil::Op::Create(
						to_path + "/detached_sharded_blocks/" + block_id + "/number",
						toString(part_number),
						acl,
						zkutil::CreateMode::Persistent));

				block_number_lock.getUnlockOps(ops);

				auto code = zookeeper->tryMulti(ops);
				if (code != ZOK)
					throw Exception("Unexpected error while adding block " + toString(part_number)
						+ " with ID " + block_id + ": " + zkutil::ZooKeeper::error2string(code),
						ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);

				abortIfRequested();

				/// Добавить новый кусок в список кусков соответствущего шарда, которые должны
				/// быть слиты. Если установлено, что при вставке этого куска, суммарный размер
				/// кусков бы превышал некоторый предел, сначала слияем все куски, затем их
				/// перемещаем в список кусков новой партиции.
				PartsToBeMerged & parts_to_be_merged = to_merge[block_with_dates.shard_no];

				if ((parts_to_be_merged.total_size + block_part->size_in_bytes) > storage.data.settings.max_bytes_to_merge_parts)
				{
					MergeTreeData::MutableDataParts & sharded_parts = per_shard_data_parts[block_with_dates.shard_no];

					if (parts_to_be_merged.parts.size() >= 2)
					{
						MergeTreeData::DataPartsVector parts(parts_to_be_merged.parts.begin(), parts_to_be_merged.parts.end());
						std::string merged_name = createMergedPartName(parts);

						const auto & merge_entry = storage.data.context.getMergeList().insert(job.database_name,
							job.table_name, merged_name);

						MergeTreeData::MutableDataPartPtr new_part = storage.merger.mergeParts(parts, merged_name, *merge_entry,
							storage.data.context.getSettings().min_bytes_to_use_direct_io);

						sharded_parts.insert(new_part);
					}
					else
						sharded_parts.insert(block_part);

					/// Удалить исходные куски.
					parts_to_be_merged.clear();
				}

				parts_to_be_merged.add(block_part);
			}
		}

		/// Обработать все оставшиеся куски.
		for (auto & entry : to_merge)
		{
			abortIfRequested();

			size_t shard_no = entry.first;
			PartsToBeMerged & parts_to_be_merged = entry.second;

			MergeTreeData::MutableDataParts & sharded_parts = per_shard_data_parts[shard_no];

			if (parts_to_be_merged.parts.size() >= 2)
			{
				MergeTreeData::DataPartsVector parts(parts_to_be_merged.parts.begin(), parts_to_be_merged.parts.end());
				std::string merged_name = createMergedPartName(parts);

				const auto & merge_entry = storage.data.context.getMergeList().insert(job.database_name,
					job.table_name, merged_name);

				MergeTreeData::MutableDataPartPtr new_part = storage.merger.mergeParts(parts, merged_name, *merge_entry,
					storage.data.context.getSettings().min_bytes_to_use_direct_io);

				sharded_parts.insert(new_part);
			}
			else
			{
				auto single_part = *(parts_to_be_merged.parts.begin());
				sharded_parts.insert(single_part);
			}

			/// Удалить исходные куски.
			parts_to_be_merged.clear();
		}
	}

	/// До сих пор все куски новых партиций были временны.
	for (auto & entry : per_shard_data_parts)
	{
		size_t shard_no = entry.first;
		MergeTreeData::MutableDataParts & sharded_parts = entry.second;
		for (auto & sharded_part : sharded_parts)
		{
			sharded_part->is_temp = false;
			std::string prefix = storage.full_path + "reshard/" + toString(shard_no) + "/";
			std::string old_name = sharded_part->name;
			std::string new_name = ActiveDataPartSet::getPartName(sharded_part->left_date,
				sharded_part->right_date, sharded_part->left, sharded_part->right, sharded_part->level);
			sharded_part->name = new_name;
			Poco::File(prefix + old_name).renameTo(prefix + new_name);
		}
	}
}

void ReshardingWorker::publishShardedPartitions(StorageReplicatedMergeTree & storage, const ReshardingJob & job)
{
	abortIfRequested();

	LOG_DEBUG(log, "Sending newly created partitions to their respective shards.");

	auto zookeeper = storage.getZooKeeper();

	MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;

	struct TaskInfo
	{
		TaskInfo(const std::string & replica_path_,
			const std::vector<std::string> & parts_,
			const ReplicatedMergeTreeAddress & dest_,
			size_t shard_no_)
			: replica_path(replica_path_), dest(dest_), parts(parts_),
			shard_no(shard_no_)
		{
		}

		std::string replica_path;
		ReplicatedMergeTreeAddress dest;
		std::vector<std::string> parts;
		size_t shard_no;
	};

	using TaskInfoList = std::vector<TaskInfo>;
	TaskInfoList task_info_list;

	/// Копировать новые партиции на реплики соответствующих шардов.

	/// Количество участвующих локальных реплик. Должно быть <= 1.
	size_t local_count = 0;

	for (size_t shard_no = 0; shard_no < job.paths.size(); ++shard_no)
	{
		const WeightedZooKeeperPath & weighted_path = job.paths[shard_no];
		const std::string & zookeeper_path = weighted_path.first;

		std::vector<std::string> part_names;
		const MergeTreeData::MutableDataParts & sharded_parts = per_shard_data_parts.at(shard_no);
		for (const MergeTreeData::DataPartPtr & sharded_part : sharded_parts)
			part_names.push_back(sharded_part->name);

		auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
		for (const auto & child : children)
		{
			const std::string replica_path = zookeeper_path + "/replicas/" + child;
			auto host = zookeeper->get(replica_path + "/host");
			ReplicatedMergeTreeAddress host_desc(host);
			task_info_list.emplace_back(replica_path, part_names, host_desc, shard_no);
			if (replica_path == storage.replica_path)
			{
				++local_count;
				if (local_count > 1)
					throw Exception("Detected more than one local replica", ErrorCodes::LOGICAL_ERROR);
				std::swap(task_info_list[0], task_info_list[task_info_list.size() - 1]);
			}
		}
	}

	abortIfRequested();

	size_t remote_count = task_info_list.size() - local_count;

	boost::threadpool::pool pool(remote_count);

	using Tasks = std::vector<std::packaged_task<bool()> >;
	Tasks tasks(remote_count);

	ReplicatedMergeTreeAddress local_address(zookeeper->get(storage.replica_path + "/host"));
	InterserverIOEndpointLocation from_location(storage.replica_path, local_address.host, local_address.replication_port);

	try
	{
		for (size_t i = local_count; i < task_info_list.size(); ++i)
		{
			const TaskInfo & entry = task_info_list[i];
			const auto & replica_path = entry.replica_path;
			const auto & dest = entry.dest;
			const auto & parts = entry.parts;
			size_t shard_no = entry.shard_no;

			InterserverIOEndpointLocation to_location(replica_path, dest.host, dest.replication_port);

			size_t j = i - local_count;
			tasks[j] = Tasks::value_type(std::bind(&ShardedPartitionSender::Client::send,
				&storage.sharded_partition_sender_client, to_location, from_location, parts, shard_no));
			pool.schedule([j, &tasks]{ tasks[j](); });
		}
	}
	catch (...)
	{
		pool.wait();
		throw;
	}

	pool.wait();

	for (auto & task : tasks)
	{
		bool res = task.get_future().get();
		if (!res)
			throw Exception("Failed to copy partition", ErrorCodes::PARTITION_COPY_FAILED);
	}

	abortIfRequested();

	if (local_count == 1)
	{
		/// На локальной реплике просто перемещаем шардированную паритцию в папку detached/.
		const TaskInfo & entry = task_info_list[0];
		const auto & parts = entry.parts;
		size_t shard_no = entry.shard_no;

		for (const auto & part : parts)
		{
			std::string from_path = storage.full_path + "reshard/" + toString(shard_no) + "/" + part + "/";
			std::string to_path = storage.full_path + "detached/";
			Poco::File(from_path).moveTo(to_path);
		}
	}
}

void ReshardingWorker::applyChanges(StorageReplicatedMergeTree & storage, const ReshardingJob & job)
{
	abortIfRequested();

	LOG_DEBUG(log, "Attaching new partitions.");

	auto zookeeper = storage.getZooKeeper();

	/// На локальном узле удалить первоначальную партицию.
	std::string query_str = "ALTER TABLE " + job.database_name + "." + job.table_name + " DROP PARTITION " + job.partition;
	(void) executeQuery(query_str, context, true);

	/// На всех участвующих репликах добавить соответствующие шардированные партиции в таблицу.
	struct TaskInfo
	{
		TaskInfo(const std::string & replica_path_, const ReplicatedMergeTreeAddress & dest_)
			: replica_path(replica_path_), dest(dest_)
		{
		}

		std::string replica_path;
		ReplicatedMergeTreeAddress dest;
	};

	using TaskInfoList = std::vector<TaskInfo>;
	TaskInfoList task_info_list;

	for (size_t i = 0; i < job.paths.size(); ++i)
	{
		const WeightedZooKeeperPath & weighted_path = job.paths[i];
		const std::string & zookeeper_path = weighted_path.first;

		auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
		for (const auto & child : children)
		{
			const std::string replica_path = zookeeper_path + "/replicas/" + child;
			auto host = zookeeper->get(replica_path + "/host");
			ReplicatedMergeTreeAddress host_desc(host);
			task_info_list.emplace_back(replica_path, host_desc);
		}
	}

	boost::threadpool::pool pool(task_info_list.size());

	using Tasks = std::vector<std::packaged_task<bool()> >;
	Tasks tasks(task_info_list.size());

	try
	{
		for (size_t i = 0; i < task_info_list.size(); ++i)
		{
			const auto & entry = task_info_list[i];
			const auto & replica_path = entry.replica_path;
			const auto & dest = entry.dest;

			InterserverIOEndpointLocation location(replica_path, dest.host, dest.replication_port);

			std::string query_str = "ALTER TABLE " + dest.database + "." + dest.table + " ATTACH PARTITION " + job.partition;

			tasks[i] = Tasks::value_type(std::bind(&RemoteQueryExecutor::Client::executeQuery,
				&storage.remote_query_executor_client, location, query_str));

			pool.schedule([i, &tasks]{ tasks[i](); });
		}
	}
	catch (...)
	{
		pool.wait();
		throw;
	}

	pool.wait();

	for (auto & task : tasks)
	{
		bool res = task.get_future().get();
		if (!res)
			throw Exception("Failed to attach partition on replica", ErrorCodes::PARTITION_ATTACH_FAILED);
	}
}

void ReshardingWorker::cleanup(StorageReplicatedMergeTree & storage, const ReshardingJob & job)
{
	LOG_DEBUG(log, "Performing cleanup.");

	try
	{
		storage.data.per_shard_data_parts.clear();

		Poco::DirectoryIterator end;
		for (Poco::DirectoryIterator it(storage.full_path + "/reshard"); it != end; ++it)
		{
			auto absolute_path = it.path().absolute().toString();
			Poco::File(absolute_path).remove(true);
		}

		auto zookeeper = storage.getZooKeeper();
		zkutil::Ops ops;
		for (size_t i = 0; i < job.paths.size(); ++i)
		{
			const WeightedZooKeeperPath & weighted_path = job.paths[i];
			const std::string & zookeeper_path = weighted_path.first;

			auto children = zookeeper->getChildren(zookeeper_path + "/detached_sharded_blocks");
			if (!children.empty())
			{
				for (const auto & child : children)
				{
					ops.push_back(
						new zkutil::Op::Remove(
							zookeeper_path + "/detached_sharded_blocks/" + child + "/number", -1));
					ops.push_back(
						new zkutil::Op::Remove(
							zookeeper_path + "/detached_sharded_blocks/" + child + "/checksum", -1));
					ops.push_back(
						new zkutil::Op::Remove(
							zookeeper_path + "/detached_sharded_blocks/" + child, -1));
				}
			}
		}
		zookeeper->multi(ops);
	}
	catch (...)
	{
		throw Exception("Failed to perform cleanup during resharding operation",
			ErrorCodes::RESHARDING_CLEANUP_FAILED);
	}
}

void ReshardingWorker::abortIfRequested() const
{
	if (must_stop)
		throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
}

bool ReshardingWorker::hasAborted(const Exception & ex) const
{
	return must_stop && (ex.code() == ErrorCodes::ABORTED);
}

}
