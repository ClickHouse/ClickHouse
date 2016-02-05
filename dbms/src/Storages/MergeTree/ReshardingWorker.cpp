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
#include <DB/Common/Increment.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Context.h>
#include <threadpool.hpp>
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
	extern const int UNKNOWN_ELEMENT_IN_CONFIG;
	extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

class Arguments final
{
public:
	Arguments(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_name, keys);
		for (const auto & key : keys)
		{
			if (key == "task_queue_path")
			{
				task_queue_path = config.getString(config_name + "." + key);
				if (task_queue_path.empty())
					throw Exception("Invalid parameter in resharding configuration", ErrorCodes::INVALID_CONFIG_PARAMETER);
			}
			else
				throw Exception("Unknown parameter in resharding configuration", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
		}
	}

	Arguments(const Arguments &) = delete;
	Arguments & operator=(const Arguments &) = delete;

	std::string getTaskQueuePath() const
	{
		return task_queue_path;
	}

private:
	std::string task_queue_path;
};

}

ReshardingWorker::ReshardingWorker(const Poco::Util::AbstractConfiguration & config,
	const std::string & config_name, Context & context_)
	: context(context_), log(&Logger::get("ReshardingWorker"))
{
	Arguments arguments(config, config_name);
	auto zookeeper = context.getZooKeeper();

	host_task_queue_path = arguments.getTaskQueuePath() + "resharding/" + getFQDNOrHostName();
	zookeeper->createAncestors(host_task_queue_path + "/");
}

ReshardingWorker::~ReshardingWorker()
{
	must_stop = true;
	{
		std::lock_guard<std::mutex> guard(cancel_mutex);
		if (merger)
			merger->cancel();
	}
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
	const ASTPtr & sharding_key_expr)
{
	auto serialized_job = ReshardingJob(database_name, table_name, partition, weighted_zookeeper_paths, sharding_key_expr).toString();
	auto zookeeper = context.getZooKeeper();
	(void) zookeeper->create(host_task_queue_path + "/task-", serialized_job,
		zkutil::CreateMode::PersistentSequential);
}

bool ReshardingWorker::isStarted() const
{
	return is_started;
}

void ReshardingWorker::pollAndExecute()
{
	bool error = false;

	try
	{
		bool old_val = false;
		if (!is_started.compare_exchange_strong(old_val, true, std::memory_order_seq_cst,
			std::memory_order_relaxed))
			throw Exception("Resharding background thread already started", ErrorCodes::LOGICAL_ERROR);

		LOG_DEBUG(log, "Started resharding background thread.");

		try
		{
			performPendingJobs();
		}
		catch (const Exception & ex)
		{
			if (ex.code() == ErrorCodes::ABORTED)
				throw;
			else
				LOG_ERROR(log, ex.message());
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
				if (ex.code() == ErrorCodes::ABORTED)
					throw;
				else
					LOG_ERROR(log, ex.message());
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}
		}
	}
	catch (const Exception & ex)
	{
		if (ex.code() != ErrorCodes::ABORTED)
			error = true;
	}
	catch (...)
	{
		error = true;
	}

	if (error)
	{
		/// Если мы попали сюда, это значит, что где-то кроется баг.
		LOG_ERROR(log, "Resharding background thread terminated with critical error.");
	}
	else
		LOG_DEBUG(log, "Resharding background thread terminated.");
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

		try
		{
			perform(job);
		}
		catch (const Exception & ex)
		{
			if (ex.code() != ErrorCodes::ABORTED)
				zookeeper->remove(child_full_path);
			throw;
		}
		catch (...)
		{
			zookeeper->remove(child_full_path);
			throw;
		}

		zookeeper->remove(child_full_path);
	}
}

void ReshardingWorker::perform(const ReshardingJob & job)
{
	LOG_DEBUG(log, "Performing resharding job.");

	StoragePtr generic_storage = context.getTable(job.database_name, job.table_name);
	auto & storage = typeid_cast<StorageReplicatedMergeTree &>(*(generic_storage.get()));

	/// Защитить перешардируемую партицию от задачи слияния.
	const MergeTreeMergeBlocker merge_blocker{storage.merger};

	try
	{
		createShardedPartitions(storage, job);
		publishShardedPartitions(storage, job);
		applyChanges(storage, job);
	}
	catch (const Exception & ex)
	{
		cleanup(storage, job);

		if (ex.code() == ErrorCodes::ABORTED)
			LOG_DEBUG(log, "Resharding job cancelled.");

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

	{
		std::lock_guard<std::mutex> guard(cancel_mutex);
		merger = std::make_unique<MergeTreeDataMerger>(storage.data);
	}

	MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;
	per_shard_data_parts = merger->reshardPartition(job, storage.data.context.getSettings().min_bytes_to_use_direct_io);
}

void ReshardingWorker::publishShardedPartitions(StorageReplicatedMergeTree & storage, const ReshardingJob & job)
{
	abortIfRequested();

	LOG_DEBUG(log, "Sending newly created partitions to their respective shards.");

	auto zookeeper = storage.getZooKeeper();

	struct TaskInfo
	{
		TaskInfo(const std::string & replica_path_,
			const std::string & part_,
			const ReplicatedMergeTreeAddress & dest_,
			size_t shard_no_)
			: replica_path(replica_path_), dest(dest_), part(part_),
			shard_no(shard_no_)
		{
		}

		std::string replica_path;
		ReplicatedMergeTreeAddress dest;
		std::string part;
		size_t shard_no;
	};

	using TaskInfoList = std::vector<TaskInfo>;
	TaskInfoList task_info_list;

	/// Копировать новые партиции на реплики соответствующих шардов.

	/// Количество участвующих локальных реплик. Должно быть <= 1.
	size_t local_count = 0;

	for (const auto & entry : storage.data.per_shard_data_parts)
	{
		size_t shard_no = entry.first;
		const MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
		if (!part_from_shard)
			continue;

		const WeightedZooKeeperPath & weighted_path = job.paths[shard_no];
		const std::string & zookeeper_path = weighted_path.first;

		auto children = zookeeper->getChildren(zookeeper_path + "/replicas");
		for (const auto & child : children)
		{
			const std::string replica_path = zookeeper_path + "/replicas/" + child;
			auto host = zookeeper->get(replica_path + "/host");
			ReplicatedMergeTreeAddress host_desc(host);
			task_info_list.emplace_back(replica_path, part_from_shard->name, host_desc, shard_no);
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
			const auto & part = entry.part;
			size_t shard_no = entry.shard_no;

			InterserverIOEndpointLocation to_location(replica_path, dest.host, dest.replication_port);

			size_t j = i - local_count;
			tasks[j] = Tasks::value_type(std::bind(&ShardedPartitionSender::Client::send,
				&storage.sharded_partition_sender_client, to_location, from_location, part, shard_no));
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
		const auto & part = entry.part;
		size_t shard_no = entry.shard_no;

		std::string from_path = storage.full_path + "reshard/" + toString(shard_no) + "/" + part + "/";
		std::string to_path = storage.full_path + "detached/";
		Poco::File(from_path).moveTo(to_path);
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

	for (const auto & entry : storage.data.per_shard_data_parts)
	{
		size_t shard_no = entry.first;
		const MergeTreeData::MutableDataPartPtr & part_from_shard = entry.second;
		if (!part_from_shard)
			continue;

		const WeightedZooKeeperPath & weighted_path = job.paths[shard_no];
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

	storage.data.per_shard_data_parts.clear();

	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(storage.full_path + "/reshard"); it != end; ++it)
	{
		auto absolute_path = it.path().absolute().toString();
		Poco::File(absolute_path).remove(true);
	}
}

void ReshardingWorker::abortIfRequested() const
{
	if (must_stop)
		throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
}

}
