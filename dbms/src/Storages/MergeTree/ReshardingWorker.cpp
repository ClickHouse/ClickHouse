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
#include <DB/IO/HexWriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Common/getFQDNOrHostName.h>

#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Cluster.h>

#include <threadpool.hpp>

#include <zkutil/ZooKeeper.h>

#include <Poco/Event.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/SharedPtr.h>

#include <openssl/sha.h>
#include <future>
#include <chrono>

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
	extern const int RESHARDING_BUSY_CLUSTER;
	extern const int RESHARDING_NO_SUCH_COORDINATOR;
	extern const int RESHARDING_NO_COORDINATOR_MEMBERSHIP;
	extern const int RESHARDING_ALREADY_SUBSCRIBED;
	extern const int RESHARDING_REMOTE_NODE_UNAVAILABLE;
	extern const int RESHARDING_REMOTE_NODE_ERROR;
	extern const int RESHARDING_COORDINATOR_DELETED;
	extern const int RESHARDING_DISTRIBUTED_JOB_ON_HOLD;
	extern const int RESHARDING_INVALID_QUERY;
}

namespace
{

constexpr long wait_duration = 1000;

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

/// Rationale for distributed jobs.
///
/// A distributed job is initiated in a query ALTER TABLE RESHARD inside which
/// we specify a distributed table. Then ClickHouse creates a job coordinating
/// structure in ZooKeeper, namely a coordinator, identified by a so-called
/// coordinator id.
/// Each shard of the cluster specified in the distributed table metadata
/// receives one query ALTER TABLE RESHARD with the keyword COORDINATE WITH
/// indicating the aforementioned coordinator id.
///
/// At the highest level we have under the /resharding_distributed znode:
///
/// /lock: global distributed read/write lock;
/// /online: currently online nodes;
/// /coordinators: one znode for each coordinator.
///
/// A coordinator whose identifier is ${id} has the following layout
/// under the /coordinators/${id} znode:
///
/// /lock: coordinator-specific distributed read/write lock;
///
/// /query_hash: hash value obtained from the query that
/// is sent to the participating nodes;
///
/// /increment: unique block number allocator;
///
/// /status: coordinator status;
///
/// /status/${host}: distributed job status on a given participating node;
///
/// /cluster: cluster on which the distributed job is to be performed;
///
/// /node_count: number of nodes of the cluster that participate in at
/// least one distributed resharding job;
///
/// /cluster_addresses: the list of addresses, in both IP and hostname
/// representations, of all the nodes of the cluster; used to check if a given node
/// is a member of the cluster;
///
/// /check_barrier: when all the participating nodes have checked
/// that they can perform resharding operations, proceed further;
///
/// /opt_out_barrier: after having crossed this barrier, each node of the cluster
/// knows exactly whether it will take part in distributed jobs or not.
///
/// /partitions: partitions that must be resharded on more than one shard;
///
/// /partitions/${partition_id}/nodes: participating nodes;
///
/// /partitions/${partition_id}/upload_barrier: when all the participating
/// nodes have uploaded new data to their respective replicas, we can apply changes;
///
/// /partitions/${partition_id}/recovery_barrier: recovery if
/// one or several participating nodes had previously gone offline.
///

ReshardingWorker::ReshardingWorker(const Poco::Util::AbstractConfiguration & config,
	const std::string & config_name, Context & context_)
	: context(context_), log(&Logger::get("ReshardingWorker"))
{
	Arguments arguments(config, config_name);
	auto zookeeper = context.getZooKeeper();

	std::string root = arguments.getTaskQueuePath();
	if (*(root.rbegin()) != '/')
		root += "/";

	auto current_host = getFQDNOrHostName();

	host_task_queue_path = root + "resharding/" + current_host;
	zookeeper->createAncestors(host_task_queue_path + "/");

	distributed_path = root + "resharding_distributed";
	zookeeper->createAncestors(distributed_path + "/");

	distributed_online_path = distributed_path + "/online";
	zookeeper->createIfNotExists(distributed_online_path, "");

	/// Notify that we are online.
	zookeeper->create(distributed_online_path + "/" + current_host, "",
		zkutil::CreateMode::Ephemeral);

	distributed_lock_path = distributed_path + "/lock";
	zookeeper->createIfNotExists(distributed_lock_path, "");

	coordination_path = distributed_path + "/coordination";
	zookeeper->createAncestors(coordination_path + "/");
}

ReshardingWorker::~ReshardingWorker()
{
	try
	{
		shutdown();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

void ReshardingWorker::start()
{
	polling_thread = std::thread(&ReshardingWorker::pollAndExecute, this);
}

void ReshardingWorker::shutdown()
{
	must_stop = true;
	if (polling_thread.joinable())
		polling_thread.join();
}

void ReshardingWorker::submitJob(const ReshardingJob & job)
{
	auto serialized_job = job.toString();
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
						abortPollingIfRequested();
					}
					while (!event->tryWait(wait_duration));
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

void ReshardingWorker::jabScheduler()
{
	/// We create then delete a node so that the main loop of the job scheduler will notice
	/// something has just happened. This forces the scheduler to fetch all the current
	/// pending jobs. We need this when a distributed job is not ready to be performed yet.
	/// Otherwise if no new jobs were submitted, we wouldn't be able to check again whether
	/// we can perform the distributed job.
	auto zookeeper = context.getZooKeeper();
	(void) zookeeper->create(host_task_queue_path + "/fake_task", "", zkutil::CreateMode::Persistent);
	zookeeper->remove(host_task_queue_path + "/fake_task");

	/// Sleep for 3 time units in order to prevent scheduler overloading
	/// if we were the only job in the queue.
	if (zookeeper->getChildren(host_task_queue_path).size() == 1)
		std::this_thread::sleep_for(3 * std::chrono::milliseconds(wait_duration));
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

		try
		{
			perform(job_descriptor);
		}
		catch (const Exception & ex)
		{
			/// If the job has been cancelled, either locally or remotely, we keep it
			/// in the corresponding task queues for a later execution.
			/// If an error has occurred, either locally or remotely, while
			/// performing the job, we delete it from the corresponding task queues.

			if ((ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE) ||
				(ex.code() == ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD))
			{
				LOG_DEBUG(log, ex.displayText());
				continue;
			}
			else
			{
				try
				{
					if (ex.code() == ErrorCodes::ABORTED)
					{
						/// nothing here
					}
					else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_ERROR)
						zookeeper->remove(child_full_path);
					else if (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED)
						zookeeper->remove(child_full_path);
					else
						zookeeper->remove(child_full_path);
				}
				catch (...)
				{
					tryLogCurrentException(__PRETTY_FUNCTION__);
				}

				throw;
			}
		}
		catch (...)
		{
			zookeeper->remove(child_full_path);
			throw;
		}

		zookeeper->remove(child_full_path);
	}
}

void ReshardingWorker::perform(const std::string & job_descriptor)
{
	LOG_DEBUG(log, "Performing resharding job.");

	current_job = ReshardingJob{job_descriptor};

	StoragePtr generic_storage = context.getTable(current_job.database_name, current_job.table_name);
	auto & storage = typeid_cast<StorageReplicatedMergeTree &>(*(generic_storage.get()));
	current_job.storage = &storage;

	/// Защитить перешардируемую партицию от задачи слияния.
	const MergeTreeMergeBlocker merge_blocker{current_job.storage->merger};

	try
	{
		attachJob();
		createShardedPartitions();
		publishShardedPartitions();
		waitForUploadCompletion();
		applyChanges();
	}
	catch (const Exception & ex)
	{
		try
		{
			if (ex.code() == ErrorCodes::ABORTED)
			{
				LOG_DEBUG(log, "Resharding job cancelled.");
				/// A soft shutdown is being performed on this node.
				/// Put the current distributed job on hold in order to reliably handle
				/// the scenario in which the remote nodes undergo a hard shutdown.
				if (current_job.isCoordinated())
					setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD);
				softCleanup();
			}
			else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE)
			{
				/// A remote node has gone offline.
				/// Put the current distributed job on hold. Also jab the job scheduler
				/// so that it will come accross this distributed job even if no new jobs
				/// are submitted.
				setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD);
				softCleanup();
				jabScheduler();
			}
			else if (ex.code() == ErrorCodes::RESHARDING_REMOTE_NODE_ERROR)
			{
				hardCleanup();
			}
			else if (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED)
			{
				// nothing here
			}
			else if (ex.code() == ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD)
			{
				/// The current distributed job is on hold and one or more required nodes
				/// have not gone online yet. Jab the job scheduler so that it will come
				/// accross this distributed job even if no new jobs are submitted.
				setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD);
				jabScheduler();
			}
			else
			{
				/// Cancellation has priority over errors.
				if (must_stop)
				{
					LOG_DEBUG(log, "Resharding job cancelled.");
					if (current_job.isCoordinated())
						setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD);
					softCleanup();
				}
				else
				{
					/// An error has occurred on this node.
					if (current_job.isCoordinated())
					{
						auto current_host = getFQDNOrHostName();
						setStatus(current_job.coordinator_id, current_host, STATUS_ERROR);
					}
					hardCleanup();
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		throw;
	}
	catch (...)
	{
		/// An error has occurred on this node.
		try
		{
			/// Cancellation has priority over errors.
			if (must_stop)
			{
				LOG_DEBUG(log, "Resharding job cancelled.");
				if (current_job.isCoordinated())
					setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ON_HOLD);
				softCleanup();
			}
			else
			{
				if (current_job.isCoordinated())
				{
					auto current_host = getFQDNOrHostName();
					setStatus(current_job.coordinator_id, current_host, STATUS_ERROR);
				}
				hardCleanup();
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		throw;
	}

	hardCleanup();
	LOG_DEBUG(log, "Resharding job successfully completed.");
}

void ReshardingWorker::createShardedPartitions()
{
	abortJobIfRequested();

	LOG_DEBUG(log, "Splitting partition shard-wise.");

	auto & storage = *(current_job.storage);

	merger = std::make_unique<MergeTreeDataMerger>(storage.data);

	MergeTreeDataMerger::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
	merger->setCancellationHook(hook);

	MergeTreeData::PerShardDataParts & per_shard_data_parts = storage.data.per_shard_data_parts;
	per_shard_data_parts = merger->reshardPartition(current_job);
}

void ReshardingWorker::publishShardedPartitions()
{
	abortJobIfRequested();

	LOG_DEBUG(log, "Sending newly created partitions to their respective shards.");

	auto & storage = *(current_job.storage);
	auto zookeeper = context.getZooKeeper();

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

		const WeightedZooKeeperPath & weighted_path = current_job.paths[shard_no];
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

	abortJobIfRequested();

	size_t remote_count = task_info_list.size() - local_count;

	boost::threadpool::pool pool(remote_count);

	using Tasks = std::vector<std::packaged_task<bool()> >;
	Tasks tasks(remote_count);

	ReplicatedMergeTreeAddress local_address(zookeeper->get(storage.replica_path + "/host"));
	InterserverIOEndpointLocation from_location(storage.replica_path, local_address.host, local_address.replication_port);

	ShardedPartitionUploader::Client::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
	storage.sharded_partition_uploader_client.setCancellationHook(hook);

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
			tasks[j] = Tasks::value_type(std::bind(&ShardedPartitionUploader::Client::send,
				&storage.sharded_partition_uploader_client, part, shard_no, to_location));
			pool.schedule([j, &tasks]{ tasks[j](); });
		}
	}
	catch (...)
	{
		try
		{
			pool.wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		if (!current_job.is_aborted)
		{
			/// We may have caught an error because one or more remote nodes have
			/// gone offline while performing I/O. The following check is here to
			/// sort out this ambiguity.
			abortJobIfRequested();
		}

		throw;
	}

	pool.wait();

	for (auto & task : tasks)
	{
		bool res = task.get_future().get();
		if (!res)
			throw Exception("Failed to copy partition", ErrorCodes::PARTITION_COPY_FAILED);
	}

	abortJobIfRequested();

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

void ReshardingWorker::applyChanges()
{
	LOG_DEBUG(log, "Attaching new partitions.");

	auto & storage = *(current_job.storage);
	auto zookeeper = context.getZooKeeper();

	/// На локальном узле удалить первоначальную партицию.
	std::string query_str = "ALTER TABLE " + current_job.database_name + "."
		+ current_job.table_name + " DROP PARTITION " + current_job.partition;
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

		const WeightedZooKeeperPath & weighted_path = current_job.paths[shard_no];
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

			std::string query_str = "ALTER TABLE " + dest.database + "."
				+ dest.table + " ATTACH PARTITION " + current_job.partition;

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
			throw Exception("Failed to attach partition on replica",
				ErrorCodes::PARTITION_ATTACH_FAILED);
	}
}

void ReshardingWorker::softCleanup()
{
	LOG_DEBUG(log, "Performing soft cleanup.");
	cleanupCommon();
	current_job.clear();
}

void ReshardingWorker::hardCleanup()
{
	LOG_DEBUG(log, "Performing cleanup.");
	cleanupCommon();
	detachJob();
	current_job.clear();
}

void ReshardingWorker::cleanupCommon()
{
	auto & storage = *(current_job.storage);
	storage.data.per_shard_data_parts.clear();

	if (Poco::File(storage.full_path + "/reshard").exists())
	{
		Poco::DirectoryIterator end;
		for (Poco::DirectoryIterator it(storage.full_path + "/reshard"); it != end; ++it)
		{
			auto absolute_path = it.path().absolute().toString();
			Poco::File(absolute_path).remove(true);
		}
	}
}

std::string ReshardingWorker::createCoordinator(const Cluster & cluster)
{
	const std::string cluster_name = cluster.getName();
	auto zookeeper = context.getZooKeeper();

	auto lock = createLock();
	zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

	auto coordinators = zookeeper->getChildren(coordination_path);
	for (const auto & coordinator : coordinators)
	{
		auto effective_cluster_name = zookeeper->get(coordination_path + "/" + coordinator + "/cluster");
		if (effective_cluster_name == cluster_name)
			throw Exception("The cluster " + cluster_name + " is currently busy with another "
				"distributed job. Please try later", ErrorCodes::RESHARDING_BUSY_CLUSTER);
	}

	std::string coordinator_id = zookeeper->create(coordination_path + "/coordinator-", "",
		zkutil::CreateMode::PersistentSequential);
	coordinator_id = coordinator_id.substr(coordination_path.length() + 1);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/lock",
		"", zkutil::CreateMode::Persistent);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/cluster",
		cluster_name, zkutil::CreateMode::Persistent);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/increment",
		"0", zkutil::CreateMode::Persistent);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/node_count",
		toString(cluster.getRemoteShardCount() + cluster.getLocalShardCount()),
		zkutil::CreateMode::Persistent);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/status",
		"", zkutil::CreateMode::Persistent);
	setStatus(coordinator_id, STATUS_OK);

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/partitions",
		"", zkutil::CreateMode::Persistent);

	/// Register the addresses, IP and hostnames, of all the nodes of the cluster.

	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/cluster_addresses",
		"", zkutil::CreateMode::Persistent);

	std::vector<std::string> cluster_addresses;
	const auto & addresses = cluster.getShardsAddresses();
	for (const auto & address : addresses)
	{
		cluster_addresses.push_back(address.host_name);
		cluster_addresses.push_back(address.resolved_address.host().toString());
	}

	const auto & addresses_with_failover = cluster.getShardsWithFailoverAddresses();
	for (const auto & addresses : addresses_with_failover)
	{
		for (const auto & address : addresses)
		{
			cluster_addresses.push_back(address.host_name);
			cluster_addresses.push_back(address.resolved_address.host().toString());
		}
	}

	for (const auto & host : cluster_addresses)
	{
		int32_t code = zookeeper->tryCreate(getCoordinatorPath(coordinator_id) + "/cluster_addresses/"
			+ host, "", zkutil::CreateMode::Persistent);
		if ((code != ZOK) && (code != ZNODEEXISTS))
			throw zkutil::KeeperException(code);
	}

	return coordinator_id;
}

void ReshardingWorker::registerQuery(const std::string & coordinator_id, const std::string & query)
{
	auto zookeeper = context.getZooKeeper();
	(void) zookeeper->create(getCoordinatorPath(coordinator_id) + "/query_hash", computeHash(query),
		zkutil::CreateMode::Persistent);
}

void ReshardingWorker::deleteCoordinator(const std::string & coordinator_id)
{
	auto zookeeper = context.getZooKeeper();
	if (zookeeper->exists(getCoordinatorPath(coordinator_id)))
		zookeeper->removeRecursive(getCoordinatorPath(coordinator_id));
}

UInt64 ReshardingWorker::subscribe(const std::string & coordinator_id, const std::string & query)
{
	auto zookeeper = context.getZooKeeper();

	current_coordinator_id = coordinator_id;

	{
		auto lock = createCoordinatorLock();
		zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

		/// Make sure that the query ALTER TABLE RESHARD with the "COORDINATE WITH" tag
		/// is not bogus.

		if (!zookeeper->exists(getCoordinatorPath(coordinator_id)))
			throw Exception("Coordinator " + coordinator_id + " does not exist",
			ErrorCodes::RESHARDING_NO_SUCH_COORDINATOR);

		auto current_host = getFQDNOrHostName();

		auto cluster_addresses = zookeeper->getChildren(getCoordinatorPath(coordinator_id)
			+ "/cluster_addresses");
		if (std::find(cluster_addresses.begin(), cluster_addresses.end(), current_host)
			== cluster_addresses.end())
			throw Exception("This host is not allowed to subscribe to coordinator "
				+ coordinator_id,
				ErrorCodes::RESHARDING_NO_COORDINATOR_MEMBERSHIP);

		int32_t code = zookeeper->tryCreate(getCoordinatorPath(coordinator_id) + "/status/" + current_host,
			"", zkutil::CreateMode::Persistent);
		if (code == ZNODEEXISTS)
			throw Exception("Already subscribed to coordinator " + coordinator_id,
				ErrorCodes::RESHARDING_ALREADY_SUBSCRIBED);
		else if (code != ZOK)
			throw zkutil::KeeperException(code);

		auto query_hash = zookeeper->get(getCoordinatorPath(coordinator_id) + "/query_hash");
		if (computeHash(query) != query_hash)
			throw Exception("Coordinator " + coordinator_id + " does not handle this query",
				ErrorCodes::RESHARDING_INVALID_QUERY);

		setStatus(coordinator_id, current_host, STATUS_OK);
	}

	/// Assign a unique block number to the current node. We will use it in order
	/// to avoid any possible conflict when uploading resharded partitions.
	UInt64 block_number;
	{
		auto lock = createCoordinatorLock();
		zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

		auto current_block_number = zookeeper->get(getCoordinatorPath(coordinator_id) + "/increment");
		block_number = std::stoull(current_block_number);
		zookeeper->set(getCoordinatorPath(coordinator_id) + "/increment", toString(block_number + 1));
	}

	/// We set up a timeout for this barrier because until we cross it, we don't have
	/// any guarantee that all the required nodes for this distributed job are online.
	/// We are inside a lightweight function, so it is not an issue.
	auto timeout = context.getSettingsRef().resharding_barrier_timeout;
	createCheckBarrier(coordinator_id).enter(timeout);

	return block_number;
}

void ReshardingWorker::unsubscribe(const std::string & coordinator_id)
{
	auto zookeeper = context.getZooKeeper();

	auto lock = createCoordinatorLock();
	zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

	auto current_host = getFQDNOrHostName();
	zookeeper->remove(getCoordinatorPath(coordinator_id) + "/status/" + current_host);

	auto node_count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");
	UInt64 cur_node_count = std::stoull(node_count);
	if (cur_node_count == 0)
		throw Exception("ReshardingWorker: invalid node count", ErrorCodes::LOGICAL_ERROR);
	zookeeper->set(getCoordinatorPath(coordinator_id) + "/node_count", toString(cur_node_count - 1));
}

void ReshardingWorker::addPartitions(const std::string & coordinator_id,
	const PartitionList & partition_list)
{
	auto zookeeper = context.getZooKeeper();

	auto lock = createCoordinatorLock();
	zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

	auto current_host = getFQDNOrHostName();

	for (const auto & partition : partition_list)
	{
		auto path = getCoordinatorPath(coordinator_id) + "/partitions/" + partition + "/nodes/";
		zookeeper->createAncestors(path);
		(void) zookeeper->create(path + current_host, "", zkutil::CreateMode::Persistent);
	}
}

ReshardingWorker::PartitionList::iterator ReshardingWorker::categorizePartitions(const std::string & coordinator_id,
	PartitionList & partition_list)
{
	std::sort(partition_list.begin(), partition_list.end());

	PartitionList coordinated_partition_list;
	PartitionList uncoordinated_partition_list;

	{
		auto lock = createCoordinatorLock();
		zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

		auto current_host = getFQDNOrHostName();
		auto zookeeper = context.getZooKeeper();

		for (const auto & partition : partition_list)
		{
			auto path = getCoordinatorPath(coordinator_id) + "/partitions/" + partition + "/nodes";
			auto nodes = zookeeper->getChildren(path);
			if ((nodes.size() == 1) && (nodes[0] == current_host))
			{
				uncoordinated_partition_list.push_back(partition);
				zookeeper->removeRecursive(getCoordinatorPath(coordinator_id) + "/partitions/" + partition);
			}
			else
				coordinated_partition_list.push_back(partition);
		}
	}

	partition_list.clear();
	partition_list.reserve(coordinated_partition_list.size() + uncoordinated_partition_list.size());

	partition_list.insert(partition_list.end(),
		std::make_move_iterator(coordinated_partition_list.begin()),
		std::make_move_iterator(coordinated_partition_list.end()));

	partition_list.insert(partition_list.end(),
		std::make_move_iterator(uncoordinated_partition_list.begin()),
		std::make_move_iterator(uncoordinated_partition_list.end()));

	return std::next(partition_list.begin(), coordinated_partition_list.size());
}

size_t ReshardingWorker::getPartitionCount(const std::string & coordinator_id)
{
	auto lock = createCoordinatorLock();
	zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

	return getPartitionCountUnlocked(coordinator_id);
}

size_t ReshardingWorker::getPartitionCountUnlocked(const std::string & coordinator_id)
{
	auto zookeeper = context.getZooKeeper();
	return zookeeper->getChildren(getCoordinatorPath(coordinator_id) + "/partitions").size();
}

size_t ReshardingWorker::getNodeCount(const std::string & coordinator_id)
{
	auto lock = createCoordinatorLock();
	zkutil::RWLock::Guard<zkutil::RWLock::Read> guard{lock};

	auto zookeeper = context.getZooKeeper();
	auto count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");
	return std::stoull(count);
}

void ReshardingWorker::waitForCheckCompletion(const std::string & coordinator_id)
{
	createCheckBarrier(coordinator_id).leave();
}

void ReshardingWorker::waitForOptOutCompletion(const std::string & coordinator_id, size_t count)
{
	createOptOutBarrier(coordinator_id, count).enter();
}

void ReshardingWorker::setStatus(const std::string & coordinator_id, Status status)
{
	auto zookeeper = context.getZooKeeper();
	zookeeper->set(getCoordinatorPath(coordinator_id) + "/status", toString(static_cast<UInt64>(status)));
}

void ReshardingWorker::setStatus(const std::string & coordinator_id, const std::string & hostname,
	Status status)
{
	auto zookeeper = context.getZooKeeper();
	zookeeper->set(getCoordinatorPath(coordinator_id) + "/status/" + hostname,
		toString(static_cast<UInt64>(status)));
}

ReshardingWorker::Status ReshardingWorker::getCoordinatorStatus()
{
	auto zookeeper = context.getZooKeeper();

	auto status_str = zookeeper->get(getCoordinatorPath(current_coordinator_id) + "/status");
	return static_cast<Status>(std::stoull(status_str));
}

bool ReshardingWorker::updateOfflineNodes()
{
	auto zookeeper = context.getZooKeeper();

	auto job_nodes = zookeeper->getChildren(getPartitionPath(current_job) + "/nodes");
	std::sort(job_nodes.begin(), job_nodes.end());

	auto online = zookeeper->getChildren(distributed_path + "/online");
	std::sort(online.begin(), online.end());

	std::vector<std::string> offline(job_nodes.size());
	auto end = std::set_difference(job_nodes.begin(), job_nodes.end(),
		online.begin(), online.end(),
		offline.begin());
	offline.resize(end - offline.begin());

	for (const auto & node : offline)
		zookeeper->set(getCoordinatorPath(current_job.coordinator_id) + "/status/" + node,
			toString(static_cast<UInt64>(STATUS_ON_HOLD)));

	return !offline.empty();
}

/// Note: we don't need any synchronization for the status of a distributed job.
/// That's why we don't protect it with a read/write lock.
ReshardingWorker::Status ReshardingWorker::getStatus()
{
	auto zookeeper = context.getZooKeeper();

	auto coordinator_id = current_job.coordinator_id;

	auto status_str = zookeeper->get(getCoordinatorPath(coordinator_id) + "/status");
	auto coordinator_status = std::stoull(status_str);

	if (coordinator_status != STATUS_OK)
		return static_cast<Status>(coordinator_status);

	(void) updateOfflineNodes();

	auto job_nodes = zookeeper->getChildren(getPartitionPath(current_job) + "/nodes");

	bool has_error = false;
	bool has_on_hold = false;

	/// Determine the status.
	for (const auto & node : job_nodes)
	{
		auto status_str = zookeeper->get(getCoordinatorPath(coordinator_id) + "/status/" + node);
		auto status = std::stoull(status_str);
		if (status == STATUS_ERROR)
			has_error = true;
		else if (status == STATUS_ON_HOLD)
			has_on_hold = true;
	}

	/// Cancellation notifications have priority over error notifications.
	if (has_on_hold)
		return STATUS_ON_HOLD;
	else if (has_error)
		return STATUS_ERROR;
	else
		return STATUS_OK;
}

void ReshardingWorker::attachJob()
{
	if (!current_job.isCoordinated())
		return;

	current_coordinator_id = current_job.coordinator_id;

	auto zookeeper = context.getZooKeeper();

	/// Check if the corresponding coordinator exists. If it doesn't, throw an exception,
	/// silently ignore this job, and switch to the next job.
	if (!zookeeper->exists(getCoordinatorPath(current_job.coordinator_id)))
		throw Exception("Coordinator " + current_job.coordinator_id + " was deleted. Ignoring",
			ErrorCodes::RESHARDING_COORDINATOR_DELETED);

	auto status = getStatus();

	/// Important: always check status in the following order.
	if (status == STATUS_ERROR)
	{
		/// This case is triggered when an error occured on a participating node
		/// while we went offline.
		throw Exception("An error occurred on a remote node", ErrorCodes::RESHARDING_REMOTE_NODE_ERROR);
	}
	else if (status == STATUS_ON_HOLD)
	{
		/// The current distributed job is on hold. Check that all the required nodes are online.
		/// If it is so, proceed further. Otherwise throw an exception and switch to the
		/// next job.
		auto current_host = getFQDNOrHostName();

		setStatus(current_job.coordinator_id, current_host, STATUS_OK);

		/// Wait for all the participating nodes to be ready.
		createRecoveryBarrier(current_job).enter();

		/// Catch any error that could have happened while crossing the barrier.
		abortJobIfRequested();
	}
	else if (status == STATUS_OK)
	{
		/// nothing here
	}
	else
	{
		/// This should never happen but we must take this case into account for the sake
		/// of completeness.
		throw Exception("ReshardingWorker: unexpected status", ErrorCodes::LOGICAL_ERROR);
	}
}

void ReshardingWorker::detachJob()
{
	if (!current_job.isCoordinated())
		return;

	auto zookeeper = context.getZooKeeper();

	bool delete_coordinator = false;

	{
		auto lock = createCoordinatorLock();
		zkutil::RWLock::Guard<zkutil::RWLock::Write> guard{lock};

		auto children = zookeeper->getChildren(getPartitionPath(current_job) + "/nodes");
		if (children.empty())
			throw Exception("ReshardingWorker: unable to detach job", ErrorCodes::LOGICAL_ERROR);
		bool was_last_node = children.size() == 1;

		auto current_host = getFQDNOrHostName();
		zookeeper->remove(getPartitionPath(current_job) + "/nodes/" + current_host);

		if (was_last_node)
		{
			/// All the participating nodes have processed the current partition.
			zookeeper->removeRecursive(getPartitionPath(current_job));
			if (getPartitionCountUnlocked(current_job.coordinator_id) == 0)
			{
				/// All the partitions of the current distributed job have been processed.
				delete_coordinator = true;
			}
		}
	}

	if (delete_coordinator)
		deleteCoordinator(current_job.coordinator_id);
}

void ReshardingWorker::waitForUploadCompletion()
{
	if (!current_job.isCoordinated())
		return;
	createUploadBarrier(current_job).enter();
}

void ReshardingWorker::abortPollingIfRequested()
{
	if (must_stop)
		throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
}

void ReshardingWorker::abortCoordinatorIfRequested()
{
	bool must_abort;

	try
	{
		must_abort = must_stop || (getCoordinatorStatus() != STATUS_OK);
	}
	catch (...)
	{
		must_abort = true;
	}

	if (must_abort)
		throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
}

void ReshardingWorker::abortRecoveryIfRequested()
{
	bool has_offline_nodes = false;
	bool must_abort;

	try
	{
		has_offline_nodes = updateOfflineNodes();
		must_abort = must_stop || has_offline_nodes;
	}
	catch (...)
	{
		must_abort = true;
	}

	if (must_abort)
	{
		if (must_stop)
			throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
		else if (has_offline_nodes)
			throw Exception("Distributed job on hold. Ignoring for now",
				ErrorCodes::RESHARDING_DISTRIBUTED_JOB_ON_HOLD);
		else
		{
			/// We don't throw any exception here because the other nodes waiting
			/// to cross the recovery barrier would get stuck forever into a loop.
			/// Instead we rely on cancellation points to detect this error and
			/// therefore terminate the job.
			setStatus(current_job.coordinator_id, getFQDNOrHostName(), STATUS_ERROR);
		}
	}
}

void ReshardingWorker::abortJobIfRequested()
{
	bool is_remote_node_unavailable = false;
	bool is_remote_node_error = false;

	bool cancellation_result = false;
	if (current_job.isCoordinated())
	{
		try
		{
			auto status = getStatus();
			if (status == STATUS_ON_HOLD)
				is_remote_node_unavailable = true;
			else if (status == STATUS_ERROR)
				is_remote_node_error = true;

			cancellation_result = status != STATUS_OK;
		}
		catch (...)
		{
			cancellation_result = true;
		}
	}

	bool must_abort = must_stop || cancellation_result;

	if (must_abort)
	{
		current_job.is_aborted = true;

		/// Important: always keep the following order.
		if (must_stop)
			throw Exception("Cancelled resharding", ErrorCodes::ABORTED);
		else if (is_remote_node_unavailable)
			throw Exception("Remote node unavailable",
				ErrorCodes::RESHARDING_REMOTE_NODE_UNAVAILABLE);
		else if (is_remote_node_error)
			throw Exception("An error occurred on a remote node",
				ErrorCodes::RESHARDING_REMOTE_NODE_ERROR);
		else
			throw Exception("An error occurred on local node", ErrorCodes::LOGICAL_ERROR);
	}
}

zkutil::RWLock ReshardingWorker::createLock()
{
	auto zookeeper = context.getZooKeeper();

	zkutil::RWLock lock(zookeeper, distributed_lock_path);
	zkutil::RWLock::CancellationHook hook = std::bind(&ReshardingWorker::abortPollingIfRequested, this);
	lock.setCancellationHook(hook);

	return lock;
}

zkutil::RWLock ReshardingWorker::createCoordinatorLock()
{
	auto zookeeper = context.getZooKeeper();

	zkutil::RWLock lock(zookeeper, getCoordinatorPath(current_coordinator_id) + "/lock");

	zkutil::RWLock::CancellationHook hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested, this);
	lock.setCancellationHook(hook);

	return lock;
}

zkutil::Barrier ReshardingWorker::createCheckBarrier(const std::string & coordinator_id)
{
	auto zookeeper = context.getZooKeeper();

	auto node_count = zookeeper->get(getCoordinatorPath(coordinator_id) + "/node_count");
	zkutil::Barrier check_barrier{zookeeper, getCoordinatorPath(coordinator_id) + "/check_barrier",
		std::stoull(node_count)};

	zkutil::Barrier::CancellationHook hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested, this);
	check_barrier.setCancellationHook(hook);

	return check_barrier;
}

zkutil::SingleBarrier ReshardingWorker::createOptOutBarrier(const std::string & coordinator_id,
	size_t count)
{
	auto zookeeper = context.getZooKeeper();

	zkutil::SingleBarrier opt_out_barrier{zookeeper, getCoordinatorPath(coordinator_id)
		+ "/opt_out_barrier", count};

	zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortCoordinatorIfRequested, this);
	opt_out_barrier.setCancellationHook(hook);

	return opt_out_barrier;
}

zkutil::SingleBarrier ReshardingWorker::createRecoveryBarrier(const ReshardingJob & job)
{
	auto zookeeper = context.getZooKeeper();

	auto node_count = zookeeper->getChildren(getPartitionPath(job) + "/nodes").size();

	zkutil::SingleBarrier recovery_barrier{zookeeper, getPartitionPath(job) + "/recovery_barrier", node_count};
	zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortRecoveryIfRequested, this);
	recovery_barrier.setCancellationHook(hook);

	return recovery_barrier;
}

zkutil::SingleBarrier ReshardingWorker::createUploadBarrier(const ReshardingJob & job)
{
	auto zookeeper = context.getZooKeeper();

	auto node_count = zookeeper->getChildren(getPartitionPath(job) + "/nodes").size();

	zkutil::SingleBarrier upload_barrier{zookeeper, getPartitionPath(job) + "/upload_barrier", node_count};
	zkutil::SingleBarrier::CancellationHook hook = std::bind(&ReshardingWorker::abortJobIfRequested, this);
	upload_barrier.setCancellationHook(hook);

	return upload_barrier;
}

std::string ReshardingWorker::computeHash(const std::string & in)
{
	unsigned char hash[SHA512_DIGEST_LENGTH];

	SHA512_CTX ctx;
	SHA512_Init(&ctx);
	SHA512_Update(&ctx, reinterpret_cast<const void *>(in.data()), in.size());
	SHA512_Final(hash, &ctx);

	std::string out;
	{
		WriteBufferFromString buf(out);
		HexWriteBuffer hex_buf(buf);
		hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
	}

	return out;
}

std::string ReshardingWorker::getCoordinatorPath(const std::string & coordinator_id) const
{
	return coordination_path + "/" + coordinator_id;
}

std::string ReshardingWorker::getPartitionPath(const ReshardingJob & job) const
{
	return coordination_path + "/" + job.coordinator_id + "/partitions/" + job.partition;
}

}
