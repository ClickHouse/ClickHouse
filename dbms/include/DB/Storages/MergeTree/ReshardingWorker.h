#pragma once

#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/Storages/AlterCommands.h>
#include <common/logger_useful.h>

#include <zkutil/RWLock.h>
#include <zkutil/SingleBarrier.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <string>
#include <thread>
#include <atomic>
#include <functional>

namespace DB
{

class Context;
class Cluster;
class StorageReplicatedMergeTree;

/// Performer of resharding jobs. It works as a background task within a thread.
/// Its main duties are to keep track of newly submitted resharding jobs and
/// to execute them sequentially.
class ReshardingWorker final
{
	friend class AnomalyMonitor;

public:
	using PartitionList = std::vector<std::string>;

	/// Possible status values of a coordinator or a performer that
	/// has subscribed to a coordinator.
	enum StatusCode
	{
		STATUS_OK = 0,
		STATUS_ERROR,	/// An error occurred on a performer.
		STATUS_ON_HOLD	/// Job is stopped.
	};

public:
	ReshardingWorker(const Poco::Util::AbstractConfiguration & config,
		const std::string & config_name, Context & context_);

	ReshardingWorker(const ReshardingWorker &) = delete;
	ReshardingWorker & operator=(const ReshardingWorker &) = delete;

	~ReshardingWorker();

	/// Start the job tracker thread.
	void start();

	/// Stop the job tracker thread. If any job is in progress, put it on hold
	/// for future execution.
	void shutdown();

	/// Send a request for resharding on the current performer.
	void submitJob(const ReshardingJob & job);

	/// Is the job tracker thread started?
	bool isStarted() const;

	/// Create a new coordinator for a distributed job. Called from the initiator.
	std::string createCoordinator(const Cluster & cluster);
	/// Register a query into a coordinator.
	void registerQuery(const std::string & coordinator_id, const std::string & query);
	/// Clear all the information related to a given coordinator.
	void deleteCoordinator(const std::string & coordinator_id);

	/// Subscribe the performer, from which this method is called, to a given coordinator.
	UInt64 subscribe(const std::string & coordinator_id, const std::string & query);
	/// Cancel the aforementionned subscription.
	void unsubscribe(const std::string & coordinator_id);
	/// Увеличить количество партиций входящих в одну распределённую задачу. Вызывается с исполнителя.
	void addPartitions(const std::string & coordinator_id, const PartitionList & partition_list);
	/// Rearrange partitions into two categories: coordinated job, uncoordinated job.
	/// Returns an iterator to the beginning of the list of uncoordinated jobs.
	ReshardingWorker::PartitionList::iterator categorizePartitions(const std::string & coordinator_id,
		ReshardingWorker::PartitionList & partition_list);
	/// Get the number of partitions of a distributed job. Called from performers.
	size_t getPartitionCount(const std::string & coordinator_id);
	/// Get the number of performers.
	size_t getNodeCount(const std::string & coordinator_id);
	/// Wait for all the preliminary sanity checks to be completed on all the performers.
	/// Called from performers.
	void waitForCheckCompletion(const std::string & coordinator_id);
	/// Wait for all the required unsubscribe operations to be completed.
	void waitForOptOutCompletion(const std::string & coordinator_id, size_t count);

	/// Set the shard-independent status of a given coordinator.
	void setStatus(const std::string & coordinator_id, StatusCode status, const std::string & msg = "");
	/// Set the status of a shard under a given coordinator.
	void setStatus(const std::string & coordinator_id, const std::string & hostname,
		StatusCode status, const std::string & msg = "");

	zkutil::RWLock createDeletionLock(const std::string & coordinator_id);

	/// Dump the status messages of the coordinator and all the performers.
	std::string dumpCoordinatorState(const std::string & coordinator_id);

	static std::string computeHashFromPart(const std::string & path);

private:
	/// Anomalies that may be detected by the current performer.
	enum AnomalyType
	{
		ANOMALY_NONE = 0,
		ANOMALY_LOCAL_SHUTDOWN,
		ANOMALY_LOCAL_ERROR,
		ANOMALY_REMOTE_NODE_UNAVAILABLE,
		ANOMALY_REMOTE_ERROR
	};

	/// This structure stores various required information for log creation.
	struct TargetShardInfo
	{
		TargetShardInfo(size_t shard_no_, const std::string & part_name_, const std::string & hash_)
			: part_name{part_name_}, hash{hash_}, shard_no{shard_no_}
		{
		}

		std::string part_name;
		std::string hash;
		size_t shard_no;
	};

	using ShardList = std::vector<TargetShardInfo>;

	/// Structure that describes an operation to be performed as part of
	/// a commit procedure.
	class LogRecord final
	{
	public:
		/// Create a new log record.
		LogRecord(zkutil::ZooKeeperPtr zookeeper_);
		/// Open an already existing log record.
		LogRecord(zkutil::ZooKeeperPtr zookeeper_, const std::string & zk_path_);
		/// Append this log record into the specified log onto persistent storage.
		void enqueue(const std::string & log_path);
		/// Update an already existing log record.
		void writeBack();

	public:
		enum Operation
		{
			OP_DROP = 0,
			OP_ATTACH
		};

		enum State
		{
			READY = 0,
			RUNNING,
			DONE
		};

	private:
		/// Serialize this log record.
		std::string toString();

	public:
		zkutil::ZooKeeperPtr zookeeper;
		Operation operation;
		/// Source partition being resharded.
		std::string partition;
		/// Hash of the source partition (for drop operations).
		std::string partition_hash;
		/// Path of this log record on stable storage.
		std::string zk_path;
		/// List of the parts that constitute a sharded partition along with
		/// their respective hash values (for attach operations).
		std::map<std::string, std::string> parts_with_hash;
		/// Target shard index into the list of specified target shards inside
		/// a ALTER TABLE ... RESHARD request.
		size_t shard_no = 0;
		State state;
	};

private:
	/// Keep track of newly submitted jobs. Perform them sequentially.
	void trackAndPerform();

	/// Wake up the tracker thread if it was ever sleeping.
	void wakeUpTrackerThread();

	/// Perform all the jobs that were already pending when the ClickHouse instance
	/// on this node was starting.
	void performPendingJobs();

	/// Sequentially perform jobs which are specified by their corresponding
	/// znodes in ZooKeeper.
	void perform(const Strings & job_nodes);

	/// Perform one job.
	void perform(const std::string & job_descriptor, const std::string & job_name);

	/// Разбить куски входящие в партицию на несколько, согласно ключу шардирования.
	/// Оновременно перегруппировать эти куски по шардам и слить куски в каждой группе.
	/// При завершении этого процесса создаётся новая партиция для каждого шарда.
	void createShardedPartitions();

	/// Upload all the partitions resulting from source partition resharding to their
	/// respective target shards.
	void publishShardedPartitions();

	/// On each target shard attach its corresponding new sharded partition.
	/// If COPY is not specified in the ALTER TABLE ... RESHARD ... query,
	/// drop source partitions on each performer.
	void commit();

	void repairLogRecord(LogRecord & log_record);
	void executeLogRecord(LogRecord & log_record);
	void executeDrop(LogRecord & log_record);
	void executeAttach(LogRecord & log_record);
	bool checkAttachLogRecord(LogRecord & log_record);

	/// Delete temporary data from the local node and ZooKeeper.
	void softCleanup();
	void hardCleanup();

	/// Forcibly abort the job tracker thread if requested.
	void abortTrackingIfRequested();
	/// Forcibly abort the distributed job coordinated by the given coordinator
	/// if requested.
	void abortCoordinatorIfRequested(const std::string & coordinator_id);
	/// Forcibly abort the recovery of the current distributed job if requested.
	void abortRecoveryIfRequested();
	/// Forcibly abort the current job if requested.
	void abortJobIfRequested();

	/// Get the current job-independent status of the coordinator.
	StatusCode getCoordinatorStatus(const std::string & coordinator_id);
	/// Get the status of the current distributed job.
	StatusCode getStatus();

	/// Prepare the current job for execution.
	void initializeJob();
	/// Remove the current job's data from its coordinator.
	/// If there is no job left, also delete the coordinator.
	void finalizeJob();
	/// Wait for all the necesssary sharded partitions uploads to their respective
	/// target shards to be completed.
	void waitForUploadCompletion();
	/// Wait for all the performers to be checked in for leader election.
	void waitForElectionCompletion();
	/// Wait for all the changes required by the current distributed resharding job
	/// to be applied by all the performers on all the target shards.
	void waitForCommitCompletion();

	size_t getPartitionCountUnlocked(const std::string & coordinator_id);

	/// Detect offline performers under a given coordinator.
	bool detectOfflineNodes(const std::string & coordinator_id);
	/// Detect offline performers under the current job.
	bool detectOfflineNodes();

	bool isPublished();
	void markAsPublished();

	bool isLogCreated();
	void markLogAsCreated();

	bool isCommitted();
	void markAsCommitted();

	/// Store onto ZooKeeper information about the target shards. It is used
	/// in order to create the log of operations that apply changes all the
	/// changes required by the resharding operation.
	void storeTargetShardsInfo();

	/// Retrieve from ZooKeeper the aforementioned information about the target shards.
	ShardList getTargetShardsInfo(const std::string & hostname, const std::string & job_name);

	/// Create the log of operations.
	void createLog();

	void electLeader();
	bool isLeader();

	/// Access the global lock handler.
	zkutil::RWLock getGlobalLock();
	/// Acccess the given coordinator lock handler.
	zkutil::RWLock getCoordinatorLock(const std::string & coordinator_id,
		bool usable_in_emergency = false);

	/// The following functions are to design access barrier handlers we use
	/// to synchronize the progression of distributed jobs.
	zkutil::SingleBarrier getCheckBarrier(const std::string & coordinator_id);
	zkutil::SingleBarrier getOptOutBarrier(const std::string & coordinator_id, size_t count);
	zkutil::SingleBarrier getRecoveryBarrier();
	zkutil::SingleBarrier getUploadBarrier();
	zkutil::SingleBarrier getElectionBarrier();
	zkutil::SingleBarrier getCommitBarrier();

	/// Get the ZooKeeper path of a given coordinator.
	std::string getCoordinatorPath(const std::string & coordinator_id) const;
	/// Get the ZooKeeper path of a given job partition.
	std::string getPartitionPath() const;
	/// Get the ZooKeeper path of the job currently running on this performer.
	std::string getLocalJobPath() const;

	/// Common code for softCleanup() and hardCleanup().
	void deleteTemporaryData();
	/// Common code for detectOfflineNodes().
	bool detectOfflineNodesCommon(const std::string & path, const std::string & coordinator_id);
	/// Common code for getStatus() and getCoordinatorStatus().
	StatusCode getStatusCommon(const std::string & path, const std::string & coordinator_id);

	AnomalyType probeForAnomaly();
	void processAnomaly(AnomalyType anomaly_type);

private:
	/// This class is used to spawn a thread which periodically checks for any
	/// anomaly that should lead to abort the job currently being processed.
	/// Jobs perform such checks with a negligible performance overhead by calling
	/// the method getAnomalyType() which merely reads the value of an atomic variable.
	class AnomalyMonitor final
	{
	public:
		AnomalyMonitor(ReshardingWorker & resharding_worker_);
		~AnomalyMonitor();

		AnomalyMonitor(const AnomalyMonitor &) = delete;
		AnomalyMonitor & operator=(const AnomalyMonitor &) = delete;

		/// Start the thread.
		void start();
		/// Shutdown the thread.
		void shutdown();
		/// Get the type of anomaly that has been registered.
		AnomalyType getAnomalyType() const;

	private:
		void routine();

	private:
		ReshardingWorker & resharding_worker;
		std::thread thread_routine;

		std::atomic<ReshardingWorker::AnomalyType> anomaly_type{ReshardingWorker::ANOMALY_NONE};
		std::atomic<bool> is_started{false};
		std::atomic<bool> must_stop{false};
	};

	/// Guarded exception-safe handling of the class above.
	class ScopedAnomalyMonitor final
	{
	public:
		ScopedAnomalyMonitor(AnomalyMonitor & anomaly_monitor_)
			: anomaly_monitor{anomaly_monitor_}
		{
			anomaly_monitor.start();
		}

		~ScopedAnomalyMonitor()
		{
			try
			{
				anomaly_monitor.shutdown();
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}
		}

		ScopedAnomalyMonitor(const ScopedAnomalyMonitor &) = delete;
		ScopedAnomalyMonitor & operator=(const ScopedAnomalyMonitor &) = delete;

	private:
		AnomalyMonitor & anomaly_monitor;
	};

private:
	ReshardingJob current_job;
	std::thread job_tracker;
	AnomalyMonitor anomaly_monitor{*this};

	std::string task_queue_path;
	std::string host_task_queue_path;
	std::string distributed_path;
	std::string distributed_online_path;
	std::string distributed_lock_path;
	std::string coordination_path;

	Context & context;
	Logger * log = &Logger::get("ReshardingWorker");

	zkutil::EventPtr event = std::make_shared<Poco::Event>();

	/// Helper that acquires an alive ZooKeeper session.
	zkutil::GetZooKeeper get_zookeeper;

	std::atomic<bool> is_started{false};
	std::atomic<bool> must_stop{false};
};

using ReshardingWorkerPtr = std::shared_ptr<ReshardingWorker>;

}
