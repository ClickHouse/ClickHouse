#pragma once

#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/Storages/AlterCommands.h>
#include <common/logger_useful.h>

#include <zkutil/RWLock.h>
#include <zkutil/SingleBarrier.h>

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/SharedPtr.h>

#include <string>
#include <thread>
#include <atomic>
#include <functional>

namespace DB
{

class Context;
class Cluster;
class StorageReplicatedMergeTree;

/** Исполнитель задач перешардирования.
  * Рабоает в фоновом режиме внутри одного потока.
  * Следит за появлением задач и назначает их на выполнение.
  * Задачи выполняются последовательно.
  */
class ReshardingWorker final
{
	friend class AnomalyMonitor;

public:
	using PartitionList = std::vector<std::string>;

	/// Possible status values of a coordinator or a node that
	/// has subscribed to a coordinator.
	enum StatusCode
	{
		STATUS_OK = 0,
		STATUS_ERROR,	/// Произошла ошибка на одном исполнителе.
		STATUS_ON_HOLD	/// Задача приостановлена.
	};

public:
	ReshardingWorker(const Poco::Util::AbstractConfiguration & config,
		const std::string & config_name, Context & context_);

	ReshardingWorker(const ReshardingWorker &) = delete;
	ReshardingWorker & operator=(const ReshardingWorker &) = delete;

	~ReshardingWorker();

	/// Start the thread which performs resharding jobs.
	void start();

	/// Stop the thread which performs resharding jobs.
	/// If any job is in progress, put it on hold for further execution.
	void shutdown();

	/// Прислать запрос на перешардирование.
	void submitJob(const ReshardingJob & job);

	/// Был ли поток запущен?
	bool isStarted() const;

	/// Создать новый координатор распределённой задачи. Вызывается с инициатора.
	std::string createCoordinator(const Cluster & cluster);
	/// Register a query into a coordinator.
	void registerQuery(const std::string & coordinator_id, const std::string & query);
	/// Удалить координатор.
	void deleteCoordinator(const std::string & coordinator_id);

	/// Подписаться к заданному координатору. Вызывается с исполнителя.
	UInt64 subscribe(const std::string & coordinator_id, const std::string & query);
	/// Отменить подпись к заданному координатору. Вызывается с исполнителя.
	void unsubscribe(const std::string & coordinator_id);
	/// Увеличить количество партиций входящих в одну распределённую задачу. Вызывается с исполнителя.
	void addPartitions(const std::string & coordinator_id, const PartitionList & partition_list);
	/// Rearrange partitions into two categories: coordinated job, uncoordinated job.
	/// Returns an iterator to the beginning of the list of uncoordinated jobs.
	ReshardingWorker::PartitionList::iterator categorizePartitions(const std::string & coordinator_id,
		ReshardingWorker::PartitionList & partition_list);
	/// Получить количество партиций входящих в одну распределённую задачу. Вызывается с исполнителя.
	size_t getPartitionCount(const std::string & coordinator_id);
	/// Получить количество учавствующих узлов.
	size_t getNodeCount(const std::string & coordinator_id);
	/// Ждать завершение проверок на всех исполнителях. Вызывается с исполнителя.
	void waitForCheckCompletion(const std::string & coordinator_id);
	/// Ждать завершение всех необходмых отмен подписей.
	void waitForOptOutCompletion(const std::string & coordinator_id, size_t count);

	/// Set the shard-independent status of a given coordinator.
	void setStatus(const std::string & coordinator_id, StatusCode status, const std::string & msg = "");
	/// Set the status of a shard under a given coordinator.
	void setStatus(const std::string & coordinator_id, const std::string & hostname,
		StatusCode status, const std::string & msg = "");

	zkutil::RWLock createDeletionLock(const std::string & coordinator_id);

	/// Dump the status messages of the coordinator and all the participating nodes.
	std::string dumpCoordinatorState(const std::string & coordinator_id);

private:
	/// Anomalies that may be detected by the local node.
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
	/// Следить за появлением новых задач. Выполнить их последовательно.
	void pollAndExecute();

	/// Подтолкнуть планировщик задач.
	void jabScheduler();

	/// Выполнить задачи, которые были в очереди выполнения при запуске узла.
	void performPendingJobs();

	/// Выполнить задачи, которые заданы по путям в БД ZooKeeper.
	void perform(const Strings & job_nodes);

	/// Выполнить одну задачу.
	void perform(const std::string & job_descriptor, const std::string & job_name);

	/// Разбить куски входящие в партицию на несколько, согласно ключу шардирования.
	/// Оновременно перегруппировать эти куски по шардам и слить куски в каждой группе.
	/// При завершении этого процесса создаётся новая партиция для каждого шарда.
	void createShardedPartitions();

	/// Копировать все партиции полученные путём перешардирования на каждую реплику
	/// соответствующих шардов.
	void publishShardedPartitions();

	/// Для каждого шарда добавить данные из новой партиции этого шарда в таблицу на всех
	/// репликах входящих в этот же шард. На локальном узле, который выполняет задачу
	/// перешардирования, удалить данные из первоначальной партиции.
	void commit();

	void repairLogRecord(LogRecord & log_record);
	void executeLogRecord(LogRecord & log_record);
	void executeDrop(LogRecord & log_record);
	void executeAttach(LogRecord & log_record);
	bool checkAttachLogRecord(LogRecord & log_record);

	/// Удалить временные данные с локального узла и ZooKeeper'а.
	void softCleanup();
	void hardCleanup();

	/// Принудительно завершить поток, если выполнено условие.
	void abortPollingIfRequested();
	void abortCoordinatorIfRequested(const std::string & coordinator_id);
	void abortRecoveryIfRequested();
	void abortJobIfRequested();

	/// Get the current job-independent status of the coordinator.
	StatusCode getCoordinatorStatus(const std::string & coordinator_id);
	/// Get the status of the current distributed job.
	StatusCode getStatus();

	/// Зарегистрировать задачу в соответствующий координатор.
	void attachJob();
	/// Снять задачу с координатора.
	void detachJob();
	/// Ждать завершение загрузок на всех исполнителях.
	void waitForUploadCompletion();
	///
	void waitForElectionCompletion();
	/// Wait for all the partition operations to be completed on all the participating nodes.
	void waitForCommitCompletion();

	size_t getPartitionCountUnlocked(const std::string & coordinator_id);

	/// Detect offline nodes under a given coordinator.
	bool detectOfflineNodes(const std::string & coordinator_id);
	/// Detect offline nodes under the current job.
	bool detectOfflineNodes();

	bool isPublished();
	void markAsPublished();

	bool isLogCreated();
	void markLogAsCreated();

	bool isCommitted();
	void markAsCommitted();

	///
	void storeTargetShards();
	///
	ShardList getTargetShards(const std::string & hostname, const std::string & job_name);

	///
	void createLog();

	void electLeader();
	bool isLeader();

	/// Функции, которые создают необходимые объекты для синхронизации
	/// распределённых задач.
	zkutil::RWLock createLock();
	zkutil::RWLock createCoordinatorLock(const std::string & coordinator_id,
		bool usable_in_emergency = false);
	zkutil::SingleBarrier createCheckBarrier(const std::string & coordinator_id);
	zkutil::SingleBarrier createOptOutBarrier(const std::string & coordinator_id, size_t count);
	zkutil::SingleBarrier createRecoveryBarrier(const ReshardingJob & job);
	zkutil::SingleBarrier createUploadBarrier(const ReshardingJob & job);
	zkutil::SingleBarrier createElectionBarrier(const ReshardingJob & job);
	zkutil::SingleBarrier createCommitBarrier(const ReshardingJob & job);

	/// Prevent merging jobs from being performed on the partition that we
	/// want to reshard on the current host. This operation is persistent:
	/// even if a node failure occurred, a partition remains frozen as long
	/// as it is not unfrozen explicitely. So use it with care as regards
	/// exceptions.
	void freezeSourcePartition();

	/// Make the partition that we want to reshard available for merging jobs.
	void unfreezeSourcePartition();

	/// Get the ZooKeeper path of a given coordinator.
	std::string getCoordinatorPath(const std::string & coordinator_id) const;
	/// Get the ZooKeeper path of a given job partition.
	std::string getPartitionPath(const ReshardingJob & job) const;
	///
	std::string getLocalJobPath(const ReshardingJob & job) const;

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
	std::thread polling_thread;
	AnomalyMonitor anomaly_monitor{*this};

	std::string task_queue_path;
	std::string host_task_queue_path;
	std::string distributed_path;
	std::string distributed_online_path;
	std::string distributed_lock_path;
	std::string coordination_path;

	Context & context;
	Logger * log = &Logger::get("ReshardingWorker");

	zkutil::EventPtr event = new Poco::Event;
	zkutil::GetZooKeeper get_zookeeper;

	std::atomic<bool> is_started{false};
	std::atomic<bool> must_stop{false};
};

using ReshardingWorkerPtr = std::shared_ptr<ReshardingWorker>;

}
