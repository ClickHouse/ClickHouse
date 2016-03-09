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
public:
	using PartitionList = std::vector<std::string>;

	enum Status
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
	void setStatus(const std::string & coordinator_id, Status status);
	/// Set the status of a shard under a given coordinator.
	void setStatus(const std::string & coordinator_id, const std::string & hostname, Status status);

	zkutil::RWLock createDeletionLock(const std::string & coordinator_id);

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
	void perform(const std::string & job_descriptor);

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
	void applyChanges();

	/// Удалить временные данные с локального узла и ZooKeeper'а.
	void softCleanup();
	void hardCleanup();

	/// Принудительно завершить поток, если выполнено условие.
	void abortPollingIfRequested();
	void abortCoordinatorIfRequested(const std::string & coordinator_id);
	void abortRecoveryIfRequested();
	void abortJobIfRequested();

	/// Get the current job-independent status of the coordinator.
	Status getCoordinatorStatus(const std::string & coordinator_id);
	/// Get the status of the current distributed job.
	Status getStatus();

	/// Зарегистрировать задачу в соответствующий координатор.
	void attachJob();
	/// Снять задачу с координатора.
	void detachJob();
	/// Ждать завершение загрузок на всех исполнителях.
	void waitForUploadCompletion();

	size_t getPartitionCountUnlocked(const std::string & coordinator_id);

	/// Detect offline nodes under a given coordinator.
	bool detectOfflineNodes(const std::string & coordinator_id);
	/// Detect offline nodes under the current job.
	bool detectOfflineNodes();

	/// Функции, которые создают необходимые объекты для синхронизации
	/// распределённых задач.
	zkutil::RWLock createLock();
	zkutil::RWLock createCoordinatorLock(const std::string & coordinator_id);
	zkutil::SingleBarrier createCheckBarrier(const std::string & coordinator_id);
	zkutil::SingleBarrier createOptOutBarrier(const std::string & coordinator_id, size_t count);
	zkutil::SingleBarrier createRecoveryBarrier(const ReshardingJob & job);
	zkutil::SingleBarrier createUploadBarrier(const ReshardingJob & job);

	/// Get the ZooKeeper path of a given coordinator.
	std::string getCoordinatorPath(const std::string & coordinator_id) const;
	/// Get the ZooKeeper path of a given job partition.
	std::string getPartitionPath(const ReshardingJob & job) const;

	/// Common code for softCleanup() and hardCleanup().
	void cleanupCommon();
	/// Common code for detectOfflineNodes().
	bool detectOfflineNodesCommon(const std::string & path, const std::string & coordinator_id);
	/// Common code for getStatus() and getCoordinatorStatus().
	Status getStatusCommon(const std::string & path, const std::string & coordinator_id);

	/// Compute the hash value of a given string.
	std::string computeHash(const std::string & in);

private:
	ReshardingJob current_job;
	std::thread polling_thread;

	std::string host_task_queue_path;
	std::string distributed_path;
	std::string distributed_online_path;
	std::string distributed_lock_path;
	std::string coordination_path;

	Context & context;
	Logger * log;

	zkutil::EventPtr event = new Poco::Event;

	std::atomic<bool> is_started{false};
	std::atomic<bool> must_stop{false};
};

using ReshardingWorkerPtr = std::shared_ptr<ReshardingWorker>;

}
