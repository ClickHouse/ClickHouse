#pragma once

#include <DB/Storages/AlterCommands.h>
#include <common/logger_useful.h>
#include <Poco/SharedPtr.h>
#include <string>
#include <thread>
#include <atomic>

namespace DB
{

class Context;
class StorageReplicatedMergeTree;
class ReshardingJob;

/** Исполнитель задач перешардирования.
  * Рабоает в фоновом режиме внутри одного потока.
  * Следит за появлением задач и назначает их на выполнение.
  * Задачи выполняются последовательно.
  */
class ReshardingWorker final
{
public:
	ReshardingWorker(Context & context_);

	ReshardingWorker(const ReshardingWorker &) = delete;
	ReshardingWorker & operator=(const ReshardingWorker &) = delete;

	~ReshardingWorker();

	/// Запустить поток выполняющий задачи перешардирования.
	void start();

	/// Прислать запрос на перешардирование.
	void submitJob(const std::string & database_name,
		const std::string & table_name,
		const std::string & partition,
		const WeightedZooKeeperPaths & weighted_zookeeper_paths,
		const std::string & sharding_key);

	/// Прислать запрос на перешардирование.
	void submitJob(const ReshardingJob & job);

	/// Был ли поток запущен?
	bool isStarted() const;

private:
	/// Прислать запрос на перешардирование (внутренняя версия).
	void submitJobImpl(const std::string & serialized_job);

	/// Следить за появлением новых задач. Выполнить их последовательно.
	void pollAndExecute();

	/// Выполнить задачи, которые были в очереди выполнения при запуске узла.
	void performPendingJobs();

	/// Выполнить задачи, которые заданы по путям в БД ZooKeeper.
	void perform(const Strings & job_nodes);

	/// Выполнить одну задачу.
	void perform(const ReshardingJob & job);

	/// Разбить куски входящие в партицию на несколько, согласно ключу шардирования.
	/// Оновременно перегруппировать эти куски по шардам и слить куски в каждой группе.
	/// При завершении этого процесса создаётся новая партиция для каждого шарда.
	void createShardedPartitions(StorageReplicatedMergeTree & storage, const ReshardingJob & job);

	/// Копировать все партиции полученные путём перешардирования на каждую реплику
	/// соответствующих шардов.
	void publishShardedPartitions(StorageReplicatedMergeTree & storage, const ReshardingJob & job);

	/// Для каждого шарда добавить данные из новой партиции этого шарда в таблицу на всех
	/// репликах входящих в этот же шард. На локальном узле, который выполняет задачу
	/// перешардирования, удалить данные из первоначальной партиции.
	void applyChanges(StorageReplicatedMergeTree & storage, const ReshardingJob & job);

	/// Удалить временные данные с локального узла и ZooKeeper'а.
	void cleanup(StorageReplicatedMergeTree & storage, const ReshardingJob & job);

	/// Принудительно завершить поток.
	void abortIfRequested() const;

	/// Был ли поток завершён?
	bool hasAborted(const Exception & ex) const;

private:
	Context & context;
	Logger * log;
	std::thread polling_thread;
	std::string host_task_queue_path;
	std::atomic<bool> is_started{false};
	std::atomic<bool> must_stop{false};
};

using ReshardingWorkerPtr = Poco::SharedPtr<ReshardingWorker>;

}
