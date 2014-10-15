#pragma once

#include <Yandex/logger_useful.h>
#include <thread>


namespace DB
{

class StorageReplicatedMergeTree;


/** Удаляет устаревшие данные таблицы типа ReplicatedMergeTree.
  */
class ReplicatedMergeTreeCleanupThread
{
public:
	ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_);

	~ReplicatedMergeTreeCleanupThread()
	{
		if (thread.joinable())
			thread.join();
	}

private:
	StorageReplicatedMergeTree & storage;
	Logger * log;
	std::thread thread;

	void run();
	void iterate();

	/// Удалить старые куски с диска и из ZooKeeper.
	void clearOldParts();

	/// Удалить из ZooKeeper старые записи в логе.
	void clearOldLogs();

	/// Удалить из ZooKeeper старые хеши блоков. Это делает ведущая реплика.
	void clearOldBlocks();
};


}
