#pragma once

#include <set>
#include <map>
#include <list>
#include <mutex>
#include <thread>
#include <atomic>
#include <Poco/Event.h>
#include <DB/Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{

class StorageReplicatedMergeTree;


/** Проверяет целостность кусков, запрошенных для проверки.
  *
  * Определяет лишние куски и убирает их из рабочего набора.
  * Находит отсутствующие куски и добавляет их для скачивания с реплик.
  * Проверяет целостность данных и, в случае нарушения,
  *  убирает кусок из рабочего набора и добавляет для скачивания с реплик.
  */
class ReplicatedMergeTreePartCheckThread
{
public:
	ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_);

	/// Разбор очереди для проверки осуществляется в фоновом потоке, который нужно сначала запустить.
	void start();
	void stop();

	/// Добавить кусок (для которого есть подозрения, что он отсутствует, повреждён или не нужен) в очередь для проверки.
	/// delay_to_check_seconds - проверять не раньше чем через указанное количество секунд.
	void enqueuePart(const String & name, time_t delay_to_check_seconds = 0);

	/// Получить количество кусков в очереди для проверки.
	size_t size() const;

	~ReplicatedMergeTreePartCheckThread()
	{
		stop();
	}

private:
	void run();

	void checkPart(const String & part_name);
	void searchForMissingPart(const String & part_name);

	StorageReplicatedMergeTree & storage;
	Logger * log;

	using StringSet = std::set<String>;
	using PartToCheck = std::pair<String, time_t>;	/// Имя куска и минимальное время для проверки (или ноль, если не важно).
	using PartsToCheckQueue = std::list<PartToCheck>;

	/** Куски, для которых нужно проверить одно из двух:
	  *  - Если кусок у нас есть, сверить, его данные с его контрольными суммами, а их с ZooKeeper.
	  *  - Если куска у нас нет, проверить, есть ли он (или покрывающий его кусок) хоть у кого-то.
	  */

	StringSet parts_set;
	PartsToCheckQueue parts_queue;
	mutable std::mutex mutex;
	Poco::Event wakeup_event;
	std::atomic<bool> need_stop { false };

	std::thread thread;
};

}
