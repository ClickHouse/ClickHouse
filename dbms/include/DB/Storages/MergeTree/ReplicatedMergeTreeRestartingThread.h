#pragma once

#include <Poco/Event.h>
#include <common/logger_useful.h>
#include <DB/Core/Types.h>
#include <thread>
#include <atomic>


namespace DB
{

class StorageReplicatedMergeTree;


/** Инициализирует сессию в ZK.
  * Выставляет эфемерные ноды. Выставляет нужные для обнаружения реплики значения нод.
  * Запускает участие в выборе лидера. Запускает все фоновые потоки.
  * Затем следит за тем, не истекла ли сессия. И если истекла - переинициализирует её.
  */
class ReplicatedMergeTreeRestartingThread
{
public:
	ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_);

	~ReplicatedMergeTreeRestartingThread()
	{
		if (thread.joinable())
			thread.join();
	}

	void wakeup()
	{
		wakeup_event.set();
	}

	Poco::Event & getWakeupEvent()
	{
		return wakeup_event;
	}

	void stop()
	{
		need_stop = true;
		wakeup();
	}

	void getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay) const
	{
		out_absolute_delay = absolute_delay.load(std::memory_order_relaxed);
		out_relative_delay = relative_delay.load(std::memory_order_relaxed);
	}

private:
	StorageReplicatedMergeTree & storage;
	Logger * log;
	Poco::Event wakeup_event;
	volatile bool need_stop = false;

	/// Случайные данные, которые мы записали в /replicas/me/is_active.
	String active_node_identifier;

	std::thread thread;

	/// Отставание реплики.
	std::atomic<time_t> absolute_delay {};
	std::atomic<time_t> relative_delay {};


	void run();

	/// Запустить или остановить фоновые потоки. Используется для частичной переинициализации при пересоздании сессии в ZooKeeper.
	bool tryStartup(); /// Возвращает false, если недоступен ZooKeeper.

	/// Отметить в ZooKeeper, что эта реплика сейчас активна.
	void activateReplica();

	/// Удалить куски, для которых кворум пофейлился (за то время, когда реплика была неактивной).
	void removeFailedQuorumParts();

	/// Если есть недостигнутый кворум, и у нас есть кусок, то добавить эту реплику в кворум.
	void updateQuorumIfWeHavePart();

	void partialShutdown();

	/// Запретить запись в таблицу и завершить все фоновые потоки.
	void goReadOnlyPermanently();

	/// Получить информацию об отставании реплик.
	void checkReplicationDelays(time_t & out_absolute_delay, time_t & out_relative_delay);
};


}
