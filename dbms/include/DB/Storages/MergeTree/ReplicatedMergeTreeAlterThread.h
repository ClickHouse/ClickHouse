#pragma once

#include <thread>
#include <zkutil/Types.h>
#include <DB/Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{

class StorageReplicatedMergeTree;


/** Следит за изменением структуры таблицы в ZooKeeper и выполняет необходимые преобразования.
  *
  * NOTE Это не имеет отношения к манипуляциям с партициями,
  *  которые обрабатываются через очередь репликации.
  */
class ReplicatedMergeTreeAlterThread
{
public:
	ReplicatedMergeTreeAlterThread(StorageReplicatedMergeTree & storage_);

	~ReplicatedMergeTreeAlterThread()
	{
		need_stop = true;
		wakeup_event->set();
		if (thread.joinable())
			thread.join();
	}

private:
	void run();

	StorageReplicatedMergeTree & storage;
	Logger * log;

	zkutil::EventPtr wakeup_event { new Poco::Event };
	volatile bool need_stop { false };

	std::thread thread;
};

}
