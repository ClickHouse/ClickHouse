#include <DB/IO/Operators.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>


namespace DB
{


/// Используется для проверки, выставили ли ноду is_active мы, или нет.
static String generateActiveNodeIdentifier()
{
	struct timespec times;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
		throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);
	return toString(times.tv_nsec + times.tv_sec + getpid());
}


ReplicatedMergeTreeRestartingThread::ReplicatedMergeTreeRestartingThread(StorageReplicatedMergeTree & storage_)
	: storage(storage_),
	log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, RestartingThread)")),
	active_node_identifier(generateActiveNodeIdentifier()),
	thread([this] { run(); })
{
}


void ReplicatedMergeTreeRestartingThread::run()
{
	constexpr auto retry_delay_ms = 10 * 1000;
	constexpr auto check_delay_ms = 60 * 1000;

	try
	{
		/// Запуск реплики при старте сервера/создании таблицы.
		while (!need_stop && !tryStartup())
			wakeup_event.tryWait(retry_delay_ms);

		/// Цикл перезапуска реплики при истечении сессии с ZK.
		while (!need_stop)
		{
			if (storage.zookeeper->expired())
			{
				LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");
				storage.is_read_only = true;

				partialShutdown();

				do
				{
					try
					{
						/// TODO race condition при присваивании?
						storage.zookeeper = storage.context.getZooKeeper();
					}
					catch (const zkutil::KeeperException & e)
					{
						/// Исключение при попытке zookeeper_init обычно бывает, если не работает DNS. Будем пытаться сделать это заново.
						tryLogCurrentException(__PRETTY_FUNCTION__);
						wakeup_event.tryWait(retry_delay_ms);
						continue;
					}
				} while (false);

				while (!need_stop && !tryStartup())
					wakeup_event.tryWait(retry_delay_ms);

				if (need_stop)
					break;

				storage.is_read_only = false;
			}

			wakeup_event.tryWait(check_delay_ms);
		}
	}
	catch (...)
	{
		tryLogCurrentException("StorageReplicatedMergeTree::restartingThread");
		LOG_ERROR(log, "Unexpected exception in restartingThread. The storage will be read-only until server restart.");
		goReadOnlyPermanently();
		LOG_DEBUG(log, "Restarting thread finished");
		return;
	}

	try
	{
		storage.endpoint_holder = nullptr;
		partialShutdown();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	LOG_DEBUG(log, "Restarting thread finished");
}


bool ReplicatedMergeTreeRestartingThread::tryStartup()
{
	try
	{
		activateReplica();

		storage.leader_election = new zkutil::LeaderElection(storage.zookeeper_path + "/leader_election", *storage.zookeeper,
			std::bind(&StorageReplicatedMergeTree::becomeLeader, &storage), storage.replica_name);

		/// Все, что выше, может бросить KeeperException, если что-то не так с ZK.
		/// Все, что ниже, не должно бросать исключений.

		storage.shutdown_called = false;
		storage.shutdown_event.reset();

		storage.merger.uncancelAll();
		if (storage.unreplicated_merger)
			storage.unreplicated_merger->uncancelAll();

		storage.queue_updating_thread = std::thread(&StorageReplicatedMergeTree::queueUpdatingThread, &storage);
		storage.cleanup_thread.reset(new ReplicatedMergeTreeCleanupThread(storage));
		storage.alter_thread = std::thread(&StorageReplicatedMergeTree::alterThread, &storage);
		storage.part_check_thread = std::thread(&StorageReplicatedMergeTree::partCheckThread, &storage);
		storage.queue_task_handle = storage.context.getBackgroundPool().addTask(
			std::bind(&StorageReplicatedMergeTree::queueTask, &storage, std::placeholders::_1));
		storage.queue_task_handle->wake();
		return true;
	}
	catch (const zkutil::KeeperException & e)
	{
		storage.replica_is_active_node = nullptr;
		storage.leader_election = nullptr;
		LOG_ERROR(log, "Couldn't start replication: " << e.what() << ", " << e.displayText() << ", stack trace:\n"
			<< e.getStackTrace().toString());
		return false;
	}
	catch (const Exception & e)
	{
		if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
			throw;

		storage.replica_is_active_node = nullptr;
		storage.leader_election = nullptr;
		LOG_ERROR(log, "Couldn't start replication: " << e.what() << ", " << e.displayText() << ", stack trace:\n"
			<< e.getStackTrace().toString());
		return false;
	}
	catch (...)
	{
		storage.replica_is_active_node = nullptr;
		storage.leader_election = nullptr;
		throw;
	}
}


void ReplicatedMergeTreeRestartingThread::activateReplica()
{
	auto host_port = storage.context.getInterserverIOAddress();

	std::string address;
	{
		WriteBufferFromString address_buf(address);
		address_buf
			<< "host: " << host_port.first << '\n'
			<< "port: " << host_port.second << '\n';
	}

	/** Если нода отмечена как активная, но отметка сделана в этом же экземпляре, удалим ее.
	  * Такое возможно только при истечении сессии в ZooKeeper.
	  * Здесь есть небольшой race condition (можем удалить не ту ноду, для которой сделали tryGet),
	  *  но он крайне маловероятен при нормальном использовании.
	  */
	String data;
	if (storage.zookeeper->tryGet(storage.replica_path + "/is_active", data) && data == active_node_identifier)
		storage.zookeeper->tryRemove(storage.replica_path + "/is_active");

	/// Одновременно объявим, что эта реплика активна, и обновим хост.
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(storage.replica_path + "/is_active",
		active_node_identifier, storage.zookeeper->getDefaultACL(), zkutil::CreateMode::Ephemeral));
	ops.push_back(new zkutil::Op::SetData(storage.replica_path + "/host", address, -1));

	try
	{
		storage.zookeeper->multi(ops);
	}
	catch (const zkutil::KeeperException & e)
	{
		if (e.code == ZNODEEXISTS)
			throw Exception("Replica " + storage.replica_path + " appears to be already active. If you're sure it's not, "
				"try again in a minute or remove znode " + storage.replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

		throw;
	}

	storage.replica_is_active_node = zkutil::EphemeralNodeHolder::existing(storage.replica_path + "/is_active", *storage.zookeeper);
}


void ReplicatedMergeTreeRestartingThread::partialShutdown()
{
	storage.leader_election = nullptr;
	storage.shutdown_called = true;
	storage.shutdown_event.set();
	storage.merge_selecting_event.set();
	storage.queue_updating_event->set();
	storage.alter_thread_event->set();
	storage.alter_query_event->set();
	storage.parts_to_check_event.set();
	storage.replica_is_active_node = nullptr;

	storage.merger.cancelAll();
	if (storage.unreplicated_merger)
		storage.unreplicated_merger->cancelAll();

	LOG_TRACE(log, "Waiting for threads to finish");
	if (storage.is_leader_node)
	{
		storage.is_leader_node = false;
		if (storage.merge_selecting_thread.joinable())
			storage.merge_selecting_thread.join();
	}
	if (storage.queue_updating_thread.joinable())
		storage.queue_updating_thread.join();

	storage.cleanup_thread.reset();

	if (storage.alter_thread.joinable())
		storage.alter_thread.join();
	if (storage.part_check_thread.joinable())
		storage.part_check_thread.join();
	if (storage.queue_task_handle)
		storage.context.getBackgroundPool().removeTask(storage.queue_task_handle);
	storage.queue_task_handle.reset();
	LOG_TRACE(log, "Threads finished");
}


void ReplicatedMergeTreeRestartingThread::goReadOnlyPermanently()
{
	LOG_INFO(log, "Going to read-only mode");

	storage.is_read_only = true;
	stop();

	partialShutdown();
}


}
