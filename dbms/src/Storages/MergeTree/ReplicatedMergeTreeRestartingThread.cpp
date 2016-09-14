#include <DB/IO/Operators.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int REPLICA_IS_ALREADY_ACTIVE;
}


/// Используется для проверки, выставили ли ноду is_active мы, или нет.
static String generateActiveNodeIdentifier()
{
	return "pid: " + toString(getpid()) + ", random: " + toString(randomSeed());
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
	constexpr auto retry_period_ms = 10 * 1000;

	/// Периодичность проверки истечения сессии в ZK.
	time_t check_period_ms = 60 * 1000;

	/// Периодичность проверки величины отставания реплики.
	if (check_period_ms > static_cast<time_t>(storage.data.settings.check_delay_period) * 1000)
		check_period_ms = storage.data.settings.check_delay_period * 1000;

	setThreadName("ReplMTRestart");

	try
	{
		bool first_time = true;					/// Активация реплики в первый раз.
		bool need_restart = false;				/// Перезапуск по собственной инициативе, чтобы отдать лидерство.
		time_t prev_time_of_check_delay = 0;

		/// Запуск реплики при старте сервера/создании таблицы. Перезапуск реплики при истечении сессии с ZK.
		while (!need_stop)
		{
			if (first_time || need_restart || storage.getZooKeeper()->expired())
			{
				if (first_time)
				{
					LOG_DEBUG(log, "Activating replica.");
				}
				else
				{
					if (need_restart)
						LOG_WARNING(log, "Will reactivate replica.");
					else
						LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");

					if (!storage.is_readonly)
						CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
					storage.is_readonly = true;
					partialShutdown();
				}

				while (true)
				{
					try
					{
						storage.setZooKeeper(storage.context.getZooKeeper());
					}
					catch (const zkutil::KeeperException & e)
					{
						/// Исключение при попытке zookeeper_init обычно бывает, если не работает DNS. Будем пытаться сделать это заново.
						tryLogCurrentException(__PRETTY_FUNCTION__);

						wakeup_event.tryWait(retry_period_ms);
						continue;
					}

					if (!need_stop && !tryStartup())
					{
						wakeup_event.tryWait(retry_period_ms);
						continue;
					}

					break;
				}

				if (storage.is_readonly)
					CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
				storage.is_readonly = false;
				first_time = false;
				need_restart = false;
			}

			time_t current_time = time(0);
			if (current_time >= prev_time_of_check_delay + static_cast<time_t>(storage.data.settings.check_delay_period))
			{
				/// Выясняем отставания реплик.
				time_t absolute_delay = 0;
				time_t relative_delay = 0;

				bool error = false;
				try
				{
					storage.getReplicaDelays(absolute_delay, relative_delay);
					LOG_TRACE(log, "Absolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".");
				}
				catch (...)
				{
					tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot get replica delays");
					error = true;
				}

				prev_time_of_check_delay = current_time;

				/// Уступаем лидерство, если относительное отставание больше порога.
				if (storage.is_leader_node
					&& (error || relative_delay > static_cast<time_t>(storage.data.settings.min_relative_delay_to_yield_leadership)))
				{
					if (error)
						LOG_INFO(log, "Will yield leadership.");
					else
						LOG_INFO(log, "Relative replica delay (" << relative_delay << " seconds) is bigger than threshold ("
							<< storage.data.settings.min_relative_delay_to_yield_leadership << "). Will yield leadership.");

					ProfileEvents::increment(ProfileEvents::ReplicaYieldLeadership);

					need_restart = true;
					continue;
				}
			}

			wakeup_event.tryWait(check_period_ms);
		}
	}
	catch (...)
	{
		tryLogCurrentException("StorageReplicatedMergeTree::restartingThread");
		LOG_ERROR(log, "Unexpected exception in restartingThread. The storage will be readonly until server restart.");
		goReadOnlyPermanently();
		LOG_DEBUG(log, "Restarting thread finished");
		return;
	}

	try
	{
		storage.endpoint_holder->cancel();
		storage.endpoint_holder = nullptr;

		storage.disk_space_monitor_endpoint_holder->cancel();
		storage.disk_space_monitor_endpoint_holder = nullptr;

		storage.sharded_partition_uploader_endpoint_holder->cancel();
		storage.sharded_partition_uploader_endpoint_holder = nullptr;

		storage.remote_query_executor_endpoint_holder->cancel();
		storage.remote_query_executor_endpoint_holder = nullptr;

		storage.remote_part_checker_endpoint_holder->cancel();
		storage.remote_part_checker_endpoint_holder = nullptr;

		storage.merger.cancelForever();
		if (storage.unreplicated_merger)
			storage.unreplicated_merger->cancelForever();

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
		removeFailedQuorumParts();
		activateReplica();
		updateQuorumIfWeHavePart();

		storage.leader_election = std::make_shared<zkutil::LeaderElection>(
			storage.zookeeper_path + "/leader_election",
			*storage.current_zookeeper,		/// current_zookeeper живёт в течение времени жизни leader_election,
											///  так как до изменения current_zookeeper, объект leader_election уничтожается в методе partialShutdown.
			[this] { storage.becomeLeader(); },
			storage.replica_name);

		/// Все, что выше, может бросить KeeperException, если что-то не так с ZK.
		/// Все, что ниже, не должно бросать исключений.

		storage.shutdown_called = false;
		storage.shutdown_event.reset();

		storage.queue_updating_thread = std::thread(&StorageReplicatedMergeTree::queueUpdatingThread, &storage);
		storage.alter_thread.reset(new ReplicatedMergeTreeAlterThread(storage));
		storage.cleanup_thread.reset(new ReplicatedMergeTreeCleanupThread(storage));
		storage.part_check_thread.start();
		storage.queue_task_handle = storage.context.getBackgroundPool().addTask(
			std::bind(&StorageReplicatedMergeTree::queueTask, &storage, std::placeholders::_1));
		storage.queue_task_handle->wake();

		return true;
	}
	catch (...)
	{
		storage.replica_is_active_node 	= nullptr;
		storage.leader_election 		= nullptr;

		try
		{
			throw;
		}
		catch (const zkutil::KeeperException & e)
		{
			LOG_ERROR(log, "Couldn't start replication: " << e.what() << ", " << e.displayText() << ", stack trace:\n" << e.getStackTrace().toString());
			return false;
		}
		catch (const Exception & e)
		{
			if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
				throw;

			LOG_ERROR(log, "Couldn't start replication: " << e.what() << ", " << e.displayText() << ", stack trace:\n" << e.getStackTrace().toString());
			return false;
		}
	}
}


void ReplicatedMergeTreeRestartingThread::removeFailedQuorumParts()
{
	auto zookeeper = storage.getZooKeeper();

	Strings failed_parts;
	if (!zookeeper->tryGetChildren(storage.zookeeper_path + "/quorum/failed_parts", failed_parts))
		return;

	for (auto part_name : failed_parts)
	{
		auto part = storage.data.getPartIfExists(part_name);
		if (part)
		{
			LOG_DEBUG(log, "Found part " << part_name << " with failed quorum. Moving to detached. This shouldn't happen often.");

			zkutil::Ops ops;
			storage.removePartFromZooKeeper(part_name, ops);
			auto code = zookeeper->tryMulti(ops);
			if (code == ZNONODE)
				LOG_WARNING(log, "Part " << part_name << " with failed quorum is not in ZooKeeper. This shouldn't happen often.");

			storage.data.renameAndDetachPart(part, "noquorum");
		}
	}
}


void ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart()
{
	auto zookeeper = storage.getZooKeeper();

	String quorum_str;
	if (zookeeper->tryGet(storage.zookeeper_path + "/quorum/status", quorum_str))
	{
		ReplicatedMergeTreeQuorumEntry quorum_entry;
		quorum_entry.fromString(quorum_str);

		if (!quorum_entry.replicas.count(storage.replica_name)
			&& zookeeper->exists(storage.replica_path + "/parts/" + quorum_entry.part_name))
		{
			LOG_WARNING(log, "We have part " << quorum_entry.part_name
				<< " but we is not in quorum. Updating quorum. This shouldn't happen often.");
			storage.updateQuorum(quorum_entry.part_name);
		}
	}
}


void ReplicatedMergeTreeRestartingThread::activateReplica()
{
	auto host_port = storage.context.getInterserverIOAddress();
	auto zookeeper = storage.getZooKeeper();

	/// Как другие реплики могут обращаться к данной.
	ReplicatedMergeTreeAddress address;
	address.host = host_port.first;
	address.replication_port = host_port.second;
	address.queries_port = storage.context.getTCPPort();
	address.database = storage.database_name;
	address.table = storage.table_name;

	String is_active_path = storage.replica_path + "/is_active";

	/** Если нода отмечена как активная, но отметка сделана в этом же экземпляре, удалим ее.
	  * Такое возможно только при истечении сессии в ZooKeeper.
	  */
	String data;
	Stat stat;
	bool has_is_active = zookeeper->tryGet(is_active_path, data, &stat);
	if (has_is_active && data == active_node_identifier)
	{
		auto code = zookeeper->tryRemove(is_active_path, stat.version);

		if (code == ZBADVERSION)
			throw Exception("Another instance of replica " + storage.replica_path + " was created just now."
				" You shouldn't run multiple instances of same replica. You need to check configuration files.",
				ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

		if (code != ZOK && code != ZNONODE)
			throw zkutil::KeeperException(code, is_active_path);
	}

	/// Одновременно объявим, что эта реплика активна, и обновим хост.
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(is_active_path,
		active_node_identifier, zookeeper->getDefaultACL(), zkutil::CreateMode::Ephemeral));
	ops.push_back(new zkutil::Op::SetData(storage.replica_path + "/host", address.toString(), -1));

	try
	{
		zookeeper->multi(ops);
	}
	catch (const zkutil::KeeperException & e)
	{
		if (e.code == ZNODEEXISTS)
			throw Exception("Replica " + storage.replica_path + " appears to be already active. If you're sure it's not, "
				"try again in a minute or remove znode " + storage.replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

		throw;
	}

	/// current_zookeeper живёт в течение времени жизни replica_is_active_node,
	///  так как до изменения current_zookeeper, объект replica_is_active_node уничтожается в методе partialShutdown.
	storage.replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *storage.current_zookeeper);
}


void ReplicatedMergeTreeRestartingThread::partialShutdown()
{
	ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

	storage.shutdown_called = true;
	storage.shutdown_event.set();
	storage.merge_selecting_event.set();
	storage.queue_updating_event->set();
	storage.alter_query_event->set();
	storage.replica_is_active_node = nullptr;

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
	storage.alter_thread.reset();
	storage.part_check_thread.stop();

	if (storage.queue_task_handle)
		storage.context.getBackgroundPool().removeTask(storage.queue_task_handle);
	storage.queue_task_handle.reset();

	/// Yielding leadership only after finish of merge_selecting_thread.
	/// Otherwise race condition with parallel run of merge selecting thread on different servers is possible.
	storage.leader_election = nullptr;

	LOG_TRACE(log, "Threads finished");
}


void ReplicatedMergeTreeRestartingThread::goReadOnlyPermanently()
{
	LOG_INFO(log, "Going to readonly mode");
	ProfileEvents::increment(ProfileEvents::ReplicaPermanentlyReadonly);

	if (!storage.is_readonly)
		CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
	storage.is_readonly = true;
	stop();

	partialShutdown();
}


}
