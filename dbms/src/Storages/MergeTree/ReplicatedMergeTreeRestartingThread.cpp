#include <DB/IO/Operators.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/randomSeed.h>


namespace ProfileEvents
{
	extern const Event ReplicaYieldLeadership;
	extern const Event ReplicaPartialShutdown;
}

namespace CurrentMetrics
{
	extern const Metric ReadonlyReplica;
	extern const Metric LeaderReplica;
}


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
	Int64 check_period_ms = storage.data.settings.zookeeper_session_expiration_check_period * 1000;

	/// Периодичность проверки величины отставания реплики.
	if (check_period_ms > static_cast<Int64>(storage.data.settings.check_delay_period) * 1000)
		check_period_ms = storage.data.settings.check_delay_period * 1000;

	setThreadName("ReplMTRestart");

	bool first_time = true;					/// Активация реплики в первый раз.
	time_t prev_time_of_check_delay = 0;

	/// Запуск реплики при старте сервера/создании таблицы. Перезапуск реплики при истечении сессии с ZK.
	while (!need_stop)
	{
		try
		{
			if (first_time || storage.getZooKeeper()->expired())
			{
				if (first_time)
				{
					LOG_DEBUG(log, "Activating replica.");
				}
				else
				{
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
			}

			time_t current_time = time(0);
			if (current_time >= prev_time_of_check_delay + static_cast<time_t>(storage.data.settings.check_delay_period))
			{
				/// Выясняем отставания реплик.
				time_t absolute_delay = 0;
				time_t relative_delay = 0;

				storage.getReplicaDelays(absolute_delay, relative_delay);

				if (absolute_delay)
					LOG_TRACE(log, "Absolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".");

				prev_time_of_check_delay = current_time;

				/// Уступаем лидерство, если относительное отставание больше порога.
				if (storage.is_leader_node
					&& relative_delay > static_cast<time_t>(storage.data.settings.min_relative_delay_to_yield_leadership))
				{
					LOG_INFO(log, "Relative replica delay (" << relative_delay << " seconds) is bigger than threshold ("
						<< storage.data.settings.min_relative_delay_to_yield_leadership << "). Will yield leadership.");

					ProfileEvents::increment(ProfileEvents::ReplicaYieldLeadership);

					if (storage.is_leader_node)
					{
						storage.is_leader_node = false;
						CurrentMetrics::sub(CurrentMetrics::LeaderReplica);
						if (storage.merge_selecting_thread.joinable())
							storage.merge_selecting_thread.join();

						storage.leader_election->yield();
					}
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		wakeup_event.tryWait(check_period_ms);
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

		if (storage.queue_task_handle)
			storage.context.getBackgroundPool().removeTask(storage.queue_task_handle);
		storage.queue_task_handle.reset();
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
			[this] { storage.becomeLeader(); CurrentMetrics::add(CurrentMetrics::LeaderReplica); },
			storage.replica_name);

		/// Все, что выше, может бросить KeeperException, если что-то не так с ZK.
		/// Все, что ниже, не должно бросать исключений.

		storage.shutdown_called = false;
		storage.shutdown_event.reset();

		storage.queue_updating_thread = std::thread(&StorageReplicatedMergeTree::queueUpdatingThread, &storage);
		storage.alter_thread = std::make_unique<ReplicatedMergeTreeAlterThread>(storage);
		storage.cleanup_thread = std::make_unique<ReplicatedMergeTreeCleanupThread>(storage);
		storage.part_check_thread.start();

		if (!storage.queue_task_handle)
			storage.queue_task_handle = storage.context.getBackgroundPool().addTask(
				std::bind(&StorageReplicatedMergeTree::queueTask, &storage));

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
	ops.emplace_back(std::make_unique<zkutil::Op::Create>(is_active_path,
		active_node_identifier, zookeeper->getDefaultACL(), zkutil::CreateMode::Ephemeral));
	ops.emplace_back(std::make_unique<zkutil::Op::SetData>(storage.replica_path + "/host", address.toString(), -1));

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
	{
		std::lock_guard<std::mutex> lock(storage.leader_node_mutex);

		if (storage.is_leader_node)
		{
			storage.is_leader_node = false;
			CurrentMetrics::sub(CurrentMetrics::LeaderReplica);
			if (storage.merge_selecting_thread.joinable())
				storage.merge_selecting_thread.join();
		}
	}
	if (storage.queue_updating_thread.joinable())
		storage.queue_updating_thread.join();

	storage.cleanup_thread.reset();
	storage.alter_thread.reset();
	storage.part_check_thread.stop();

	/// Yielding leadership only after finish of merge_selecting_thread.
	/// Otherwise race condition with parallel run of merge selecting thread on different servers is possible.
	///
	/// On the other hand, leader_election could call becomeLeader() from own thread after
	/// merge_selecting_thread is finished and restarting_thread is destroyed.
	/// becomeLeader() recreates merge_selecting_thread and it becomes joinable again, even restarting_thread is destroyed.
	/// But restarting_thread is responsible to stop merge_selecting_thread.
	/// It will lead to std::terminate in ~StorageReplicatedMergeTree().
	/// Such behaviour was rarely observed on DROP queries.
	/// Therefore we need either avoid becoming leader after first shutdown call (more deliberate choice),
	/// either manually wait merge_selecting_thread.join() inside ~StorageReplicatedMergeTree(), either or something third.
	/// So, we added shutdown check in becomeLeader() and made its creation and deletion atomic.
	storage.leader_election = nullptr;

	LOG_TRACE(log, "Threads finished");
}

}
