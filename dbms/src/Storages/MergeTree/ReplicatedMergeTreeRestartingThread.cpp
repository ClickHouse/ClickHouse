#include <DB/IO/Operators.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Common/setThreadName.h>


namespace DB
{


/// Используется для проверки, выставили ли ноду is_active мы, или нет.
static String generateActiveNodeIdentifier()
{
	struct timespec times;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
		throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);
	return "pid: " + toString(getpid()) + ", random: " + toString(times.tv_nsec + times.tv_sec + getpid());
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

				storage.is_readonly = false;
				first_time = false;
				need_restart = false;
			}

			time_t current_time = time(0);
			if (current_time >= prev_time_of_check_delay + static_cast<time_t>(storage.data.settings.check_delay_period))
			{
				/// Выясняем отставания реплик.
				time_t new_absolute_delay = 0;
				time_t new_relative_delay = 0;

				checkReplicationDelays(new_absolute_delay, new_relative_delay);

				absolute_delay.store(new_absolute_delay, std::memory_order_relaxed);
				relative_delay.store(new_relative_delay, std::memory_order_relaxed);

				prev_time_of_check_delay = current_time;

				/// Уступаем лидерство, если относительное отставание больше порога.
				if (storage.is_leader_node && new_relative_delay > static_cast<time_t>(storage.data.settings.min_relative_delay_to_yield_leadership))
				{
					LOG_INFO(log, "Relative replica delay (" << new_relative_delay << " seconds) is bigger than threshold ("
						<< storage.data.settings.min_relative_delay_to_yield_leadership << "). Will yield leadership.");

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
		removeFailedQuorumParts();
		activateReplica();
		updateQuorumIfWeHavePart();

		storage.leader_election = new zkutil::LeaderElection(
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
		storage.cleanup_thread.reset(new ReplicatedMergeTreeCleanupThread(storage));
		storage.alter_thread = std::thread(&StorageReplicatedMergeTree::alterThread, &storage);
		storage.part_check_thread = std::thread(&StorageReplicatedMergeTree::partCheckThread, &storage);
		storage.queue_task_handle = storage.context.getBackgroundPool().addTask(
			std::bind(&StorageReplicatedMergeTree::queueTask, &storage, std::placeholders::_1));
		storage.queue_task_handle->wake();

		storage.merger.uncancel();
		if (storage.unreplicated_merger)
			storage.unreplicated_merger->uncancel();

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
	storage.leader_election = nullptr;
	storage.shutdown_called = true;
	storage.shutdown_event.set();
	storage.merge_selecting_event.set();
	storage.queue_updating_event->set();
	storage.alter_thread_event->set();
	storage.alter_query_event->set();
	storage.parts_to_check_event.set();
	storage.replica_is_active_node = nullptr;

	storage.merger.cancel();
	if (storage.unreplicated_merger)
		storage.unreplicated_merger->cancel();

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
	LOG_INFO(log, "Going to readonly mode");

	storage.is_readonly = true;
	stop();

	partialShutdown();
}


static time_t extractTimeOfLogEntryIfGetPart(zkutil::ZooKeeperPtr & zookeeper, const String & name, const String & path)
{
	String content;
	zkutil::Stat stat;
	if (!zookeeper->tryGet(path + "/" + name, content, &stat))
		return 0;	/// Узел уже успел удалиться.

	ReplicatedMergeTreeLogEntry entry;
	entry.parse(content, stat);

	if (entry.type == ReplicatedMergeTreeLogEntry::GET_PART)
		return entry.create_time;

	return 0;
}

/// В массиве имён узлов - элементов очереди/лога находит первый, имеющий тип GET_PART и возвращает его время; либо ноль, если не нашёл.
static time_t findFirstGetPartEntry(zkutil::ZooKeeperPtr & zookeeper, const Strings & nodes, const String & path)
{
	for (const auto & name : nodes)
	{
		time_t res = extractTimeOfLogEntryIfGetPart(zookeeper, name, path);
		if (res)
			return res;
	}

	return 0;
}


void ReplicatedMergeTreeRestartingThread::checkReplicationDelays(time_t & out_absolute_delay, time_t & out_relative_delay)
{
	/** Нужно получить следующую информацию:
	  * 1. Время последней записи типа GET в логе.
	  * 2. Время первой записи типа GET в очереди каждой активной реплики
	  *    (или в логе, после log_pointer реплики - то есть, среди записей, ещё не попавших в очередь реплики).
	  *
	  * Разница между этими величинами называется (абсолютным) отставанием реплик.
	  * Кроме абсолютного отставания также будем рассматривать относительное - от реплики с минимальным отставанием.
	  *
	  * Если относительное отставание текущей реплики больше некоторого порога,
	  *  и текущая реплика является лидером, то текущая реплика должна уступить лидерство.
	  *
	  * Также в случае превышения абсолютного либо относительного отставания некоторого порога, необходимо:
	  * - не отвечать Ok на некоторую ручку проверки реплик для балансировщика;
	  * - не принимать соединения для обработки запросов.
	  * Это делается в других местах.
	  *
	  * NOTE Реализация громоздкая, так как нужные значения вынимаются путём обхода списка узлов.
	  * Могут быть проблемы в случае разрастания лога до большого размера.
	  */

	out_absolute_delay = 0;
	out_relative_delay = 0;

	auto zookeeper = storage.getZooKeeper();

	/// Последняя запись GET в логе.
	String log_path = storage.zookeeper_path + "/log";
	Strings log_entries_desc = zookeeper->getChildren(log_path);
	std::sort(log_entries_desc.begin(), log_entries_desc.end(), std::greater<String>());
	time_t last_entry_to_get_part = findFirstGetPartEntry(zookeeper, log_entries_desc, log_path);

	/** Возможно, что в логе нет записей типа GET. Тогда считаем, что никто не отстаёт.
	  * В очередях у реплик могут быть не выполненные старые записи типа GET,
	  *  которые туда добавлены не из лога, а для восстановления битых кусков.
	  * Не будем считать это отставанием.
	  */

	if (!last_entry_to_get_part)
		return;

	/// Для каждой активной реплики время первой невыполненной записи типа GET, либо ноль, если таких записей нет.
	std::map<String, time_t> replicas_first_entry_to_get_part;

	Strings active_replicas = zookeeper->getChildren(storage.zookeeper_path + "/leader_election");
	for (const auto & node : active_replicas)
	{
		String replica;
		if (!zookeeper->tryGet(storage.zookeeper_path + "/leader_election/" + node, replica))
			continue;	/// Реплика только что перестала быть активной.

		String queue_path = storage.zookeeper_path + "/replicas/" + replica + "/queue";
		Strings queue_entries = zookeeper->getChildren(queue_path);
		std::sort(queue_entries.begin(), queue_entries.end());
		time_t & first_time = replicas_first_entry_to_get_part[replica];
		first_time = findFirstGetPartEntry(zookeeper, queue_entries, queue_path);

		if (!first_time)
		{
			/// Ищем среди записей лога после log_pointer для реплики.
			String log_pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");
			String log_min_entry = "log-" + storage.padIndex(parse<UInt64>(log_pointer));

			for (const auto & name : log_entries_desc)
			{
				if (name < log_min_entry)
					break;

				first_time = extractTimeOfLogEntryIfGetPart(zookeeper, name, log_path);
				if (first_time)
					break;
			}
		}
	}

	if (active_replicas.empty())
	{
		/// Нет активных реплик. Очень необычная ситуация - как же тогда у нас была сессия с ZK, чтобы это выяснить?
		/// Предполагаем, что всё же может быть потенциальный race condition при установке эфемерной ноды для leader election, а значит, это нормально.
		LOG_ERROR(log, "No active replicas when checking replication delays: very strange.");
		return;
	}

	time_t first_entry_of_most_recent_replica = -1;
	for (const auto & replica_time : replicas_first_entry_to_get_part)
	{
		if (0 == replica_time.second)
		{
			/// Есть реплика, которая совсем не отстаёт.
			first_entry_of_most_recent_replica = 0;
			break;
		}

		if (replica_time.second > first_entry_of_most_recent_replica)
			first_entry_of_most_recent_replica = replica_time.second;
	}

	time_t our_first_entry_to_get_part = replicas_first_entry_to_get_part[storage.replica_name];
	if (0 == our_first_entry_to_get_part)
		return;	/// Если мы совсем не отстаём.

	out_absolute_delay = last_entry_to_get_part - our_first_entry_to_get_part;
	out_relative_delay = first_entry_of_most_recent_replica - our_first_entry_to_get_part;

	LOG_TRACE(log, "Absolute delay: " << out_absolute_delay << ". Relative delay: " << out_relative_delay << ".");
}


}
