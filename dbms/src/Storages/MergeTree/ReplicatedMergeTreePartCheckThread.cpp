#include <DB/Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/setThreadName.h>


namespace DB
{

static const auto PART_CHECK_ERROR_SLEEP_MS = 5 * 1000;


ReplicatedMergeTreePartCheckThread::ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_)
	: storage(storage_),
	log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, PartCheckThread)"))
{
}


void ReplicatedMergeTreePartCheckThread::start()
{
	need_stop = false;
	thread = std::thread([this] { run(); });
}


void ReplicatedMergeTreePartCheckThread::stop()
{
	need_stop = true;
	wakeup_event.set();

	if (thread.joinable())
		thread.join();
}


void ReplicatedMergeTreePartCheckThread::enqueuePart(const String & name, time_t delay_to_check_seconds)
{
	std::lock_guard<std::mutex> lock(mutex);

	if (parts_set.count(name))
		return;

	parts_queue.emplace_back(name, time(0) + delay_to_check_seconds);
	parts_set.insert(name);
	wakeup_event.set();
}


size_t ReplicatedMergeTreePartCheckThread::size() const
{
	std::lock_guard<std::mutex> lock(mutex);
	return parts_set.size();
}


void ReplicatedMergeTreePartCheckThread::searchForMissingPart(const String & part_name)
{
	auto zookeeper = storage.getZooKeeper();
	String part_path = storage.replica_path + "/parts/" + part_name;

	/// Если кусок есть в ZooKeeper, удалим его оттуда и добавим в очередь задание скачать его.
	if (zookeeper->exists(part_path))
	{
		LOG_WARNING(log, "Part " << part_name << " exists in ZooKeeper but not locally. "
			"Removing from ZooKeeper and queueing a fetch.");
		ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

		storage.removePartAndEnqueueFetch(part_name);
		return;
	}

	/// Если куска нет в ZooKeeper, проверим есть ли он хоть у кого-то.
	ActiveDataPartSet::Part part_info;
	ActiveDataPartSet::parsePartName(part_name, part_info);

	/** Логика такая:
		* - если у какой-то живой или неактивной реплики есть такой кусок, или покрывающий его кусок
		*   - всё Ок, ничего делать не нужно, он скачается затем при обработке очереди, когда реплика оживёт;
		*   - или, если реплика никогда не оживёт, то администратор удалит или создаст новую реплику с тем же адресом и см. всё сначала;
		* - если ни у кого нет такого или покрывающего его куска, то
		*   - если у кого-то есть все составляющие куски, то ничего делать не будем - это просто значит, что другие реплики ещё недоделали мердж
		*   - если ни у кого нет всех составляющих кусков, то признаем кусок навечно потерянным,
		*     и удалим запись из очереди репликации.
		*/

	LOG_WARNING(log, "Checking if anyone has part covering " << part_name << ".");

	bool found = false;

	size_t part_length_in_blocks = part_info.right + 1 - part_info.left;
	std::vector<char> found_blocks(part_length_in_blocks);

	Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");
	for (const String & replica : replicas)
	{
		Strings parts = zookeeper->getChildren(storage.zookeeper_path + "/replicas/" + replica + "/parts");
		for (const String & part_on_replica : parts)
		{
			if (part_on_replica == part_name || ActiveDataPartSet::contains(part_on_replica, part_name))
			{
				found = true;
				LOG_WARNING(log, "Found part " << part_on_replica << " on " << replica);
				break;
			}

			if (ActiveDataPartSet::contains(part_name, part_on_replica))
			{
				ActiveDataPartSet::Part part_on_replica_info;
				ActiveDataPartSet::parsePartName(part_on_replica, part_on_replica_info);

				for (auto block_num = part_on_replica_info.left; block_num <= part_on_replica_info.right; ++block_num)
					found_blocks.at(block_num - part_info.left) = 1;
			}
		}
		if (found)
			break;
	}

	if (found)
	{
		/// На какой-то живой или мёртвой реплике есть нужный кусок или покрывающий его.
		return;
	}

	size_t num_found_blocks = 0;
	for (auto found_block : found_blocks)
		num_found_blocks += (found_block == 1);

	if (num_found_blocks == part_length_in_blocks)
	{
		/// На совокупности живых или мёртвых реплик есть все куски, из которых можно составить нужный кусок. Ничего делать не будем.
		LOG_WARNING(log, "Found all blocks for missing part " << part_name << ". Will wait for them to be merged.");
		return;
	}

	/// Ни у кого нет такого куска.
	LOG_ERROR(log, "No replica has part covering " << part_name);

	if (num_found_blocks != 0)
		LOG_WARNING(log, "When looking for smaller parts, that is covered by " << part_name
			<< ", we found just " << num_found_blocks << " of " << part_length_in_blocks << " blocks.");

	ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

	/// Есть ли он в очереди репликации? Если есть - удалим, так как задачу невозможно обработать.
	if (!storage.queue.remove(zookeeper, part_name))
	{
		/// Куска не было в нашей очереди. С чего бы это?
		LOG_ERROR(log, "Missing part " << part_name << " is not in our queue.");
		return;
	}

	/** Такая ситуация возможна, если на всех репликах, где был кусок, он испортился.
		* Например, у реплики, которая только что его записала, отключили питание, и данные не записались из кеша на диск.
		*/
	LOG_ERROR(log, "Part " << part_name << " is lost forever.");
	ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);

	/** Нужно добавить отсутствующий кусок в block_numbers, чтобы он не мешал слияниям.
		* Вот только в сам block_numbers мы его добавить не можем - если так сделать,
		*  ZooKeeper зачем-то пропустит один номер для автоинкремента,
		*  и в номерах блоков все равно останется дырка.
		* Специально из-за этого приходится отдельно иметь nonincrement_block_numbers.
		*
		* Кстати, если мы здесь сдохнем, то слияния не будут делаться сквозь эти отсутствующие куски.
		*
		* А ещё, не будем добавлять, если:
		* - потребовалось бы создать слишком много (больше 1000) узлов;
		* - кусок является первым в партиции или был при-ATTACH-ен.
		* NOTE Возможно, добавить также условие, если запись в очереди очень старая.
		*/

	if (part_length_in_blocks > 1000)
	{
		LOG_ERROR(log, "Won't add nonincrement_block_numbers because part spans too much blocks (" << part_length_in_blocks << ")");
		return;
	}

	if (part_info.left <= RESERVED_BLOCK_NUMBERS)
	{
		LOG_ERROR(log, "Won't add nonincrement_block_numbers because part is one of first in partition");
		return;
	}

	const auto partition_str = part_name.substr(0, 6);
	for (auto i = part_info.left; i <= part_info.right; ++i)
	{
		zookeeper->createIfNotExists(storage.zookeeper_path + "/nonincrement_block_numbers/" + partition_str, "");
		AbandonableLockInZooKeeper::createAbandonedIfNotExists(
			storage.zookeeper_path + "/nonincrement_block_numbers/" + partition_str + "/block-" + padIndex(i),
			*zookeeper);
	}
}


void ReplicatedMergeTreePartCheckThread::checkPart(const String & part_name)
{
	LOG_WARNING(log, "Checking part " << part_name);
	ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

	auto part = storage.data.getActiveContainingPart(part_name);

	/// Этого или покрывающего куска у нас нет.
	if (!part)
	{
		searchForMissingPart(part_name);
	}
	/// У нас есть этот кусок, и он активен. Будем проверять, нужен ли нам этот кусок и правильные ли у него данные.
	else if (part->name == part_name)
	{
		auto zookeeper = storage.getZooKeeper();
		auto table_lock = storage.lockStructure(false);

		/// Если кусок есть в ZooKeeper, сверим его данные с его чексуммами, а их с ZooKeeper.
		if (zookeeper->exists(storage.replica_path + "/parts/" + part_name))
		{
			LOG_WARNING(log, "Checking data of part " << part_name << ".");

			try
			{
				auto zk_checksums = MergeTreeData::DataPart::Checksums::parse(
					zookeeper->get(storage.replica_path + "/parts/" + part_name + "/checksums"));
				zk_checksums.checkEqual(part->checksums, true);

				auto zk_columns = NamesAndTypesList::parse(
					zookeeper->get(storage.replica_path + "/parts/" + part_name + "/columns"));
				if (part->columns != zk_columns)
					throw Exception("Columns of local part " + part_name + " are different from ZooKeeper");

				MergeTreePartChecker::Settings settings;
				settings.setIndexGranularity(storage.data.index_granularity);
				settings.setRequireChecksums(true);
				settings.setRequireColumnFiles(true);

				MergeTreePartChecker::checkDataPart(
					storage.data.getFullPath() + part_name, settings, storage.data.primary_key_data_types, nullptr, &need_stop);

				if (need_stop)
				{
					LOG_INFO(log, "Checking part was cancelled.");
					return;
				}

				LOG_INFO(log, "Part " << part_name << " looks good.");
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);

				LOG_ERROR(log, "Part " << part_name << " looks broken. Removing it and queueing a fetch.");
				ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

				storage.removePartAndEnqueueFetch(part_name);

				/// Удалим кусок локально.
				storage.data.renameAndDetachPart(part, "broken_");
			}
		}
		else if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(0))
		{
			/// Если куска нет в ZooKeeper, удалим его локально.
			/// Возможно, кусок кто-то только что записал, и еще не успел добавить в ZK.
			/// Поэтому удаляем только если кусок старый (не очень надежно).
			ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

			LOG_ERROR(log, "Unexpected part " << part_name << " in filesystem. Removing.");
			storage.data.renameAndDetachPart(part, "unexpected_");
		}
		else
		{
			/// TODO Надо сделать так, чтобы кусок всё-таки проверился через некоторое время.
			/// Иначе возможна ситуация, что кусок не добавился в ZK,
			///  но остался в файловой системе и в множестве активных кусков.
			/// И тогда в течение долгого времени (до перезапуска), данные на репликах будут разными.

			LOG_TRACE(log, "Young part " << part_name
				<< " with age " << (time(0) - part->modification_time)
				<< " seconds hasn't been added to ZooKeeper yet. It's ok.");
		}
	}
	else
	{
		/// Если у нас есть покрывающий кусок, игнорируем все проблемы с этим куском.
		/// В худшем случае в лог еще old_parts_lifetime секунд будут валиться ошибки, пока кусок не удалится как старый.
		LOG_WARNING(log, "We have part " << part->name << " covering part " << part_name);
	}
}


void ReplicatedMergeTreePartCheckThread::run()
{
	setThreadName("ReplMTPartCheck");

	while (!need_stop)
	{
		try
		{
			time_t current_time = time(0);

			/// Достанем из очереди кусок для проверки.
			PartsToCheckQueue::iterator selected = parts_queue.end();	/// end у std::list не инвалидируется
			time_t min_check_time = std::numeric_limits<time_t>::max();

			{
				std::lock_guard<std::mutex> lock(mutex);

				if (parts_queue.empty())
				{
					if (!parts_set.empty())
					{
						LOG_ERROR(log, "Non-empty parts_set with empty parts_queue. This is a bug.");
						parts_set.clear();
					}
				}
				else
				{
					for (auto it = parts_queue.begin(); it != parts_queue.end(); ++it)
					{
						if (it->second <= current_time)
						{
							selected = it;
							break;
						}

						if (it->second < min_check_time)
							min_check_time = it->second;
					}
				}
			}

			if (selected == parts_queue.end())
			{
				/// Poco::Event срабатывает сразу, если signal был до вызова wait.
				/// Можем подождать чуть больше, чем нужно из-за использования старого current_time.

				if (min_check_time != std::numeric_limits<time_t>::max() && min_check_time > current_time)
					wakeup_event.tryWait(1000 * (min_check_time - current_time));
				else
					wakeup_event.wait();

				continue;
			}

			checkPart(selected->first);

			if (need_stop)
				break;

			/// Удалим кусок из очереди проверок.
			{
				std::lock_guard<std::mutex> lock(mutex);

				if (parts_queue.empty())
				{
					LOG_ERROR(log, "Someone erased cheking part from parts_queue. This is a bug.");
				}
				else
				{
					parts_set.erase(selected->first);
					parts_queue.erase(selected);
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
			wakeup_event.tryWait(PART_CHECK_ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "Part check thread finished");
}

}
