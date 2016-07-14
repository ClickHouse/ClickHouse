#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
}


void ReplicatedMergeTreeQueue::initVirtualParts(const MergeTreeData::DataParts & parts)
{
	std::lock_guard<std::mutex> lock(mutex);

	for (const auto & part : parts)
		virtual_parts.add(part->name);
}


void ReplicatedMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
	auto queue_path = replica_path + "/queue";
	LOG_DEBUG(log, "Loading queue from " << queue_path);

	std::lock_guard<std::mutex> lock(mutex);

	Strings children = zookeeper->getChildren(queue_path);
	LOG_DEBUG(log, "Having " << children.size() << " queue entries to load.");

	std::sort(children.begin(), children.end());

	std::vector<std::pair<String, zkutil::ZooKeeper::GetFuture>> futures;
	futures.reserve(children.size());

	for (const String & child : children)
		futures.emplace_back(child, zookeeper->asyncGet(queue_path + "/" + child));

	for (auto & future : futures)
	{
		zkutil::ZooKeeper::ValueAndStat res = future.second.get();
		LogEntryPtr entry = LogEntry::parse(res.value, res.stat);

		entry->znode_name = future.first;
		insertUnlocked(entry);
	}

	updateTimesInZooKeeper(zookeeper, true, false);

	LOG_TRACE(log, "Loaded queue");
}


void ReplicatedMergeTreeQueue::initialize(
	const String & zookeeper_path_, const String & replica_path_, const String & logger_name_,
	const MergeTreeData::DataParts & parts, zkutil::ZooKeeperPtr zookeeper)
{
	zookeeper_path = zookeeper_path_;
	replica_path = replica_path_;
	logger_name = logger_name_;
	log = &Logger::get(logger_name);

	initVirtualParts(parts);
	load(zookeeper);
}


void ReplicatedMergeTreeQueue::insertUnlocked(LogEntryPtr & entry)
{
	virtual_parts.add(entry->new_part_name);
	queue.push_back(entry);

	if (entry->type == LogEntry::GET_PART)
	{
		inserts_by_time.insert(entry);

		if (entry->create_time && (!min_unprocessed_insert_time || entry->create_time < min_unprocessed_insert_time))
			min_unprocessed_insert_time = entry->create_time;
	}
}


void ReplicatedMergeTreeQueue::insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
	time_t prev_min_unprocessed_insert_time;

	{
		std::lock_guard<std::mutex> lock(mutex);
		prev_min_unprocessed_insert_time = min_unprocessed_insert_time;
		insertUnlocked(entry);
	}

	if (min_unprocessed_insert_time != prev_min_unprocessed_insert_time)
		updateTimesInZooKeeper(zookeeper, true, false);
}


void ReplicatedMergeTreeQueue::updateTimesOnRemoval(
	const LogEntryPtr & entry,
	bool & min_unprocessed_insert_time_changed,
	bool & max_processed_insert_time_changed)
{
	if (entry->type != LogEntry::GET_PART)
		return;

	inserts_by_time.erase(entry);

	if (inserts_by_time.empty())
	{
		min_unprocessed_insert_time = 0;
		min_unprocessed_insert_time_changed = true;
	}
	else if ((*inserts_by_time.begin())->create_time > min_unprocessed_insert_time)
	{
		min_unprocessed_insert_time = (*inserts_by_time.begin())->create_time;
		min_unprocessed_insert_time_changed = true;
	}

	if (entry->create_time > max_processed_insert_time)
	{
		max_processed_insert_time = entry->create_time;
		max_processed_insert_time_changed = true;
	}
}


void ReplicatedMergeTreeQueue::updateTimesInZooKeeper(
	zkutil::ZooKeeperPtr zookeeper,
	bool min_unprocessed_insert_time_changed,
	bool max_processed_insert_time_changed)
{
	/// Здесь может быть race condition (при одновременном выполнении разных remove).
	/// Считаем его несущественным (в течение небольшого времени, в ZK будет записано немного отличающееся значение времени).
	/// Также читаем значения переменных min_unprocessed_insert_time, max_processed_insert_time без синхронизации.
	zkutil::Ops ops;

	if (min_unprocessed_insert_time_changed)
		ops.push_back(new zkutil::Op::SetData(
			replica_path + "/min_unprocessed_insert_time", toString(min_unprocessed_insert_time), -1));

	if (max_processed_insert_time_changed)
		ops.push_back(new zkutil::Op::SetData(
			replica_path + "/max_processed_insert_time", toString(max_processed_insert_time), -1));

	if (!ops.empty())
	{
		auto code = zookeeper->tryMulti(ops);

		if (code != ZOK)
			LOG_ERROR(log, "Couldn't set value of nodes for insert times ("
				<< replica_path << "/min_unprocessed_insert_time, max_processed_insert_time)" << ": "
				<< zkutil::ZooKeeper::error2string(code) + ". This shouldn't happen often.");
	}
}


void ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
	auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);

	if (code != ZOK)
		LOG_ERROR(log, "Couldn't remove " << replica_path << "/queue/" << entry->znode_name << ": "
			<< zkutil::ZooKeeper::error2string(code) << ". This shouldn't happen often.");

	bool min_unprocessed_insert_time_changed = false;
	bool max_processed_insert_time_changed = false;

	{
		std::lock_guard<std::mutex> lock(mutex);

		/// Удалим задание из очереди в оперативке.
		/// Нельзя просто обратиться по заранее сохраненному итератору, потому что задание мог успеть удалить кто-то другой.
		/// Почему просматриваем очередь с конца?
		///  - потому что задание к выполнению сначала перемещается в конец очереди, чтобы в случае неуспеха оно осталось в конце.
		for (Queue::iterator it = queue.end(); it != queue.begin();)
		{
			--it;
			if (*it == entry)
			{
				queue.erase(it);
				break;
			}
		}

		updateTimesOnRemoval(entry, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
	}

	updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
}


bool ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
	LogEntryPtr found;

	bool min_unprocessed_insert_time_changed = false;
	bool max_processed_insert_time_changed = false;

	{
		std::lock_guard<std::mutex> lock(mutex);

		for (Queue::iterator it = queue.begin(); it != queue.end();)
		{
			if ((*it)->new_part_name == part_name)
			{
				found = *it;
				queue.erase(it++);
				updateTimesOnRemoval(found, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
				break;
			}
			else
				++it;
		}
	}

	if (!found)
		return false;

	zookeeper->tryRemove(replica_path + "/queue/" + found->znode_name);
	updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

	return true;
}


bool ReplicatedMergeTreeQueue::pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, zkutil::EventPtr next_update_event)
{
	std::lock_guard<std::mutex> lock(pull_logs_to_queue_mutex);

	String index_str = zookeeper->get(replica_path + "/log_pointer");
	UInt64 index;

	Strings log_entries = zookeeper->getChildren(zookeeper_path + "/log");

	if (index_str.empty())
	{
		/// Если у нас еще нет указателя на лог, поставим указатель на первую запись в нем.
		index = log_entries.empty() ? 0 : parse<UInt64>(std::min_element(log_entries.begin(), log_entries.end())->substr(strlen("log-")));

		zookeeper->set(replica_path + "/log_pointer", toString(index));
	}
	else
	{
		index = parse<UInt64>(index_str);
	}

	String min_log_entry = "log-" + padIndex(index);

	/// Множество записей лога, которые должны быть скопированы в очередь.

	log_entries.erase(
		std::remove_if(log_entries.begin(), log_entries.end(), [&min_log_entry](const String & entry) { return entry < min_log_entry; }),
		log_entries.end());

	if (!log_entries.empty())
	{
		std::sort(log_entries.begin(), log_entries.end());

		/// ZK содержит ограничение на количество или суммарный размер операций в multi-запросе.
		/// При превышении ограничения просто разрывается соединение.
		/// Константа выбрана с запасом. Ограничение по-умолчанию в ZK - 1 МБ данных суммарно.
		/// Средний размер значения узла в данном случае, меньше десяти килобайт.
		static constexpr auto MAX_MULTI_OPS = 100;

		for (size_t i = 0, size = log_entries.size(); i < size; i += MAX_MULTI_OPS)
		{
			auto begin = log_entries.begin() + i;
			auto end = i + MAX_MULTI_OPS >= log_entries.size()
				? log_entries.end()
				: (begin + MAX_MULTI_OPS);
			auto last = end - 1;

			String last_entry = *last;
			if (!startsWith(last_entry, "log-"))
				throw Exception("Error in zookeeper data: unexpected node " + last_entry + " in " + zookeeper_path + "/log",
					ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER);

			UInt64 last_entry_index = parse<UInt64>(last_entry.substr(strlen("log-")));

			LOG_DEBUG(log, "Pulling " << (end - begin) << " entries to queue: " << *begin << " - " << *last);

			std::vector<std::pair<String, zkutil::ZooKeeper::GetFuture>> futures;
			futures.reserve(end - begin);

			for (auto it = begin; it != end; ++it)
				futures.emplace_back(*it, zookeeper->asyncGet(zookeeper_path + "/log/" + *it));

			/// Одновременно добавим все новые записи в очередь и продвинем указатель на лог.

			zkutil::Ops ops;
			std::vector<LogEntryPtr> copied_entries;
			copied_entries.reserve(end - begin);

			bool min_unprocessed_insert_time_changed = false;

			for (auto & future : futures)
			{
				zkutil::ZooKeeper::ValueAndStat res = future.second.get();
				copied_entries.emplace_back(LogEntry::parse(res.value, res.stat));

				ops.push_back(new zkutil::Op::Create(
					replica_path + "/queue/queue-", res.value, zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));

				const auto & entry = *copied_entries.back();
				if (entry.type == LogEntry::GET_PART)
				{
					if (entry.create_time && (!min_unprocessed_insert_time || entry.create_time < min_unprocessed_insert_time))
					{
						min_unprocessed_insert_time = entry.create_time;
						min_unprocessed_insert_time_changed = true;
					}
				}
			}

			ops.push_back(new zkutil::Op::SetData(
				replica_path + "/log_pointer", toString(last_entry_index + 1), -1));

			if (min_unprocessed_insert_time_changed)
				ops.push_back(new zkutil::Op::SetData(
					replica_path + "/min_unprocessed_insert_time", toString(min_unprocessed_insert_time), -1));

			auto results = zookeeper->multi(ops);

			/// Сейчас мы успешно обновили очередь в ZooKeeper. Обновим её в оперативке.

			try
			{
				std::lock_guard<std::mutex> lock(mutex);

				for (size_t i = 0, size = copied_entries.size(); i < size; ++i)
				{
					String path_created = dynamic_cast<zkutil::Op::Create &>(ops[i]).getPathCreated();
					copied_entries[i]->znode_name = path_created.substr(path_created.find_last_of('/') + 1);

					insertUnlocked(copied_entries[i]);
				}

				last_queue_update = time(0);
			}
			catch (...)
			{
				/// Если не удалось, то данные в оперативке некорректные. Во избежание возможной дальнейшей порчи данных в ZK, убъёмся.
				/// Попадание сюда возможно лишь в случае неизвестной логической ошибки.
				std::terminate();
			}

			if (!copied_entries.empty())
				LOG_DEBUG(log, "Pulled " << copied_entries.size() << " entries to queue.");
		}
	}

	if (next_update_event)
	{
		if (zookeeper->exists(zookeeper_path + "/log/log-" + padIndex(index), nullptr, next_update_event))
			next_update_event->set();
	}

	return !log_entries.empty();
}


ReplicatedMergeTreeQueue::StringSet ReplicatedMergeTreeQueue::moveSiblingPartsForMergeToEndOfQueue(const String & part_name)
{
	std::lock_guard<std::mutex> lock(mutex);

	/// Найдем действие по объединению этого куска с другими. Запомним других.
	StringSet parts_for_merge;
	Queue::iterator merge_entry;
	for (Queue::iterator it = queue.begin(); it != queue.end(); ++it)
	{
		if ((*it)->type == LogEntry::MERGE_PARTS)
		{
			if (std::find((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end(), part_name)
				!= (*it)->parts_to_merge.end())
			{
				parts_for_merge = StringSet((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end());
				merge_entry = it;
				break;
			}
		}
	}

	if (!parts_for_merge.empty())
	{
		/// Переместим в конец очереди действия, получающие parts_for_merge.
		for (Queue::iterator it = queue.begin(); it != queue.end();)
		{
			auto it0 = it;
			++it;

			if (it0 == merge_entry)
				break;

			if (((*it0)->type == LogEntry::MERGE_PARTS || (*it0)->type == LogEntry::GET_PART)
				&& parts_for_merge.count((*it0)->new_part_name))
			{
				queue.splice(queue.end(), queue, it0, it);
			}
		}
	}

	return parts_for_merge;
}


void ReplicatedMergeTreeQueue::removeGetsAndMergesInRange(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
	Queue to_wait;
	size_t removed_entries = 0;
	bool min_unprocessed_insert_time_changed = false;
	bool max_processed_insert_time_changed = false;

	/// Удалим из очереди операции с кусками, содержащимися в удаляемом диапазоне.
	std::unique_lock<std::mutex> lock(mutex);
	for (Queue::iterator it = queue.begin(); it != queue.end();)
	{
		if (((*it)->type == LogEntry::GET_PART || (*it)->type == LogEntry::MERGE_PARTS) &&
			ActiveDataPartSet::contains(part_name, (*it)->new_part_name))
		{
			if ((*it)->currently_executing)
				to_wait.push_back(*it);
			auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
			if (code != ZOK)
				LOG_INFO(log, "Couldn't remove " << replica_path + "/queue/" + (*it)->znode_name << ": "
					<< zkutil::ZooKeeper::error2string(code));

			updateTimesOnRemoval(*it, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
			queue.erase(it++);
			++removed_entries;
		}
		else
			++it;
	}

	updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

	LOG_DEBUG(log, "Removed " << removed_entries << " entries from queue. "
		"Waiting for " << to_wait.size() << " entries that are currently executing.");

	/// Дождемся завершения операций с кусками, содержащимися в удаляемом диапазоне.
	for (LogEntryPtr & entry : to_wait)
		entry->execution_complete.wait(lock, [&entry] { return !entry->currently_executing; });
}


bool ReplicatedMergeTreeQueue::shouldExecuteLogEntry(const LogEntry & entry, String & out_postpone_reason, MergeTreeDataMerger & merger)
{
	/// mutex уже захвачен. Функция вызывается только из selectEntryToProcess.

	if (entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART)
	{
		/// Проверим, не создаётся ли сейчас этот же кусок другим действием.
		if (future_parts.count(entry.new_part_name))
		{
			String reason = "Not executing log entry for part " + entry.new_part_name
				+ " because another log entry for the same part is being processed. This shouldn't happen often.";
			LOG_DEBUG(log, reason);
			out_postpone_reason = reason;
			return false;

			/** Когда соответствующее действие завершится, то shouldExecuteLogEntry, в следующий раз, пройдёт успешно,
			  *  и элемент очереди будет обработан. Сразу же в функции executeLogEntry будет выяснено, что кусок у нас уже есть,
			  *  и элемент очереди будет сразу считаться обработанным.
			  */
		}

		/// Более сложная проверка - не создаётся ли сейчас другим действием кусок, который покроет этот кусок.
		/// NOTE То, что выше - избыточно, но оставлено ради более удобного сообщения в логе.
		ActiveDataPartSet::Part result_part;
		ActiveDataPartSet::parsePartName(entry.new_part_name, result_part);

		/// Оно может тормозить при большом размере future_parts. Но он не может быть большим, так как ограничен BackgroundProcessingPool.
		for (const auto & future_part_name : future_parts)
		{
			ActiveDataPartSet::Part future_part;
			ActiveDataPartSet::parsePartName(future_part_name, future_part);

			if (future_part.contains(result_part))
			{
				String reason = "Not executing log entry for part " + entry.new_part_name
					+ " because another log entry for covering part " + future_part_name + " is being processed.";
				LOG_DEBUG(log, reason);
				out_postpone_reason = reason;
				return false;
			}
		}
	}

	if (entry.type == LogEntry::MERGE_PARTS)
	{
		/** Если какая-то из нужных частей сейчас передается или мерджится, подождем окончания этой операции.
		  * Иначе, даже если всех нужных частей для мерджа нет, нужно попытаться сделать мердж.
		  * Если каких-то частей не хватает, вместо мерджа будет попытка скачать кусок.
		  * Такая ситуация возможна, если получение какого-то куска пофейлилось, и его переместили в конец очереди.
		  */
		for (const auto & name : entry.parts_to_merge)
		{
			if (future_parts.count(name))
			{
				String reason = "Not merging into part " + entry.new_part_name
					+ " because part " + name + " is not ready yet (log entry for that part is being processed).";
				LOG_TRACE(log, reason);
				out_postpone_reason = reason;
				return false;
			}
		}

		if (merger.isCancelled())
		{
			String reason = "Not executing log entry for part " + entry.new_part_name + " because merges are cancelled now.";
			LOG_DEBUG(log, reason);
			out_postpone_reason = reason;
			return false;
		}
	}

	return true;
}


ReplicatedMergeTreeQueue::CurrentlyExecuting::CurrentlyExecuting(ReplicatedMergeTreeQueue::LogEntryPtr & entry, ReplicatedMergeTreeQueue & queue)
	: entry(entry), queue(queue)
{
	entry->currently_executing = true;
	++entry->num_tries;
	entry->last_attempt_time = time(0);

	if (!queue.future_parts.insert(entry->new_part_name).second)
		throw Exception("Tagging already tagged future part " + entry->new_part_name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
}

ReplicatedMergeTreeQueue::CurrentlyExecuting::~CurrentlyExecuting()
{
	std::lock_guard<std::mutex> lock(queue.mutex);

	entry->currently_executing = false;
	entry->execution_complete.notify_all();

	if (!queue.future_parts.erase(entry->new_part_name))
		LOG_ERROR(queue.log, "Untagging already untagged future part " + entry->new_part_name + ". This is a bug.");
}


ReplicatedMergeTreeQueue::SelectedEntry ReplicatedMergeTreeQueue::selectEntryToProcess(MergeTreeDataMerger & merger)
{
	std::lock_guard<std::mutex> lock(mutex);

	LogEntryPtr entry;

	for (Queue::iterator it = queue.begin(); it != queue.end(); ++it)
	{
		if ((*it)->currently_executing)
			continue;

		if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, merger))
		{
			entry = *it;
			queue.splice(queue.end(), queue, it);
			break;
		}
		else
		{
			++(*it)->num_postponed;
			(*it)->last_postpone_time = time(0);
		}
	}

	if (entry)
		return { entry, std::unique_ptr<CurrentlyExecuting>{ new CurrentlyExecuting(entry, *this) } };
	else
		return {};
}


bool ReplicatedMergeTreeQueue::processEntry(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry, const std::function<bool(LogEntryPtr &)> func)
{
	std::exception_ptr saved_exception;

	try
	{
		if (func(entry))
			remove(zookeeper, entry);
	}
	catch (...)
	{
		saved_exception = std::current_exception();
	}

	if (saved_exception)
	{
		std::lock_guard<std::mutex> lock(mutex);
		entry->exception = saved_exception;
		return false;
	}

	return true;
}


bool ReplicatedMergeTreeQueue::partWillBeMergedOrMergesDisabled(const String & part_name) const
{
	return virtual_parts.getContainingPart(part_name) != part_name;
}

void ReplicatedMergeTreeQueue::disableMergesInRange(const String & part_name)
{
	virtual_parts.add(part_name);
}



ReplicatedMergeTreeQueue::Status ReplicatedMergeTreeQueue::getStatus()
{
	std::lock_guard<std::mutex> lock(mutex);

	Status res;

	res.future_parts = future_parts.size();
	res.queue_size = queue.size();
	res.last_queue_update = last_queue_update;

	res.inserts_in_queue = 0;
	res.merges_in_queue = 0;
	res.queue_oldest_time = 0;
	res.inserts_oldest_time = 0;
	res.merges_oldest_time = 0;

	for (const LogEntryPtr & entry : queue)
	{
		if (entry->create_time && (!res.queue_oldest_time || entry->create_time < res.queue_oldest_time))
			res.queue_oldest_time = entry->create_time;

		if (entry->type == LogEntry::GET_PART)
		{
			++res.inserts_in_queue;

			if (entry->create_time && (!res.inserts_oldest_time || entry->create_time < res.inserts_oldest_time))
			{
				res.inserts_oldest_time = entry->create_time;
				res.oldest_part_to_get = entry->new_part_name;
			}
		}

		if (entry->type == LogEntry::MERGE_PARTS)
		{
			++res.merges_in_queue;

			if (entry->create_time && (!res.merges_oldest_time || entry->create_time < res.merges_oldest_time))
			{
				res.merges_oldest_time = entry->create_time;
				res.oldest_part_to_merge_to = entry->new_part_name;
			}
		}
	}

	return res;
}


void ReplicatedMergeTreeQueue::getEntries(LogEntriesData & res)
{
	res.clear();
	std::lock_guard<std::mutex> lock(mutex);

	res.reserve(queue.size());
	for (const auto & entry : queue)
		res.emplace_back(*entry);
}


void ReplicatedMergeTreeQueue::countMerges(size_t & all_merges, size_t & big_merges, size_t max_big_merges,
	const std::function<bool(const String &)> & is_part_big)
{
	all_merges = 0;
	big_merges = 0;

	std::lock_guard<std::mutex> lock(mutex);

	for (const auto & entry : queue)
	{
		if (entry->type == LogEntry::MERGE_PARTS)
		{
			++all_merges;

			if (big_merges + big_merges < max_big_merges)
			{
				for (const String & name : entry->parts_to_merge)
				{
					if (is_part_big(name))
					{
						++big_merges;
						break;
					}
				}
			}
		}
	}
}


void ReplicatedMergeTreeQueue::getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const
{
	out_min_unprocessed_insert_time = min_unprocessed_insert_time;
	out_max_processed_insert_time = max_processed_insert_time;
}


String padIndex(Int64 index)
{
	String index_str = toString(index);
	return std::string(10 - index_str.size(), '0') + index_str;
}

}
