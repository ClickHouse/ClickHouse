#include <DB/Interpreters/ProcessList.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int TOO_MUCH_SIMULTANEOUS_QUERIES;
	extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
}


ProcessList::EntryPtr ProcessList::insert(
	const String & query_, const String & user_, const String & query_id_, const Poco::Net::IPAddress & ip_address_,
	const Settings & settings)
{
	EntryPtr res;

	{
		std::lock_guard<std::mutex> lock(mutex);

		if (max_size && cur_size >= max_size
			&& (!settings.queue_max_wait_ms.totalMilliseconds() || !have_space.tryWait(mutex, settings.queue_max_wait_ms.totalMilliseconds())))
			throw Exception("Too much simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MUCH_SIMULTANEOUS_QUERIES);

		{
			UserToQueries::iterator user_process_list = user_to_queries.find(user_);

			if (user_process_list != user_to_queries.end())
			{
				if (settings.max_concurrent_queries_for_user && user_process_list->second.queries.size() >= settings.max_concurrent_queries_for_user)
					throw Exception("Too much simultaneous queries for user " + user_
						+ ". Current: " + toString(user_process_list->second.queries.size())
						+ ", maximum: " + toString(settings.max_concurrent_queries_for_user),
						ErrorCodes::TOO_MUCH_SIMULTANEOUS_QUERIES);

				if (!query_id_.empty())
				{
					ProcessListForUser::QueryToElement::iterator element = user_process_list->second.queries.find(query_id_);
					if (element != user_process_list->second.queries.end())
					{
						if (!settings.replace_running_query)
							throw Exception("Query with id = " + query_id_ + " is already running.",
								ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

						element->second->is_cancelled = true;
						/// В случае если запрос отменяется, данные о нем удаляются из мапа в момент отмены.
						user_process_list->second.queries.erase(element);
					}
				}
			}
		}

		++cur_size;

		res.reset(new Entry(*this, cont.emplace(cont.end(),
			query_, user_, query_id_, ip_address_,
			settings.limits.max_memory_usage, settings.memory_tracker_fault_probability,
			priorities.insert(settings.priority))));

		if (!query_id_.empty())
		{
			ProcessListForUser & user_process_list = user_to_queries[user_];
			user_process_list.queries[query_id_] = &res->get();

			if (current_memory_tracker)
			{
				/// Отслеживаем суммарное потребление оперативки на одновременно выполняющиеся запросы одного пользователя.
				user_process_list.user_memory_tracker.setLimit(settings.limits.max_memory_usage_for_user);
				user_process_list.user_memory_tracker.setDescription("(for user)");
				current_memory_tracker->setNext(&user_process_list.user_memory_tracker);

				/// Отслеживаем суммарное потребление оперативки на все одновременно выполняющиеся запросы.
				total_memory_tracker.setLimit(settings.limits.max_memory_usage_for_all_queries);
				total_memory_tracker.setDescription("(total)");
				user_process_list.user_memory_tracker.setNext(&total_memory_tracker);
			}
		}
	}

	return res;
}


ProcessListEntry::~ProcessListEntry()
{
	std::lock_guard<std::mutex> lock(parent.mutex);

	/// Важен порядок удаления memory_tracker-ов.

	String user = it->user;
	String query_id = it->query_id;
	bool is_cancelled = it->is_cancelled;

	/// Здесь удаляется memory_tracker одного запроса.
	parent.cont.erase(it);

	ProcessList::UserToQueries::iterator user_process_list = parent.user_to_queries.find(user);
	if (user_process_list != parent.user_to_queries.end())
	{
		/// В случае, если запрос отменяется, данные о нем удаляются из мапа в момент отмены, а не здесь.
		if (!is_cancelled && !query_id.empty())
		{
			ProcessListForUser::QueryToElement::iterator element = user_process_list->second.queries.find(query_id);
			if (element != user_process_list->second.queries.end())
				user_process_list->second.queries.erase(element);
		}

		/// Здесь удаляется memory_tracker на пользователя. В это время, ссылающийся на него memory_tracker одного запроса не живёт.

		/// Если запросов для пользователя больше нет, то удаляем запись.
		/// При этом также очищается MemoryTracker на пользователя, и сообщение о потреблении памяти выводится в лог.
		/// Важно иногда сбрасывать MemoryTracker, так как в нём может накапливаться смещённость
		///  в следствие того, что есть случаи, когда память может быть выделена при обработке запроса, а освобождена - позже.
		if (user_process_list->second.queries.empty())
			parent.user_to_queries.erase(user_process_list);
	}

	--parent.cur_size;
	parent.have_space.signal();

	/// Здесь удаляется memory_tracker на все запросы. В это время никакие другие memory_tracker-ы не живут.
	if (parent.cur_size == 0)
	{
		/// Сбрасываем MemoryTracker, аналогично (см. выше).
		parent.total_memory_tracker.logPeakMemoryUsage();
		parent.total_memory_tracker.reset();
	}
}


void ProcessList::addTemporaryTable(ProcessListElement & elem, const String & table_name, StoragePtr storage)
{
	std::lock_guard<std::mutex> lock(mutex);

	elem.temporary_tables[table_name] = storage;
}


StoragePtr ProcessList::tryGetTemporaryTable(const String & query_id, const String & table_name) const
{
	std::lock_guard<std::mutex> lock(mutex);

	/// NOTE Ищем по всем user-ам. То есть, нет изоляции, и сложность O(users).
	for (const auto & user_queries : user_to_queries)
	{
		auto it = user_queries.second.queries.find(query_id);
		if (user_queries.second.queries.end() == it)
			continue;

		auto jt = (*it->second).temporary_tables.find(table_name);
		if ((*it->second).temporary_tables.end() == jt)
			continue;

		return jt->second;
	}

	return {};
}

}
