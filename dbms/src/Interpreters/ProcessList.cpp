#include <DB/Interpreters/ProcessList.h>

namespace DB
{


ProcessList::EntryPtr ProcessList::insert(
	const String & query_, const String & user_, const String & query_id_, const Poco::Net::IPAddress & ip_address_,
	const Settings & settings)
{
	EntryPtr res;

	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		if (max_size && cur_size >= max_size
			&& (!settings.queue_max_wait_ms.totalMilliseconds() || !have_space.tryWait(mutex, settings.queue_max_wait_ms.totalMilliseconds())))
			throw Exception("Too much simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MUCH_SIMULTANEOUS_QUERIES);

		UserToQueries::iterator queries = user_to_queries.find(user_);

		if (queries != user_to_queries.end())
		{
			if (settings.max_concurrent_queries_for_user && queries->second.size() >= settings.max_concurrent_queries_for_user)
				throw Exception("Too much simultaneous queries for user " + user_
					+ ". Current: " + toString(queries->second.size())
					+ ", maximum: " + toString(settings.max_concurrent_queries_for_user),
					ErrorCodes::TOO_MUCH_SIMULTANEOUS_QUERIES);

			if (!query_id_.empty())
			{
				QueryToElement::iterator element = queries->second.find(query_id_);
				if (element != queries->second.end())
				{
					if (!settings.replace_running_query)
						throw Exception("Query with id = " + query_id_ + " is already running.",
							ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
					element->second->is_cancelled = true;
					/// В случае если запрос отменяется, данные о нем удаляются из мапа в момент отмены.
					queries->second.erase(element);
				}
			}
		}

		++cur_size;

		res.reset(new Entry(*this, cont.emplace(cont.end(),
			query_, user_, query_id_, ip_address_,
			settings.limits.max_memory_usage, settings.memory_tracker_fault_probability,
			priorities.insert(settings.priority))));

		if (!query_id_.empty())
			user_to_queries[user_][query_id_] = &res->get();
	}

	return res;
}


ProcessListEntry::~ProcessListEntry()
{
	Poco::ScopedLock<Poco::FastMutex> lock(parent.mutex);

	/// В случае, если запрос отменяется, данные о нем удаляются из мапа в момент отмены.
	if (!it->is_cancelled && !it->query_id.empty())
	{
		ProcessList::UserToQueries::iterator queries = parent.user_to_queries.find(it->user);
		if (queries != parent.user_to_queries.end())
		{
			ProcessList::QueryToElement::iterator element = queries->second.find(it->query_id);
			if (element != queries->second.end())
				queries->second.erase(element);

			/// Если запросов для пользователя больше нет, то удаляем запись
			if (queries->second.empty())
				parent.user_to_queries.erase(queries);
		}
	}

	parent.cont.erase(it);
	--parent.cur_size;
	parent.have_space.signal();
}


void ProcessList::addTemporaryTable(ProcessListElement & elem, const String & table_name, StoragePtr storage)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	elem.temporary_tables[table_name] = storage;
}


StoragePtr ProcessList::tryGetTemporaryTable(const String & query_id, const String & table_name) const
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	/// NOTE Ищем по всем user-ам. То есть, нет изоляции, и сложность O(users).
	for (const auto & user_queries : user_to_queries)
	{
		auto it = user_queries.second.find(query_id);
		if (user_queries.second.end() == it)
			continue;

		auto jt = (*it->second).temporary_tables.find(table_name);
		if ((*it->second).temporary_tables.end() == jt)
			continue;

		return jt->second;
	}

	return {};
}

}
