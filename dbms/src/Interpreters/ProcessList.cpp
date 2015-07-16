#include <DB/Interpreters/ProcessList.h>

namespace DB
{


ProcessList::EntryPtr ProcessList::insert(
	const String & query_, const String & user_, const String & query_id_, const Poco::Net::IPAddress & ip_address_,
	size_t max_memory_usage, size_t max_wait_milliseconds, bool replace_running_query, QueryPriorities::Priority priority)
{
	EntryPtr res;

	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		if (max_size && cur_size >= max_size && (!max_wait_milliseconds || !have_space.tryWait(mutex, max_wait_milliseconds)))
			throw Exception("Too much simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MUCH_SIMULTANEOUS_QUERIES);

		if (!query_id_.empty())
		{
			UserToQueries::iterator queries = user_to_queries.find(user_);

			if (queries != user_to_queries.end())
			{
				QueryToElement::iterator element = queries->second.find(query_id_);
				if (element != queries->second.end())
				{
					if (!replace_running_query)
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
			query_, user_, query_id_, ip_address_, max_memory_usage, priorities.insert(priority))));

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
		}
	}

	parent.cont.erase(it);
	--parent.cur_size;
	parent.have_space.signal();
}

}
