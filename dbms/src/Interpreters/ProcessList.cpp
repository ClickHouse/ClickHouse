#include <Interpreters/ProcessList.h>
#include <Interpreters/Settings.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
}


ProcessList::EntryPtr ProcessList::insert(
    const String & query_, const IAST * ast, const ClientInfo & client_info, const Settings & settings)
{
    EntryPtr res;
    bool is_kill_query = ast && typeid_cast<const ASTKillQueryQuery *>(ast);

    if (client_info.current_query_id.empty())
        throw Exception("Query id cannot be empty", ErrorCodes::LOGICAL_ERROR);

    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!is_kill_query && max_size && cur_size >= max_size
            && (!settings.queue_max_wait_ms.totalMilliseconds() || !have_space.tryWait(mutex, settings.queue_max_wait_ms.totalMilliseconds())))
            throw Exception("Too many simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);

        /** Why we use current user?
          * Because initial one is passed by client and credentials for it is not verified,
          *  and using initial_user for limits will be insecure.
          *
          * Why we use current_query_id?
          * Because we want to allow distributed queries that will run multiple secondary queries on same server,
          *  like SELECT count() FROM remote('127.0.0.{1,2}', system.numbers)
          *  so they must have different query_ids.
          */

        {
            auto user_process_list = user_to_queries.find(client_info.current_user);

            if (user_process_list != user_to_queries.end())
            {
                if (!is_kill_query && settings.max_concurrent_queries_for_user
                    && user_process_list->second.queries.size() >= settings.max_concurrent_queries_for_user)
                    throw Exception("Too many simultaneous queries for user " + client_info.current_user
                        + ". Current: " + toString(user_process_list->second.queries.size())
                        + ", maximum: " + settings.max_concurrent_queries_for_user.toString(),
                        ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);

                auto range = user_process_list->second.queries.equal_range(client_info.current_query_id);
                if (range.first != range.second)
                {
                    if (!settings.replace_running_query)
                        throw Exception("Query with id = " + client_info.current_query_id + " is already running.",
                            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

                    /// Ask queries to cancel. They will check this flag.
                    for (auto it = range.first; it != range.second; ++it)
                        it->second->is_cancelled.store(true, std::memory_order_relaxed);
                }
            }
        }

        ++cur_size;

        res = std::make_shared<Entry>(*this, cont.emplace(cont.end(),
            query_, client_info,
            settings.max_memory_usage, settings.memory_tracker_fault_probability,
            priorities.insert(settings.priority)));

        ProcessListForUser & user_process_list = user_to_queries[client_info.current_user];
        user_process_list.queries.emplace(client_info.current_query_id, &res->get());

        if (current_memory_tracker)
        {
            /// Limits are only raised (to be more relaxed) or set to something instead of zero,
            ///  because settings for different queries will interfere each other:
            ///  setting from one query effectively sets values for all other queries.

            /// Track memory usage for all simultaneously running queries from single user.
            user_process_list.user_memory_tracker.setOrRaiseLimit(settings.max_memory_usage_for_user);
            user_process_list.user_memory_tracker.setDescription("(for user)");
            current_memory_tracker->setNext(&user_process_list.user_memory_tracker);

            /// Track memory usage for all simultaneously running queries.
            /// You should specify this value in configuration for default profile,
            ///  not for specific users, sessions or queries,
            ///  because this setting is effectively global.
            total_memory_tracker.setOrRaiseLimit(settings.max_memory_usage_for_all_queries);
            total_memory_tracker.setDescription("(total)");
            user_process_list.user_memory_tracker.setNext(&total_memory_tracker);
        }

        if (settings.max_network_bandwidth_for_user && !user_process_list.user_throttler)
        {
            user_process_list.user_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_user, 0,
                "Network bandwidth limit for a user exceeded.");
        }

        res->get().user_process_list = &user_process_list;
    }

    return res;
}


ProcessListEntry::~ProcessListEntry()
{
    /// Destroy all streams to avoid long lock of ProcessList
    it->releaseQueryStreams();

    std::lock_guard<std::mutex> lock(parent.mutex);

    String user = it->getClientInfo().current_user;
    String query_id = it->getClientInfo().current_query_id;

    const ProcessListElement * process_list_element_ptr = &*it;

    /// This removes the memory_tracker of one query.
    parent.cont.erase(it);

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(&Logger::get("ProcessList"), "Logical error: cannot find user in ProcessList");
        std::terminate();
    }

    ProcessListForUser & user_process_list = user_process_list_it->second;

    bool found = false;

    auto range = user_process_list.queries.equal_range(query_id);
    if (range.first != range.second)
    {
        for (auto it = range.first; it != range.second; ++it)
        {
            if (it->second == process_list_element_ptr)
            {
                user_process_list.queries.erase(it);
                found = true;
                break;
            }
        }
    }

    if (!found)
    {
        LOG_ERROR(&Logger::get("ProcessList"), "Logical error: cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }

    /// If there are no more queries for the user, then we will reset memory tracker and network throttler.
    if (user_process_list.queries.empty())
        user_process_list.reset();

    --parent.cur_size;
    parent.have_space.signal();

    /// This removes memory_tracker for all requests. At this time, no other memory_trackers live.
    if (parent.cur_size == 0)
    {
        /// Reset MemoryTracker, similarly (see above).
        parent.total_memory_tracker.logPeakMemoryUsage();
        parent.total_memory_tracker.reset();
    }
}


void ProcessListElement::setQueryStreams(const BlockIO & io)
{
    std::lock_guard<std::mutex> lock(query_streams_mutex);

    query_stream_in = io.in;
    query_stream_out = io.out;
    query_streams_initialized = true;
}

void ProcessListElement::releaseQueryStreams()
{
    std::lock_guard<std::mutex> lock(query_streams_mutex);

    query_streams_initialized = false;
    query_streams_released = true;
    query_stream_in.reset();
    query_stream_out.reset();
}

bool ProcessListElement::streamsAreReleased()
{
    std::lock_guard<std::mutex> lock(query_streams_mutex);

    return query_streams_released;
}

bool ProcessListElement::tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const
{
    std::lock_guard<std::mutex> lock(query_streams_mutex);

    if (!query_streams_initialized)
        return false;

    in = query_stream_in;
    out = query_stream_out;
    return true;
}


ThrottlerPtr ProcessListElement::getUserNetworkThrottler()
{
    if (!user_process_list)
        return {};
    return user_process_list->user_throttler;
}


ProcessListElement * ProcessList::tryGetProcessListElement(const String & current_query_id, const String & current_user)
{
    auto user_it = user_to_queries.find(current_user);
    if (user_it != user_to_queries.end())
    {
        const auto & user_queries = user_it->second.queries;
        auto query_it = user_queries.find(current_query_id);

        if (query_it != user_queries.end())
            return query_it->second;
    }

    return nullptr;
}


ProcessList::CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill)
{
    std::lock_guard<std::mutex> lock(mutex);

    ProcessListElement * elem = tryGetProcessListElement(current_query_id, current_user);

    if (!elem)
        return CancellationCode::NotFound;

    /// Streams are destroyed, and ProcessListElement will be deleted from ProcessList soon. We need wait a little bit
    if (elem->streamsAreReleased())
        return CancellationCode::CancelSent;

    BlockInputStreamPtr input_stream;
    BlockOutputStreamPtr output_stream;

    if (elem->tryGetQueryStreams(input_stream, output_stream))
    {
        IProfilingBlockInputStream * input_stream_casted;
        if (input_stream && (input_stream_casted = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get())))
        {
            input_stream_casted->cancel(kill);
            return CancellationCode::CancelSent;
        }
        return CancellationCode::CancelCannotBeSent;
    }

    return CancellationCode::QueryIsNotInitializedYet;
}

}
