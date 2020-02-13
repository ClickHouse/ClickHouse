#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/IBlockInputStream.h>
#include <common/logger_useful.h>
#include <chrono>


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}


/// Should we execute the query even if max_concurrent_queries limit is exhausted
static bool isUnlimitedQuery(const IAST * ast)
{
    if (!ast)
        return false;

    /// It is KILL QUERY
    if (ast->as<ASTKillQueryQuery>())
        return true;

    /// It is SELECT FROM system.processes
    /// NOTE: This is very rough check.
    /// False negative: USE system; SELECT * FROM processes;
    /// False positive: SELECT * FROM system.processes CROSS JOIN (SELECT ...)

    if (const auto * ast_selects = ast->as<ASTSelectWithUnionQuery>())
    {
        if (!ast_selects->list_of_selects || ast_selects->list_of_selects->children.empty())
            return false;

        const auto * ast_select = ast_selects->list_of_selects->children[0]->as<ASTSelectQuery>();
        if (!ast_select)
            return false;

        if (auto database_and_table = getDatabaseAndTable(*ast_select, 0))
            return database_and_table->database == "system" && database_and_table->table == "processes";

        return false;
    }

    return false;
}


ProcessList::ProcessList(size_t max_size_)
    : max_size(max_size_)
{
    total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);
}


ProcessList::EntryPtr ProcessList::insert(const String & query_, const IAST * ast, Context & query_context)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context.getClientInfo();
    const Settings & settings = query_context.getSettingsRef();

    if (client_info.current_query_id.empty())
        throw Exception("Query id cannot be empty", ErrorCodes::LOGICAL_ERROR);

    bool is_unlimited_query = isUnlimitedQuery(ast);

    {
        std::unique_lock lock(mutex);

        const auto queue_max_wait_ms = settings.queue_max_wait_ms.totalMilliseconds();
        if (!is_unlimited_query && max_size && processes.size() >= max_size)
        {
            if (queue_max_wait_ms)
                LOG_WARNING(&Logger::get("ProcessList"), "Too many simultaneous queries, will wait " << queue_max_wait_ms << " ms.");
            if (!queue_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(queue_max_wait_ms), [&]{ return processes.size() < max_size; }))
                throw Exception("Too many simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
        }

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
                if (!is_unlimited_query && settings.max_concurrent_queries_for_user
                    && user_process_list->second.queries.size() >= settings.max_concurrent_queries_for_user)
                    throw Exception("Too many simultaneous queries for user " + client_info.current_user
                        + ". Current: " + toString(user_process_list->second.queries.size())
                        + ", maximum: " + settings.max_concurrent_queries_for_user.toString(),
                        ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);

                auto running_query = user_process_list->second.queries.find(client_info.current_query_id);

                if (running_query != user_process_list->second.queries.end())
                {
                    if (!settings.replace_running_query)
                        throw Exception("Query with id = " + client_info.current_query_id + " is already running.",
                            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

                    /// Ask queries to cancel. They will check this flag.
                    running_query->second->is_killed.store(true, std::memory_order_relaxed);

                    const auto replace_running_query_max_wait_ms = settings.replace_running_query_max_wait_ms.totalMilliseconds();
                    if (!replace_running_query_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms),
                        [&]
                        {
                            running_query = user_process_list->second.queries.find(client_info.current_query_id);
                            if (running_query == user_process_list->second.queries.end())
                                return true;
                            running_query->second->is_killed.store(true, std::memory_order_relaxed);
                            return false;
                        }))
                    {
                        throw Exception("Query with id = " + client_info.current_query_id + " is already running and can't be stopped",
                            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
                    }
                 }
            }
        }

        /// Check other users running query with our query_id
        for (const auto & user_process_list : user_to_queries)
        {
            if (user_process_list.first == client_info.current_user)
                continue;
            if (auto running_query = user_process_list.second.queries.find(client_info.current_query_id); running_query != user_process_list.second.queries.end())
                throw Exception("Query with id = " + client_info.current_query_id + " is already running by user " + user_process_list.first,
                    ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
        }

        auto process_it = processes.emplace(processes.end(),
            query_, client_info, settings.max_memory_usage, settings.memory_tracker_fault_probability, priorities.insert(settings.priority));

        res = std::make_shared<Entry>(*this, process_it);

        process_it->query_context = &query_context;

        if (!client_info.current_query_id.empty())
        {
            ProcessListForUser & user_process_list = user_to_queries[client_info.current_user];
            user_process_list.queries.emplace(client_info.current_query_id, &res->get());

            process_it->setUserProcessList(&user_process_list);

            /// Limits are only raised (to be more relaxed) or set to something instead of zero,
            ///  because settings for different queries will interfere each other:
            ///  setting from one query effectively sets values for all other queries.

            /// Track memory usage for all simultaneously running queries.
            /// You should specify this value in configuration for default profile,
            ///  not for specific users, sessions or queries,
            ///  because this setting is effectively global.
            total_memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage_for_all_queries);
            total_memory_tracker.setDescription("(total)");

            /// Track memory usage for all simultaneously running queries from single user.
            user_process_list.user_memory_tracker.setParent(&total_memory_tracker);
            user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage_for_user);
            user_process_list.user_memory_tracker.setDescription("(for user)");

            /// Actualize thread group info
            if (auto thread_group = CurrentThread::getGroup())
            {
                std::lock_guard lock_thread_group(thread_group->mutex);
                thread_group->performance_counters.setParent(&user_process_list.user_performance_counters);
                thread_group->memory_tracker.setParent(&user_process_list.user_memory_tracker);
                thread_group->query = process_it->query;

                /// Set query-level memory trackers
                thread_group->memory_tracker.setOrRaiseHardLimit(process_it->max_memory_usage);
                thread_group->memory_tracker.setOrRaiseProfilerLimit(settings.memory_profiler_step);
                thread_group->memory_tracker.setProfilerStep(settings.memory_profiler_step);
                thread_group->memory_tracker.setDescription("(for query)");
                if (process_it->memory_tracker_fault_probability)
                    thread_group->memory_tracker.setFaultProbability(process_it->memory_tracker_fault_probability);

                /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
                ///  since allocation and deallocation could happen in different threads

                process_it->thread_group = std::move(thread_group);
            }

            if (!user_process_list.user_throttler)
            {
                if (settings.max_network_bandwidth_for_user)
                    user_process_list.user_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_user, total_network_throttler);
                else if (settings.max_network_bandwidth_for_all_users)
                    user_process_list.user_throttler = total_network_throttler;
            }
        }

        if (!total_network_throttler && settings.max_network_bandwidth_for_all_users)
        {
            total_network_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_all_users);
        }
    }

    return res;
}


ProcessListEntry::~ProcessListEntry()
{
    /// Destroy all streams to avoid long lock of ProcessList
    it->releaseQueryStreams();

    std::lock_guard lock(parent.mutex);

    String user = it->getClientInfo().current_user;
    String query_id = it->getClientInfo().current_query_id;

    const QueryStatus * process_list_element_ptr = &*it;

    /// This removes the memory_tracker of one request.
    parent.processes.erase(it);

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(&Logger::get("ProcessList"), "Logical error: cannot find user in ProcessList");
        std::terminate();
    }

    ProcessListForUser & user_process_list = user_process_list_it->second;

    bool found = false;

    if (auto running_query = user_process_list.queries.find(query_id); running_query != user_process_list.queries.end())
    {
        if (running_query->second == process_list_element_ptr)
        {
            user_process_list.queries.erase(running_query->first);
            found = true;
        }
    }

    if (!found)
    {
        LOG_ERROR(&Logger::get("ProcessList"), "Logical error: cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }
    parent.have_space.notify_all();

    /// If there are no more queries for the user, then we will reset memory tracker and network throttler.
    if (user_process_list.queries.empty())
        user_process_list.resetTrackers();

    /// This removes memory_tracker for all requests. At this time, no other memory_trackers live.
    if (parent.processes.size() == 0)
    {
        /// Reset MemoryTracker, similarly (see above).
        parent.total_memory_tracker.logPeakMemoryUsage();
        parent.total_memory_tracker.reset();
        parent.total_network_throttler.reset();
    }
}


QueryStatus::QueryStatus(
    const String & query_,
    const ClientInfo & client_info_,
    size_t max_memory_usage_,
    double memory_tracker_fault_probability_,
    QueryPriorities::Handle && priority_handle_)
    :
    query(query_),
    client_info(client_info_),
    priority_handle(std::move(priority_handle_)),
    num_queries_increment{CurrentMetrics::Query},
    max_memory_usage(max_memory_usage_),
    memory_tracker_fault_probability(memory_tracker_fault_probability_)
{
}

QueryStatus::~QueryStatus() = default;

void QueryStatus::setQueryStreams(const BlockIO & io)
{
    std::lock_guard lock(query_streams_mutex);

    query_stream_in = io.in;
    query_stream_out = io.out;
    query_streams_status = QueryStreamsStatus::Initialized;
}

void QueryStatus::releaseQueryStreams()
{
    BlockInputStreamPtr in;
    BlockOutputStreamPtr out;

    {
        std::lock_guard lock(query_streams_mutex);

        query_streams_status = QueryStreamsStatus::Released;
        in = std::move(query_stream_in);
        out = std::move(query_stream_out);
    }

    /// Destroy streams outside the mutex lock
}

bool QueryStatus::streamsAreReleased()
{
    std::lock_guard lock(query_streams_mutex);

    return query_streams_status == QueryStreamsStatus::Released;
}

bool QueryStatus::tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const
{
    std::lock_guard lock(query_streams_mutex);

    if (query_streams_status != QueryStreamsStatus::Initialized)
        return false;

    in = query_stream_in;
    out = query_stream_out;
    return true;
}

CancellationCode QueryStatus::cancelQuery(bool kill)
{
    /// Streams are destroyed, and ProcessListElement will be deleted from ProcessList soon. We need wait a little bit
    if (streamsAreReleased())
        return CancellationCode::CancelSent;

    BlockInputStreamPtr input_stream;
    BlockOutputStreamPtr output_stream;

    if (tryGetQueryStreams(input_stream, output_stream))
    {
        if (input_stream)
        {
            input_stream->cancel(kill);
            return CancellationCode::CancelSent;
        }
        return CancellationCode::CancelCannotBeSent;
    }
    /// Query is not even started
    is_killed.store(true);
    return CancellationCode::CancelSent;
}


void QueryStatus::setUserProcessList(ProcessListForUser * user_process_list_)
{
    user_process_list = user_process_list_;
}


ThrottlerPtr QueryStatus::getUserNetworkThrottler()
{
    if (!user_process_list)
        return {};
    return user_process_list->user_throttler;
}


QueryStatus * ProcessList::tryGetProcessListElement(const String & current_query_id, const String & current_user)
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


CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill)
{
    std::lock_guard lock(mutex);

    QueryStatus * elem = tryGetProcessListElement(current_query_id, current_user);

    if (!elem)
        return CancellationCode::NotFound;

    return elem->cancelQuery(kill);
}


void ProcessList::killAllQueries()
{
    std::lock_guard lock(mutex);

    for (auto & process : processes)
        process.cancelQuery(true);
}


QueryStatusInfo QueryStatus::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    QueryStatusInfo res;

    res.query             = query;
    res.client_info       = client_info;
    res.elapsed_seconds   = watch.elapsedSeconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.read_rows         = progress_in.read_rows;
    res.read_bytes        = progress_in.read_bytes;
    res.total_rows        = progress_in.total_rows_to_read;

    /// TODO: Use written_rows and written_bytes when real time progress is implemented
    res.written_rows      = progress_out.read_rows;
    res.written_bytes     = progress_out.read_bytes;

    if (thread_group)
    {
        res.memory_usage = thread_group->memory_tracker.get();
        res.peak_memory_usage = thread_group->memory_tracker.getPeak();

        if (get_thread_list)
        {
            std::lock_guard lock(thread_group->mutex);
            res.thread_ids = thread_group->thread_ids;
        }

        if (get_profile_events)
            res.profile_counters = std::make_shared<ProfileEvents::Counters>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
    }

    if (get_settings && query_context)
        res.query_settings = std::make_shared<Settings>(query_context->getSettingsRef());

    return res;
}


ProcessList::Info ProcessList::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    Info per_query_infos;

    std::lock_guard lock(mutex);

    per_query_infos.reserve(processes.size());
    for (const auto & process : processes)
        per_query_infos.emplace_back(process.getInfo(get_thread_list, get_profile_events, get_settings));

    return per_query_infos;
}


ProcessListForUser::ProcessListForUser() = default;


}
