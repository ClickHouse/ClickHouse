#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/toOneLineQuery.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>
#include <chrono>


namespace CurrentMetrics
{
    extern const Metric Query;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
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


ProcessList::EntryPtr ProcessList::insert(const String & query_, const IAST * ast, ContextPtr query_context)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    if (client_info.current_query_id.empty())
        throw Exception("Query id cannot be empty", ErrorCodes::LOGICAL_ERROR);

    bool is_unlimited_query = isUnlimitedQuery(ast);

    {
        std::unique_lock lock(mutex);
        IAST::QueryKind query_kind = ast->getQueryKind();

        const auto queue_max_wait_ms = settings.queue_max_wait_ms.totalMilliseconds();
        if (!is_unlimited_query && max_size && processes.size() >= max_size)
        {
            if (queue_max_wait_ms)
                LOG_WARNING(&Poco::Logger::get("ProcessList"), "Too many simultaneous queries, will wait {} ms.", queue_max_wait_ms);
            if (!queue_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(queue_max_wait_ms), [&]{ return processes.size() < max_size; }))
                throw Exception("Too many simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
        }

        if (!is_unlimited_query)
        {
            QueryAmount amount = getQueryKindAmount(query_kind);
            if (max_insert_queries_amount && query_kind == IAST::QueryKind::Insert && amount >= max_insert_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous insert queries. Maximum: {}, current: {}",
                                max_insert_queries_amount, amount);
            if (max_select_queries_amount && query_kind == IAST::QueryKind::Select && amount >= max_select_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous select queries. Maximum: {}, current: {}",
                                max_select_queries_amount, amount);
        }

        {
            /**
             * `max_size` check above is controlled by `max_concurrent_queries` server setting and is a "hard" limit for how many
             * queries the server can process concurrently. It is configured at startup. When the server is overloaded with queries and the
             * hard limit is reached it is impossible to connect to the server to run queries for investigation.
             *
             * With `max_concurrent_queries_for_all_users` it is possible to configure an additional, runtime configurable, limit for query concurrency.
             * Usually it should be configured just once for `default_profile` which is inherited by all users. DBAs can override
             * this setting when connecting to ClickHouse, or it can be configured for a DBA profile to have a value greater than that of
             * the default profile (or 0 for unlimited).
             *
             * One example is to set `max_size=X`, `max_concurrent_queries_for_all_users=X-10` for default profile,
             * and `max_concurrent_queries_for_all_users=0` for DBAs or accounts that are vital for ClickHouse operations (like metrics
             * exporters).
             *
             * Another creative example is to configure `max_concurrent_queries_for_all_users=50` for "analyst" profiles running adhoc queries
             * and `max_concurrent_queries_for_all_users=100` for "customer facing" services. This way "analyst" queries will be rejected
             * once is already processing 50+ concurrent queries (including analysts or any other users).
             */

            if (!is_unlimited_query && settings.max_concurrent_queries_for_all_users
                && processes.size() >= settings.max_concurrent_queries_for_all_users)
                throw Exception(
                    "Too many simultaneous queries for all users. Current: " + toString(processes.size())
                    + ", maximum: " + settings.max_concurrent_queries_for_all_users.toString(),
                    ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
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

        auto user_process_list_it = user_to_queries.find(client_info.current_user);
        if (user_process_list_it == user_to_queries.end())
            user_process_list_it = user_to_queries.emplace(client_info.current_user, this).first;
        ProcessListForUser & user_process_list = user_process_list_it->second;

        /// Actualize thread group info
        auto thread_group = CurrentThread::getGroup();
        if (thread_group)
        {
            std::lock_guard lock_thread_group(thread_group->mutex);
            thread_group->performance_counters.setParent(&user_process_list.user_performance_counters);
            thread_group->memory_tracker.setParent(&user_process_list.user_memory_tracker);
            thread_group->query = query_;
            thread_group->one_line_query = toOneLineQuery(query_);
            thread_group->normalized_query_hash = normalizedQueryHash<false>(query_);

            /// Set query-level memory trackers
            thread_group->memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage);
            thread_group->memory_tracker.setSoftLimit(settings.memory_overcommit_ratio_denominator);

            if (query_context->hasTraceCollector())
            {
                /// Set up memory profiling
                thread_group->memory_tracker.setProfilerStep(settings.memory_profiler_step);
                thread_group->memory_tracker.setSampleProbability(settings.memory_profiler_sample_probability);
            }

            thread_group->memory_tracker.setDescription("(for query)");
            if (settings.memory_tracker_fault_probability)
                thread_group->memory_tracker.setFaultProbability(settings.memory_tracker_fault_probability);

            thread_group->memory_tracker.setOvercommitWaitingTime(settings.memory_usage_overcommit_max_wait_microseconds);

            /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
            ///  since allocation and deallocation could happen in different threads
        }

        auto process_it = processes.emplace(processes.end(),
            query_context, query_, client_info, priorities.insert(settings.priority), std::move(thread_group), query_kind);

        increaseQueryKindAmount(query_kind);

        res = std::make_shared<Entry>(*this, process_it);

        process_it->setUserProcessList(&user_process_list);

        user_process_list.queries.emplace(client_info.current_query_id, &res->get());

        /// Track memory usage for all simultaneously running queries from single user.
        user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage_for_user);
        user_process_list.user_memory_tracker.setSoftLimit(settings.memory_overcommit_ratio_denominator_for_user);
        user_process_list.user_memory_tracker.setDescription("(for user)");

        if (!user_process_list.user_throttler)
        {
            if (settings.max_network_bandwidth_for_user)
                user_process_list.user_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_user, total_network_throttler);
            else if (settings.max_network_bandwidth_for_all_users)
                user_process_list.user_throttler = total_network_throttler;
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
    std::lock_guard lock(parent.mutex);

    String user = it->getClientInfo().current_user;
    String query_id = it->getClientInfo().current_query_id;
    IAST::QueryKind query_kind = it->query_kind;

    const QueryStatus * process_list_element_ptr = &*it;

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(&Poco::Logger::get("ProcessList"), "Logical error: cannot find user in ProcessList");
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

    /// This removes the memory_tracker of one request.
    parent.processes.erase(it);

    if (!found)
    {
        LOG_ERROR(&Poco::Logger::get("ProcessList"), "Logical error: cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }

    parent.decreaseQueryKindAmount(query_kind);

    parent.have_space.notify_all();

    /// If there are no more queries for the user, then we will reset memory tracker and network throttler.
    if (user_process_list.queries.empty())
        user_process_list.resetTrackers();

    /// Reset throttler, similarly (see above).
    if (parent.processes.empty())
        parent.total_network_throttler.reset();
}


QueryStatus::QueryStatus(
    ContextPtr context_,
    const String & query_,
    const ClientInfo & client_info_,
    QueryPriorities::Handle && priority_handle_,
    ThreadGroupStatusPtr && thread_group_,
    IAST::QueryKind query_kind_)
    : WithContext(context_)
    , query(query_)
    , client_info(client_info_)
    , thread_group(std::move(thread_group_))
    , priority_handle(std::move(priority_handle_))
    , query_kind(query_kind_)
    , num_queries_increment(CurrentMetrics::Query)
{
    auto settings = getContext()->getSettings();
    limits.max_execution_time = settings.max_execution_time;
    overflow_mode = settings.timeout_overflow_mode;
}

QueryStatus::~QueryStatus()
{
    assert(executors.empty());

    if (auto * memory_tracker = getMemoryTracker())
    {
        if (user_process_list)
            user_process_list->user_overcommit_tracker.onQueryStop(memory_tracker);
        if (auto shared_context = getContext())
            shared_context->getGlobalOvercommitTracker()->onQueryStop(memory_tracker);
    }
}

CancellationCode QueryStatus::cancelQuery(bool)
{
    if (is_killed.load())
        return CancellationCode::CancelSent;

    is_killed.store(true);

    std::lock_guard lock(executors_mutex);
    for (auto * e : executors)
        e->cancel();

    return CancellationCode::CancelSent;
}

void QueryStatus::addPipelineExecutor(PipelineExecutor * e)
{
    std::lock_guard lock(executors_mutex);
    assert(std::find(executors.begin(), executors.end(), e) == executors.end());
    executors.push_back(e);
}

void QueryStatus::removePipelineExecutor(PipelineExecutor * e)
{
    std::lock_guard lock(executors_mutex);
    assert(std::find(executors.begin(), executors.end(), e) != executors.end());
    std::erase_if(executors, [e](PipelineExecutor * x) { return x == e; });
}

bool QueryStatus::checkTimeLimit()
{
    if (is_killed.load())
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);

    return limits.checkTimeLimit(watch, overflow_mode);
}

bool QueryStatus::checkTimeLimitSoft()
{
    if (is_killed.load())
        return false;

    return limits.checkTimeLimit(watch, OverflowMode::BREAK);
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
    QueryStatusInfo res{};

    res.query             = query;
    res.client_info       = client_info;
    res.elapsed_seconds   = watch.elapsedSeconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.is_all_data_sent  = is_all_data_sent.load(std::memory_order_relaxed);
    res.read_rows         = progress_in.read_rows;
    res.read_bytes        = progress_in.read_bytes;
    res.total_rows        = progress_in.total_rows_to_read;

    res.written_rows      = progress_out.written_rows;
    res.written_bytes     = progress_out.written_bytes;

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
            res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
    }

    if (get_settings && getContext())
    {
        res.query_settings = std::make_shared<Settings>(getContext()->getSettings());
        res.current_database = getContext()->getCurrentDatabase();
    }

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


ProcessListForUser::ProcessListForUser(ProcessList * global_process_list)
    : user_overcommit_tracker(global_process_list, this)
{
    user_memory_tracker.setOvercommitTracker(&user_overcommit_tracker);
}


ProcessListForUserInfo ProcessListForUser::getInfo(bool get_profile_events) const
{
    ProcessListForUserInfo res;

    res.memory_usage = user_memory_tracker.get();
    res.peak_memory_usage = user_memory_tracker.getPeak();

    if (get_profile_events)
        res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(user_performance_counters.getPartiallyAtomicSnapshot());

    return res;
}


ProcessList::UserInfo ProcessList::getUserInfo(bool get_profile_events) const
{
    UserInfo per_user_infos;

    std::lock_guard lock(mutex);

    per_user_infos.reserve(user_to_queries.size());

    for (const auto & [user, user_queries] : user_to_queries)
        per_user_infos.emplace(user, user_queries.getInfo(get_profile_events));

    return per_user_infos;
}

void ProcessList::increaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        query_kind_amounts[query_kind] = 1;
    else
        found->second += 1;
}

void ProcessList::decreaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    /// TODO: we could just rebuild the map, as we have saved all query_kind.
    if (found == query_kind_amounts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease before increase on '{}'", query_kind);
    else if (found->second == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease to negative on '{}'", query_kind, found->second);
    else
        found->second -= 1;
}

ProcessList::QueryAmount ProcessList::getQueryKindAmount(const IAST::QueryKind & query_kind) const
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        return 0;
    return found->second;
}

}
