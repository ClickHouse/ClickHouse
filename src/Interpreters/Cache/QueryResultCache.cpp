#include "Interpreters/Cache/QueryResultCache.h"

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

namespace ProfileEvents
{
    extern const Event QueryResultCacheHits;
    extern const Event QueryResultCacheMisses;
};

namespace DB
{

bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    if (const auto * function = ast->as<ASTFunction>())
    {
        const FunctionFactory & function_factory = FunctionFactory::instance();
        if (const FunctionOverloadResolverPtr resolver = function_factory.tryGet(function->name, context))
        {
            if (!resolver->isDeterministic())
                return true;
        }
    }

    bool has_non_cacheable_functions = false;
    for (const auto & child : ast->children)
        has_non_cacheable_functions |= astContainsNonDeterministicFunctions(child, context);

    return has_non_cacheable_functions;
}

namespace
{

class RemoveQueryResultCacheSettingsMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTSetQuery>())
            visit(*func, ast, data);
    }

    static void visit(ASTSetQuery & func, ASTPtr &, const Data &)
    {
        assert(!func.is_standalone);

        auto is_query_result_cache_related_setting = [](const auto & change)
        {
            return change.name.starts_with("enable_experimental_query_result_cache")
                || change.name.starts_with("query_result_cache");
        };

        std::erase_if(func.changes, is_query_result_cache_related_setting);
    }

    /// TODO further improve AST cleanup, e.g. remove SETTINGS clause completely if it is empty
    /// E.g. SELECT 1 SETTINGS enable_experimental_query_result_cache = true
    /// and  SELECT 1;
    /// currently don't match.
};

using RemoveQueryResultCacheSettingsVisitor = InDepthNodeVisitor<RemoveQueryResultCacheSettingsMatcher, true>;

/// Consider
///   (1) SET enable_experimental_query_result_cache = true;
///       SELECT expensiveComputation(...) SETTINGS max_threads = 64, query_result_cache_keep_seconds_alive = 300;
///       SET enable_experimental_query_result_cache = false;
/// and
///   (2) SELECT expensiveComputation(...) SETTINGS max_threads = 64, enable_experimental_query_result_cache_passive_usage = true;
/// The SELECT query in (1) and (2) is basically the same and the user expects to find the result in the query result cache. However, query
/// results are indexed by the AST of their SELECT queries and no result will be found. Insert and retrieval behave overall more natural if
/// settings related to the query result cache are erased from the AST key. Note that at this point the settings themselves have been parsed
/// already, they are not lost or discarded.
ASTPtr removeQueryResultCacheSettings(ASTPtr ast)
{
    ASTPtr transformed_ast = ast->clone();

    RemoveQueryResultCacheSettingsMatcher::Data visitor_data{};
    RemoveQueryResultCacheSettingsVisitor visitor(visitor_data);
    visitor.visit(transformed_ast);

    return transformed_ast;
}

}

QueryResultCache::Key::Key(
    ASTPtr ast_, String partition_key_,
    Block header_, const std::optional<String> & username_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_)
    : ast(removeQueryResultCacheSettings(ast_))
    , partition_key(partition_key_)
    , header(header_)
    , username(username_)
    , expires_at(expires_at_)
{
}

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast->getTreeHash() == other.ast->getTreeHash()
        && partition_key == other.partition_key;
}

String QueryResultCache::Key::queryStringFromAst() const
{
    WriteBufferFromOwnString buf;
    IAST::FormatSettings format_settings(buf, /*one_line*/ true);
    ast->format(format_settings);
    return buf.str();
}

size_t QueryResultCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.ast->getTreeHash());
    hash.update(key.partition_key);
    auto res = hash.get64();
    return res;
}

namespace
{

auto is_stale = [](const QueryResultCache::Key & key)
{
    return (key.expires_at < std::chrono::system_clock::now());
};

}

QueryResultCache::Writer::Writer(std::mutex & mutex_, Cache & cache_, const Key & key_,
    size_t & cache_size_in_bytes_, size_t max_cache_size_in_bytes_,
    size_t max_entries_,
    size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_duration_)
    : mutex(mutex_)
    , cache(cache_)
    , key(key_)
    , cache_size_in_bytes(cache_size_in_bytes_)
    , max_cache_size_in_bytes(max_cache_size_in_bytes_)
    , max_entries(max_entries_)
    , new_entry_size_in_bytes(0)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , new_entry_size_in_rows(0)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , query_start_time(std::chrono::system_clock::now())
    , min_query_duration(min_query_duration_)
    , skip_insert(false)
{
    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        skip_insert = true; /// Do nothing if key exists in cache and it is not expired
}

QueryResultCache::Writer::~Writer()
try
{
    if (skip_insert)
        return;

    if (auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - query_start_time); query_duration < min_query_duration)
        return;

    auto to_single_chunk = [](const Chunks & chunks_) -> Chunk
    {
        if (chunks_.empty())
            return {};

        Chunk res = chunks_[0].clone();
        for (size_t i = 1; i != chunks_.size(); ++i)
            res.append(chunks_[i]);
        return res;
    };

    auto entry = std::make_shared<Chunk>(to_single_chunk(chunks));
    new_entry_size_in_bytes = entry->allocatedBytes();

    std::lock_guard lock(mutex);

    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        return; /// same check as in ctor

    auto sufficient_space_in_cache = [this]() TSA_REQUIRES(mutex)
    {
        return (cache_size_in_bytes + new_entry_size_in_bytes <= max_cache_size_in_bytes) && (cache.size() + 1 <= max_entries);
    };

    if (!sufficient_space_in_cache())
    {
        size_t removed_items = 0;
        /// Remove stale entries
        for (auto it = cache.begin(); it != cache.end();)
            if (is_stale(it->first))
            {
                cache_size_in_bytes -= it->second->allocatedBytes();
                it = cache.erase(it);
                ++removed_items;
            }
            else
                ++it;
        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "Removed {} stale entries", removed_items);
    }

    /// Insert or replace if enough space
    if (sufficient_space_in_cache())
    {
        cache_size_in_bytes += entry->allocatedBytes();
        if (auto it = cache.find(key); it != cache.end())
            cache_size_in_bytes -= it->second->allocatedBytes(); /// key replacement

        /// cache[key] = entry; /// does no replacement for unclear reasons
        cache.erase(key);
        cache[key] = entry;

        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "Stored result of query {}", key.queryStringFromAst());
    }
}
catch (const std::exception &)
{
}

void QueryResultCache::Writer::buffer(Chunk && chunk)
{
    if (skip_insert)
        return;

    chunks.emplace_back(std::move(chunk));

    new_entry_size_in_bytes += chunks.back().allocatedBytes();
    new_entry_size_in_rows += chunks.back().getNumRows();

    if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
        skip_insert = true;
}

QueryResultCache::Reader::Reader(const Cache & cache_, const Key & key)
{
    auto it = cache_.find(key);

    if (it == cache_.end())
    {
        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "No entry found for query {}", key.queryStringFromAst());
        return;
    }

    if (it->first.username.has_value() && it->first.username != key.username)
    {
        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "Inaccessible entry found for query {}", key.queryStringFromAst());
        return;
    }

    if (it->first.expires_at < std::chrono::system_clock::now())
    {
        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "Stale entry found for query {}", key.queryStringFromAst());
        return;
    }

    LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "Entry found for query {}", key.queryStringFromAst());

    pipe = Pipe(std::make_shared<SourceFromSingleChunk>(key.header, it->second->clone()));
}

bool QueryResultCache::Reader::hasCacheEntryForKey() const
{
    bool res = !pipe.empty();

    if (res)
        ProfileEvents::increment(ProfileEvents::QueryResultCacheHits);
    else
        ProfileEvents::increment(ProfileEvents::QueryResultCacheMisses);

    return res;
}

Pipe && QueryResultCache::Reader::getPipe()
{
    assert(!pipe.empty()); // cf. hasCacheEntryForKey()
    return std::move(pipe);
}

QueryResultCache::QueryResultCache(size_t max_cache_size_in_bytes_, size_t max_cache_entries_, size_t max_cache_entry_size_in_bytes_, size_t max_cache_entry_size_in_rows_)
    : cache_size_in_bytes(0)
    , max_cache_size_in_bytes(max_cache_size_in_bytes_)
    , max_cache_entries(max_cache_entries_)
    , max_cache_entry_size_in_bytes(max_cache_entry_size_in_bytes_)
    , max_cache_entry_size_in_rows(max_cache_entry_size_in_rows_)
{
}

QueryResultCache::Reader QueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return Reader(cache, key);
}

QueryResultCache::Writer QueryResultCache::createWriter(const Key & key, std::chrono::milliseconds min_query_duration)
{
    std::lock_guard lock(mutex);
    return Writer(mutex, cache, key, cache_size_in_bytes, max_cache_size_in_bytes, max_cache_entries, max_cache_entry_size_in_bytes, max_cache_entry_size_in_rows, min_query_duration);
}

void QueryResultCache::reset()
{
    std::lock_guard lock(mutex);
    cache.clear();
    times_executed.clear();
    cache_size_in_bytes = 0;
}

size_t QueryResultCache::recordQueryRun(const Key & key)
{
    static constexpr size_t TIMES_EXECUTED_MAX_SIZE = 10'000;

    std::lock_guard times_executed_lock(mutex);
    size_t times = ++times_executed[key];
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

}
