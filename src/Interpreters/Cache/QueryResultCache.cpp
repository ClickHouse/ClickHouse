#include "Interpreters/Cache/QueryResultCache.h"

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <base/defines.h> /// chassert


namespace ProfileEvents
{
    extern const Event QueryResultCacheHits;
    extern const Event QueryResultCacheMisses;
};

namespace DB
{

namespace
{

struct HasNonDeterministicFunctionsMatcher
{
    struct Data
    {
        const ContextPtr context;
        bool has_non_deterministic_functions = false;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.has_non_deterministic_functions)
            return;

        if (const auto * function = node->as<ASTFunction>())
        {
            const auto func = FunctionFactory::instance().tryGet(function->name, data.context);
            if (func && !func->isDeterministic())
                data.has_non_deterministic_functions = true;
        }
    }
};

using HasNonDeterministicFunctionsVisitor = InDepthNodeVisitor<HasNonDeterministicFunctionsMatcher, true>;

}

bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

namespace
{

class RemoveQueryResultCacheSettingsMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * set_clause = ast->as<ASTSetQuery>())
        {
            chassert(!set_clause->is_standalone);

            auto is_query_result_cache_related_setting = [](const auto & change)
            {
                return change.name.starts_with("enable_experimental_query_result_cache")
                    || change.name.starts_with("query_result_cache");
            };

            std::erase_if(set_clause->changes, is_query_result_cache_related_setting);
        }
    }

    /// TODO further improve AST cleanup, e.g. remove SETTINGS clause completely if it is empty
    /// E.g. SELECT 1 SETTINGS enable_experimental_query_result_cache = true
    /// and  SELECT 1;
    /// currently don't match.
};

using RemoveQueryResultCacheSettingsVisitor = InDepthNodeVisitor<RemoveQueryResultCacheSettingsMatcher, true>;

/// Consider
///   (1) SET enable_experimental_query_result_cache = true;
///       SELECT expensiveComputation(...) SETTINGS max_threads = 64, query_result_cache_ttl = 300;
///       SET enable_experimental_query_result_cache = false;
/// and
///   (2) SELECT expensiveComputation(...) SETTINGS max_threads = 64, enable_experimental_query_result_cache_passive_usage = true;
/// The SELECT queries in (1) and (2) are basically the same and the user expects that the second invocation is served from the query result
/// cache. However, query results are indexed by their query ASTs and therefore no result will be found. Insert and retrieval behave overall
/// more natural if settings related to the query result cache are erased from the AST key. Note that at this point the settings themselves
/// have been parsed already, they are not lost or discarded.
ASTPtr removeQueryResultCacheSettings(ASTPtr ast)
{
    ASTPtr transformed_ast = ast->clone();

    RemoveQueryResultCacheSettingsMatcher::Data visitor_data;
    RemoveQueryResultCacheSettingsVisitor(visitor_data).visit(transformed_ast);

    return transformed_ast;
}

}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    Block header_, const std::optional<String> & username_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_)
    : ast(removeQueryResultCacheSettings(ast_))
    , header(header_)
    , username(username_)
    , expires_at(expires_at_)
{
}

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast->getTreeHash() == other.ast->getTreeHash();
}

String QueryResultCache::Key::queryStringFromAst() const
{
    WriteBufferFromOwnString buf;
    IAST::FormatSettings format_settings(buf, /*one_line*/ true);
    format_settings.show_secrets = false;
    ast->format(format_settings);
    return buf.str();
}

size_t QueryResultCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.ast->getTreeHash());
    auto res = hash.get64();
    return res;
}

size_t QueryResultCache::QueryResult::sizeInBytes() const
{
    size_t res = 0;
    for (const auto & chunk : *chunks)
        res += chunk.allocatedBytes();
    return res;
};

namespace
{

auto is_stale = [](const QueryResultCache::Key & key)
{
    return (key.expires_at < std::chrono::system_clock::now());
};

}

QueryResultCache::Writer::Writer(std::mutex & mutex_, Cache & cache_, const Key & key_,
    size_t & cache_size_in_bytes_, size_t max_cache_size_in_bytes_,
    size_t max_cache_entries_,
    size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_runtime_)
    : mutex(mutex_)
    , cache(cache_)
    , key(key_)
    , cache_size_in_bytes(cache_size_in_bytes_)
    , max_cache_size_in_bytes(max_cache_size_in_bytes_)
    , max_cache_entries(max_cache_entries_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
{
    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        skip_insert = true; /// Key already contained in cache and did not expire yet --> don't replace it
}

void QueryResultCache::Writer::buffer(Chunk && partial_query_result)
{
    if (skip_insert)
        return;

    auto & chunks = query_result.chunks;

    chunks->emplace_back(std::move(partial_query_result));

    new_entry_size_in_bytes += chunks->back().allocatedBytes();
    new_entry_size_in_rows += chunks->back().getNumRows();

    if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
    {
        chunks->clear(); /// eagerly free some space
        skip_insert = true;
    }
}

void QueryResultCache::Writer::finalizeWrite()
{
    if (skip_insert)
        return;

    if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - query_start_time) < min_query_runtime)
        return;

    std::lock_guard lock(mutex);

    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        return; /// same check as in ctor because a parallel Writer could have inserted the current key in the meantime

    auto sufficient_space_in_cache = [this]() TSA_REQUIRES(mutex)
    {
        return (cache_size_in_bytes + new_entry_size_in_bytes <= max_cache_size_in_bytes) && (cache.size() + 1 <= max_cache_entries);
    };

    if (!sufficient_space_in_cache())
    {
        size_t removed_items = 0;
        /// Remove stale entries
        for (auto it = cache.begin(); it != cache.end();)
            if (is_stale(it->first))
            {
                cache_size_in_bytes -= it->second.sizeInBytes();
                it = cache.erase(it);
                ++removed_items;
            }
            else
                ++it;
        LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "Removed {} stale entries", removed_items);
    }

    /// Insert or replace if enough space
    if (sufficient_space_in_cache())
    {
        cache_size_in_bytes += query_result.sizeInBytes();
        if (auto it = cache.find(key); it != cache.end())
            cache_size_in_bytes -= it->second.sizeInBytes(); // key replacement

        cache[key] = std::move(query_result);
        LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "Stored result of query {}", key.queryStringFromAst());
    }
}

QueryResultCache::Reader::Reader(const Cache & cache_, const Key & key, size_t & cache_size_in_bytes_, const std::lock_guard<std::mutex> &)
{
    auto it = cache_.find(key);

    if (it == cache_.end())
    {
        LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "No entry found for query {}", key.queryStringFromAst());
        return;
    }

    if (it->first.username.has_value() && it->first.username != key.username)
    {
        LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "Inaccessible entry found for query {}", key.queryStringFromAst());
        return;
    }

    if (is_stale(it->first))
    {
        cache_size_in_bytes_ -= it->second.sizeInBytes();
        const_cast<Cache &>(cache_).erase(it);
        LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "Stale entry found and removed for query {}", key.queryStringFromAst());
        return;
    }

    pipe = Pipe(std::make_shared<SourceFromChunks>(it->first.header, it->second.chunks));
    LOG_TRACE(&Poco::Logger::get("QueryResultCache"), "Entry found for query {}", key.queryStringFromAst());
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
    chassert(!pipe.empty()); // cf. hasCacheEntryForKey()
    return std::move(pipe);
}

QueryResultCache::QueryResultCache(size_t max_cache_size_in_bytes_, size_t max_cache_entries_, size_t max_cache_entry_size_in_bytes_, size_t max_cache_entry_size_in_rows_)
    : max_cache_size_in_bytes(max_cache_size_in_bytes_)
    , max_cache_entries(max_cache_entries_)
    , max_cache_entry_size_in_bytes(max_cache_entry_size_in_bytes_)
    , max_cache_entry_size_in_rows(max_cache_entry_size_in_rows_)
{
}

QueryResultCache::Reader QueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return Reader(cache, key, cache_size_in_bytes, lock);
}

QueryResultCache::Writer QueryResultCache::createWriter(const Key & key, std::chrono::milliseconds min_query_runtime)
{
    std::lock_guard lock(mutex);
    return Writer(mutex, cache, key, cache_size_in_bytes, max_cache_size_in_bytes, max_cache_entries, max_cache_entry_size_in_bytes, max_cache_entry_size_in_rows, min_query_runtime);
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
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

}
