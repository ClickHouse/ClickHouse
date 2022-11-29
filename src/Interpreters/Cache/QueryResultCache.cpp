#include "Interpreters/Cache/QueryResultCache.h"

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>

namespace DB
{

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast->getTreeHash() == other.ast->getTreeHash()
        && username == other.username
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
    hash.update(key.username);
    hash.update(key.partition_key);
    auto res = hash.get64();
    return res;
}

namespace {

auto is_stale = [](const QueryResultCache::Key & key)
{
    return (key.expires_at < std::chrono::system_clock::now());
};

}

QueryResultCache::Writer::Writer(Cache & cache_, std::mutex & mutex_, const Key & key_, size_t max_cache_size_, size_t & cache_size_, size_t max_entry_size_)
    : cache(cache_)
    , mutex(mutex_)
    , key(key_)
    , max_cache_size(max_cache_size_)
    , cache_size(cache_size_)
    , max_entry_size(max_entry_size_)
    , skip_insert(false)
    , entry_size(0)
{
    std::lock_guard lock(mutex);
    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        skip_insert = true; /// Do nothing if key exists in cache and it is not expired
}

QueryResultCache::Writer::~Writer()
try
{
    if (skip_insert)
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
    const size_t new_entry_size = entry->allocatedBytes();

    std::lock_guard lock(mutex);

    if (auto it = cache.find(key); it != cache.end() && !is_stale(it->first))
        return; /// same check as in ctor

    /// In case of insufficient space, remove stale entries
    if (cache_size + new_entry_size > max_cache_size)
    {
        for (auto it = cache.begin(); it != cache.end();)
            if (is_stale(it->first))
            {
                cache_size -= it->second->allocatedBytes();
                it = cache.erase(it);
            }
            else
                ++it;
    }

    /// Insert or replace if enough space
    if (cache_size + new_entry_size < max_cache_size)
    {
        cache_size += entry->allocatedBytes();
        if (auto it = cache.find(key); it != cache.end())
            cache_size -= it->second->allocatedBytes(); /// key replacement

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

    entry_size += chunks.back().allocatedBytes();
    if (entry_size > max_entry_size)
        skip_insert = true;

}

QueryResultCache::Reader::Reader(const Cache & cache_, std::mutex & mutex_, const Key & key)
{
    std::lock_guard lock(mutex_);

    auto it = cache_.find(key);

    if (it == cache_.end())
    {
        LOG_DEBUG(&Poco::Logger::get("QueryResultCache"), "No entry found for query {}", key.queryStringFromAst());
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

bool QueryResultCache::Reader::hasEntryForKey() const
{
    return !pipe.empty();
}

Pipe && QueryResultCache::Reader::getPipe()
{
    assert(hasEntryForKey());
    return std::move(pipe);
}

QueryResultCache::QueryResultCache(size_t max_cache_size_)
    : max_cache_size(max_cache_size_)
    , cache_size(0)
{
}

QueryResultCache::Reader QueryResultCache::createReader(const Key & key)
{
    return Reader(cache, mutex, key);
}

QueryResultCache::Writer QueryResultCache::createWriter(const Key & key, size_t max_entry_size)
{
    return Writer(cache, mutex, key, max_cache_size, cache_size, max_entry_size);
}

void QueryResultCache::reset()
{
    std::lock_guard lock(mutex);
    cache.clear();
    times_executed.clear();
    cache_size = 0;
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
