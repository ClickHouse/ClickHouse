#pragma once

#include <memory>
#include <condition_variable>
#include <Common/LRUCache.h>


namespace DB
{
using QueryCachePtr = std::shared_ptr<QueryCache>;

using Data = std::pair<Block, Chunks>;

struct CacheKey
{
    CacheKey(ASTPtr ast_, const Block & header_, const Settings & settings_, std::optional<String> username_)
        : ast(ast_)
        , header(header_)
        , settings(settings_)
        , username(std::move(username_)) {}

    bool operator==(const CacheKey & other) const
    {
        return ast->getTreeHash() == other.ast->getTreeHash()
               && header.getNamesAndTypesList() == other.header.getNamesAndTypesList()
               && settings == other.settings
               && username == other.username;
    }

    ASTPtr ast;
    Block header;
    Settings settings;
    std::optional<String> username;
};

struct CacheKeyHasher
{
    size_t operator()(const CacheKey & key) const
    {
         SipHash hash;
         hash.update(key.ast->getTreeHash());
         hash.update(key.header.getNamesAndTypesList().toString());
         for (const auto & setting : key.settings)
         {
             hash.update(setting.getValueString());
         }
         if (key.username.has_value())
         {
             hash.update(*key.username);
         }
         return hash.get64();
    }
};

struct QueryWeightFunction
{
    size_t operator()(const Data & data) const
    {
        const Block & block = data.first;
        const Chunks & chunks = data.second;

        size_t res = 0;
        for (const auto & chunk : chunks)
        {
            res += chunk.allocatedBytes();
        }
        res += block.allocatedBytes();

        return res;
    }
};

class CacheRemovalScheduler
{
private:
    using Timestamp = std::chrono::time_point<std::chrono::high_resolution_clock>;
    using Duration = std::chrono::high_resolution_clock::duration;
public:
    void scheduleRemoval(Duration duration, CacheKey cache_key)
    {
        std::unique_lock lock(mutex);
        TimedCacheKey timer = {now() + duration, cache_key};
        queue.push(timer);
        auto top = queue.top();
        lock.unlock();
        if (top == timer)
        {
            timer_cv.notify_one();
        }
    }

    template <typename Cache>
    void processRemovalQueue(Cache * query_cache)
    {
        while (process_removal_queue.load())
        {
            std::unique_lock lock(mutex);

            // take the timer with the lowest timestamp from the queue if there is one
            const std::optional<TimedCacheKey> awaited_timer = nextTimer();

            // wake up if either a timer with a lower timestamp than awaited_timer was pushed to the queue, the awaited_timer went off or the server was stoped
            timer_cv.wait_until(lock, awaited_timer.has_value() ? awaited_timer->time : infinite_time);

            // if awaited_timer went off, remove entry from cache
            if (awaited_timer.has_value() && awaited_timer->time <= now())
            {
                lock.unlock();
                queue.pop();
                query_cache->remove(awaited_timer->cache_key);
            }
        }
    }

    void stopProcessingRemovalQueue()
    {
        process_removal_queue.store(false);
        timer_cv.notify_one();
    }


private:
    struct TimedCacheKey
    {
        TimedCacheKey(Timestamp timestamp, CacheKey key)
            : time(timestamp)
            , cache_key(key)
        {}

        bool operator==(const TimedCacheKey& other) const
        {
            return time == other.time;
        }

        bool operator<(const TimedCacheKey& other) const
        {
            return time < other.time;
        }

        Timestamp time;
        CacheKey cache_key;
    };

    std::optional<TimedCacheKey> nextTimer() const
    {
        if (queue.empty())
        {
            return std::nullopt;
        }
        return std::make_optional(queue.top());
    }

    static Timestamp now()
    {
        return std::chrono::high_resolution_clock::now();
    }

    const Timestamp infinite_time = Timestamp::max();
    std::atomic<bool> process_removal_queue{true};
    std::priority_queue<TimedCacheKey> queue;
    std::condition_variable timer_cv;
    std::mutex mutex;
};

class QueryCache : public LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>
{
private:
    using Base = LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>;

public:
    explicit QueryCache(size_t cache_size_in_bytes_)
        : Base(cache_size_in_bytes_)
        , removal_scheduler()
        , cache_removing_thread(&CacheRemovalScheduler::processRemovalQueue<QueryCache>, &removal_scheduler, this)
    {
    }

    ~QueryCache() override
    {
        removal_scheduler.stopProcessingRemovalQueue();
        cache_removing_thread.join();
    }

    bool insertChunk(CacheKey cache_key, Chunk && chunk)
    {
        auto data = get(cache_key);
        data->second.push_back(std::move(chunk));

        if (query_weight(*data) > cache_key.settings.max_query_cache_entry_size)
        {
            remove(cache_key);
            return false;
        }
        set(cache_key, data); // evicts cache if necessary, the entry with key=cache_key will not get evicted
        return false;
    }

    void scheduleRemoval(CacheKey cache_key)
    {
        auto entry_put_timeout =  std::chrono::milliseconds{cache_key.settings.query_cache_entry_put_timeout};
        removal_scheduler.scheduleRemoval(entry_put_timeout, cache_key);
    }

    size_t recordQueryRun(CacheKey cache_key)
    {
        std::lock_guard lock(times_executed_mutex);
        return ++times_executed[cache_key];
    }

    std::mutex& getPutInCacheMutex(CacheKey cache_key)
    {
        return put_in_cache_mutexes[cache_key];
    }

private:
    CacheRemovalScheduler removal_scheduler;
    std::thread cache_removing_thread;

    std::unordered_map<CacheKey, size_t, CacheKeyHasher> times_executed;
    std::mutex times_executed_mutex;

    QueryWeightFunction query_weight;

    std::unordered_map<CacheKey, std::mutex, CacheKeyHasher> put_in_cache_mutexes;
};

}
