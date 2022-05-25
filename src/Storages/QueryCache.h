#pragma once

#include <memory>
#include <condition_variable>
#include <Common/LRUCache.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


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

        // if the newly scheduled timer turned out to have the smallest timestamp in the entire queue, notify timer_cv
        if (top == timer)
        {
            timer_cv.notify_one();
        }
    }

    template <typename Cache>
    void processRemovalQueue(Cache * cache)
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
                cache->remove(awaited_timer->cache_key);
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

class CachePutHolder
{
private:
    using Cache = LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>;
public:
    CachePutHolder(std::mutex & mutex_, CacheRemovalScheduler * removal_scheduler_, CacheKey cache_key_, Cache * cache_)
        : mutex(mutex_)
        , removal_scheduler(removal_scheduler_)
        , cache_key(cache_key_)
        , cache(cache_)
        , data(std::move(cache->getOrSet(cache_key, [&] { return std::make_shared<Data>(cache_key_.header, Chunks{}); }).first))
    {
        tryAcquire();
    }

    ~CachePutHolder()
    {
        if (executing_put_in_cache.load())
        {
            mutex.unlock();
            removal_scheduler->scheduleRemoval(std::chrono::milliseconds{cache_key.settings.query_cache_entry_put_timeout}, cache_key);
        }
    }


    void insertChunk(Chunk && chunk)
    {
        if (!fits_into_memory || !executing_put_in_cache.load())
        {
            return;
        }
        data = cache->get(cache_key);
        data->second.push_back(std::move(chunk));

        if (query_weight(*data) > cache_key.settings.max_query_cache_entry_size)
        {
            cache->remove(cache_key);
            fits_into_memory = false;
        }
        cache->set(cache_key, data); // evicts cache if necessary, the entry with key=cache_key will not get evicted
    }

private:
    bool tryAcquire()
    {
        bool result = mutex.try_lock();
        executing_put_in_cache.store(result);
        return result;
    }

    std::mutex & mutex;
    CacheRemovalScheduler * removal_scheduler;
    CacheKey cache_key;
    Cache * cache;

    std::atomic<bool> executing_put_in_cache{false};
    bool fits_into_memory = true;
    std::shared_ptr<Data> data;

    QueryWeightFunction query_weight;
};

class CacheReadHolder
{
public:
    explicit CacheReadHolder(std::shared_ptr<Data> data)
    {
        if (data == nullptr)
        {
            return;
        }

        pipe = Pipe(std::make_shared<SourceFromSingleChunk>(data->first, toSingleChunk(data->second)));
    }

    bool containsResult() const
    {
        return !pipe.empty();
    }

    Pipe && getPipe()
    {
        return std::move(pipe);
    }

private:
    static Chunk toSingleChunk(const Chunks& chunks)
    {
        if (chunks.empty())
        {
            return {};
        }
        auto result_columns = chunks[0].clone().mutateColumns();
        for (size_t i = 1; i != chunks.size(); ++i)
        {
            auto columns = chunks[i].getColumns();
            for (size_t j = 0; j != columns.size(); ++j)
            {
                result_columns[j]->insertRangeFrom(*columns[j], 0, columns[j]->size());
            }
        }
        const size_t num_rows = result_columns[0]->size();
        return Chunk(std::move(result_columns), num_rows);
    }

    Pipe pipe;
};

class QueryCache
{
private:
    using Cache = LRUCache<CacheKey, Data, CacheKeyHasher, QueryWeightFunction>;
public:
    explicit QueryCache(size_t cache_size_in_bytes_)
        : cache(std::make_unique<Cache>(cache_size_in_bytes_))
        , removal_scheduler()
        , cache_removing_thread(&CacheRemovalScheduler::processRemovalQueue<Cache>, &removal_scheduler, cache.get())
    {
    }

    CachePutHolder tryPutInCache(CacheKey cache_key)
    {
        return CachePutHolder(put_in_cache_mutexes[cache_key], &removal_scheduler, cache_key, cache.get());
    }

    CacheReadHolder tryReadFromCache(CacheKey cache_key) {
        return CacheReadHolder(cache->get(cache_key));
    }

    bool containsResult(CacheKey cache_key)
    {
        return cache->get(cache_key) != nullptr;
    }

    void reset()
    {
        cache->reset();
    }

    ~QueryCache()
    {
        removal_scheduler.stopProcessingRemovalQueue();
        cache_removing_thread.join();
    }

    size_t recordQueryRun(CacheKey cache_key)
    {
        std::lock_guard lock(times_executed_mutex);
        return ++times_executed[cache_key];
    }


private:
    std::unique_ptr<Cache> cache;

    CacheRemovalScheduler removal_scheduler;
    std::thread cache_removing_thread;

    std::unordered_map<CacheKey, size_t, CacheKeyHasher> times_executed;
    std::mutex times_executed_mutex;

    std::unordered_map<CacheKey, std::mutex, CacheKeyHasher> put_in_cache_mutexes;
};

}
