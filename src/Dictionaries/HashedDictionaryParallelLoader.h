#pragma once

#include <Dictionaries/IDictionary.h>
#include <Common/CurrentThread.h>
#include <Common/iota.h>
#include <Common/scope_guard_safe.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>
#include <Core/Block.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <numeric>
#include <optional>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric HashedDictionaryThreads;
    extern const Metric HashedDictionaryThreadsActive;
    extern const Metric HashedDictionaryThreadsScheduled;
}

namespace DB
{

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded> class HashedDictionary;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace DB::HashedDictionaryImpl
{

/// Implementation parallel dictionary load for SHARDS
template <DictionaryKeyType dictionary_key_type, typename DictionaryType>
class HashedDictionaryParallelLoader : public boost::noncopyable
{

public:
    explicit HashedDictionaryParallelLoader(DictionaryType & dictionary_)
        : dictionary(dictionary_)
        , dictionary_name(dictionary.getFullName())
        , shards(dictionary.configuration.shards)
        , pool(CurrentMetrics::HashedDictionaryThreads, CurrentMetrics::HashedDictionaryThreadsActive, CurrentMetrics::HashedDictionaryThreadsScheduled, shards)
        , shards_queues(shards)
    {
        UInt64 backlog = dictionary.configuration.shard_load_queue_backlog;
        LOG_TRACE(dictionary.log, "Will load the {} dictionary using {} threads (with {} backlog)", dictionary_name, shards, backlog);

        shards_slots.resize(shards);
        iota(shards_slots.data(), shards_slots.size(), UInt64(0));

        for (size_t shard = 0; shard < shards; ++shard)
        {
            shards_queues[shard].emplace(backlog);
            pool.scheduleOrThrowOnError([this, shard, thread_group = CurrentThread::getGroup()]
            {
                WorkerStatistic statistic;
                SCOPE_EXIT_SAFE(
                    LOG_TRACE(dictionary.log, "Finished worker for dictionary {} shard {}, processed {} blocks, {} rows, total time {}ms",
                        dictionary_name, shard, statistic.total_blocks, statistic.total_rows, statistic.total_elapsed_ms);

                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );

                /// Do not account memory that was occupied by the dictionaries for the query/user context.
                MemoryTrackerBlockerInThread memory_blocker;

                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);
                setThreadName("HashedDictLoad");

                LOG_TRACE(dictionary.log, "Starting worker for dictionary {}, shard {}", dictionary_name, shard);

                threadWorker(shard, statistic);
            });
        }
    }

    void addBlock(Block block)
    {
        IColumn::Selector selector = createShardSelector(block, shards_slots);
        Blocks shards_blocks = splitBlock(selector, block);
        block.clear();

        for (size_t shard = 0; shard < shards; ++shard)
        {
            if (!shards_queues[shard]->push(std::move(shards_blocks[shard])))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to shards queue #{}", shard);
        }
    }

    void finish()
    {
        for (auto & queue : shards_queues)
            queue->finish();

        Stopwatch watch;
        pool.wait();
        UInt64 elapsed_ms = watch.elapsedMilliseconds();
        LOG_TRACE(dictionary.log, "Processing the tail of dictionary {} took {}ms", dictionary_name, elapsed_ms);
    }

    ~HashedDictionaryParallelLoader()
    {
        try
        {
            for (auto & queue : shards_queues)
                queue->clearAndFinish();

            /// NOTE: It is OK to not pass the exception next, since on success finish() should be called which will call wait()
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(dictionary.log, "Exception had been thrown during parallel load of the dictionary");
        }
    }

private:
    DictionaryType & dictionary;
    String dictionary_name;
    const size_t shards;
    ThreadPool pool;
    std::vector<std::optional<ConcurrentBoundedQueue<Block>>> shards_queues;
    std::vector<UInt64> shards_slots;
    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;

    struct WorkerStatistic
    {
        UInt64 total_elapsed_ms = 0;
        UInt64 total_blocks = 0;
        UInt64 total_rows = 0;
    };

    void threadWorker(size_t shard, WorkerStatistic & statistic)
    {
        Block block;
        DictionaryKeysArenaHolder<dictionary_key_type> arena_holder_;
        auto & shard_queue = *shards_queues[shard];

        while (shard_queue.pop(block))
        {
            Stopwatch watch;
            dictionary.blockToAttributes(block, arena_holder_, shard);
            UInt64 elapsed_ms = watch.elapsedMilliseconds();

            statistic.total_elapsed_ms += elapsed_ms;
            statistic.total_blocks += 1;
            statistic.total_rows += block.rows();

            if (elapsed_ms > 1'000)
                LOG_TRACE(dictionary.log, "Block processing for shard #{} is slow {}ms (rows {})", shard, elapsed_ms, block.rows());
        }

        if (!shard_queue.isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pull non finished shards queue #{}", shard);
    }

    /// Split block to shards smaller block, using 'selector'.
    Blocks splitBlock(const IColumn::Selector & selector, const Block & block)
    {
        Blocks out_blocks(shards);
        for (size_t shard = 0; shard < shards; ++shard)
            out_blocks[shard] = block.cloneEmpty();

        size_t columns = block.columns();
        for (size_t col = 0; col < columns; ++col)
        {
            MutableColumns splitted_columns = block.getByPosition(col).column->scatter(shards, selector);
            for (size_t shard = 0; shard < shards; ++shard)
                out_blocks[shard].getByPosition(col).column = std::move(splitted_columns[shard]);
        }

        return out_blocks;
    }

    IColumn::Selector createShardSelector(const Block & block, const std::vector<UInt64> & slots)
    {
        size_t num_rows = block.rows();
        IColumn::Selector selector(num_rows);

        size_t skip_keys_size_offset = dictionary.dict_struct.getKeysSize();
        Columns key_columns;
        key_columns.reserve(skip_keys_size_offset);
        for (size_t i = 0; i < skip_keys_size_offset; ++i)
            key_columns.emplace_back(block.safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto key = keys_extractor.extractCurrentKey();
            size_t shard = dictionary.getShard(key);
            selector[i] = slots[shard];
            keys_extractor.rollbackCurrentKey();
        }

        return selector;
    }
};

}
