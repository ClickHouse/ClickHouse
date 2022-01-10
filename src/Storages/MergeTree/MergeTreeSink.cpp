#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <base/scope_guard_safe.h>

namespace DB
{

void MergeTreeSink::onStart()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}


void MergeTreeSink::consume(Chunk chunk)
{
    const auto settings = storage.getSettings();
    std::unique_ptr<ThreadPool> pool = nullptr;
    if (part_blocks.size() > 1 && settings->max_part_writing_threads > 1)
    {
        size_t num_threads = std::min<size_t>(settings->max_part_writing_threads, part_blocks.size());
        pool = std::make_unique<ThreadPool>(num_threads);
    }

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);
    for (auto & current_block : part_blocks)
    {
        if (settings->max_part_writing_threads > 1 && pool != nullptr)
        {
            /// Parallel parts writing.
            pool->scheduleOrThrowOnError([&, this, thread_group = CurrentThread::getGroup()]()
            {
               SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                    );
               if (thread_group)
                   CurrentThread::attachTo(thread_group);
                Stopwatch watch;
                try
                {
                    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);
                    /// If optimize_on_insert setting is true, current_block could become empty after merge
                    /// and we didn't create part.
                    if (!part)
                        return ;
                        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
                    if (storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog()))
                    {
                        PartLog::addNewPart(storage.getContext(), part, watch.elapsed());
                        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
                        storage.background_operations_assignee.trigger();
                    }
                }
                catch (Exception & e)
                {
                    LOG_ERROR(&Poco::Logger::get("MergeTreeSink"), "failed to write part block, reason {}", e.displayText());
                }
            });
        }
        else
        {
            Stopwatch watch;
            MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);
            /// If optimize_on_insert setting is true, current_block could become empty after merge
            /// and we didn't create part.
            if (!part)
                continue;
            /// Part can be deduplicated, so increment counters and add to part log only if it's really added
            if (storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog()))
            {
                PartLog::addNewPart(storage.getContext(), part, watch.elapsed());
                /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
                storage.background_operations_assignee.trigger();
            }
        }
    }
    if (settings->max_part_writing_threads > 1 && pool != nullptr)
    {
        pool->wait();
    }
}

}
