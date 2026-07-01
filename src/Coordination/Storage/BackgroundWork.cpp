#include <Coordination/Storage/BackgroundWork.h>

#include <Coordination/Storage/NodeStream.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int ABORTED;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 flush_threads;
    extern const CoordinationSettingsUInt64 merge_threads;
    extern const CoordinationSettingsUInt64 min_files_to_merge;
    extern const CoordinationSettingsUInt64 max_files_to_merge;
    extern const CoordinationSettingsFloat max_size_ratio;
}

namespace Coordination::Storage
{

using DB::Exception;
namespace ErrorCodes = DB::ErrorCodes;

BackgroundWork::BackgroundWork(StorageState * storage_)
    : storage(storage_)
{
    const DB::CoordinationSettings & settings = storage->keeper_context->getCoordinationSettings();

    const size_t num_flush_threads = settings[DB::CoordinationSetting::flush_threads];
    for (size_t i = 0; i < num_flush_threads; ++i)
        flush_threads.emplace_back([this]
            {
                try
                {
                    flushThread();
                }
                catch (...)
                {
                    DB::tryLogCurrentException(storage->log, "Unexpected exception in flush thread", DB::LogsLevel::fatal);
                    std::abort();
                }
            });

    const size_t num_merge_threads = settings[DB::CoordinationSetting::merge_threads];
    for (size_t i = 0; i < num_merge_threads; ++i)
        merge_threads.emplace_back([this]
            {
                try
                {
                    mergeThread();
                }
                catch (...)
                {
                    DB::tryLogCurrentException(storage->log, "Unexpected exception in merge thread", DB::LogsLevel::fatal);
                    std::abort();
                }
            });
}

void BackgroundWork::shutdown()
{
    shutting_down = true;
    flush_requests.release(static_cast<std::ptrdiff_t>(flush_threads.size()));
    merge_requests.release(static_cast<std::ptrdiff_t>(merge_threads.size()));

    for (ThreadFromGlobalPool & thread : flush_threads)
        thread.join();
    flush_threads.clear();
    for (ThreadFromGlobalPool & thread : merge_threads)
        thread.join();
    merge_threads.clear();
}

void BackgroundWork::maybeStartFlush()
{
    flush_requests.release();
}

void BackgroundWork::maybeStartMerge()
{
    merge_requests.release();
}

void BackgroundWork::flushThread()
{
    while (!shutting_down.load())
    {
        /// Pick a memtable to flush.
        MemtablePtr memtable;
        {
            std::lock_guard lock(mutex);
            std::shared_lock storage_lock(*storage->storage_mutex);

            for (const MemtablePtr & m : storage->immutable_memtables)
            {
                if (!flushes_in_progress.contains(m->file_seqno))
                {
                    memtable = m;
                    flushes_in_progress.insert(m->file_seqno);
                    break;
                }
            }
        }

        if (!memtable)
        {
            flush_requests.acquire();
            continue;
        }

        LOG_DEBUG(storage->log, "Flushing memtable {} ({} bytes)", memtable->file_seqno, memtable->total_bytes);

        Stopwatch stopwatch;
        double sort_duration = 0;

        SortedRunPtr new_run;

        /// Do the flush.
        try
        {
            SortedRunWriter writer(std::make_shared<SortedRun>(memtable->file_seqno, memtable->file_seqno), storage);

            Stopwatch sort_stopwatch;
            MemtableSortedNodeStream reader(memtable);
            sort_duration = sort_stopwatch.elapsedSeconds();

            /// Collapse consecutive nodes with equal paths into one or zero nodes.
            std::optional<NodeAction> combined_action;
            while (true)
            {
                if (shutting_down)
                    throw Exception(ErrorCodes::ABORTED, "Cancelling flush because of shutdown");

                reader.next();
                if (reader.at_end)
                    break;

                combined_action = combineActions(combined_action, reader.node.action, /*strict=*/ true);

                if (reader.isNextPathEqualToCurrent())
                    /// This node gets combined with the next one. Only need the action from this node.
                    continue;

                /// We've combined all actions for current path.

                if (!combined_action.has_value())
                    /// ... and they cancelled out (e.g. Create + Remove), nothing to write.
                    continue;

                reader.node.action = *combined_action;
                combined_action.reset();

                writer.appendNode(reader.node);
                writer.finishFileIfBigEnough();
            }

            new_run = writer.finish();
            chassert(new_run);

            int64_t node_count_delta = 0;
            for (const SortedFilePtr & file : new_run->files)
                node_count_delta += file->node_count_delta;
            chassert(memtable->node_count_delta == node_count_delta);
            new_run->node_count_delta = node_count_delta;
        }
        catch (...)
        {
            DB::tryLogCurrentException(storage->log, "Memtable flush failed");

            /// Don't retry immediately.
            std::this_thread::sleep_for(std::chrono::seconds(1));

            std::lock_guard lock(mutex);
            flushes_in_progress.erase(memtable->file_seqno);
            continue;
        }

        const double duration = stopwatch.elapsedSeconds();
        LOG_DEBUG(storage->log,
            "Flushed memtable {} ({} bytes) into a sorted run of {} files ({} bytes) in {:.3} s (sorting took {:.3} s)",
            memtable->file_seqno, memtable->total_bytes,
            new_run->files.size(), new_run->total_block_size,
            duration, sort_duration);

        /// Publish the new sorted run. If some earlier memtable is not flushed yet, leave the result
        /// in the flushed_files reorder buffer for now (the thread that flushes that earlier memtable
        /// will publish our run).
        {
            std::lock_guard lock(mutex);
            std::lock_guard storage_lock(*storage->storage_mutex);

            flushed_files[memtable->file_seqno] = new_run;

            while (!flushed_files.empty())
            {
                const uint32_t seqno = flushed_files.begin()->first;
                chassert(!storage->immutable_memtables.empty());
                chassert(seqno >= storage->immutable_memtables[0]->file_seqno);
                if (seqno != storage->immutable_memtables[0]->file_seqno)
                    break; // previous memtable not flushed yet
                storage->sorted_runs.push_back(flushed_files.begin()->second);
                storage->immutable_memtables.erase(storage->immutable_memtables.begin());
                flushed_files.erase(flushed_files.begin());
                flushes_in_progress.erase(seqno);
                storage->recalculateWriteThrottling();

                maybeStartMerge();
            }
        }
    }
}

void BackgroundWork::mergeThread()
{
    while (!shutting_down.load())
    {
        /// Pick files to merge.
        std::vector<SortedRunPtr> input_runs;
        SortedRunPtr output_run;
        std::vector<uint32_t> seqnos_to_lock;
        bool picked = false;
        {
            std::lock_guard lock(mutex);
            std::shared_lock storage_lock(*storage->storage_mutex);

            picked = pickRunsToMerge(input_runs, output_run, seqnos_to_lock);
            if (picked)
            {
                std::string inputs_description;
                bool first = true;
                for (const SortedRunPtr & r : input_runs)
                {
                    if (!std::exchange(first, false))
                        inputs_description += ", ";
                    inputs_description += fmt::format("{}-{} ({} bytes)", r->min_file_seqno, r->max_file_seqno, r->total_block_size);
                }
                LOG_DEBUG(storage->log, "{} {}/{} sorted runs: [{}]", output_run ? "Resuming merge of" : "Merging", input_runs.size(), storage->sorted_runs.size(), inputs_description);

                for (uint32_t seqno : seqnos_to_lock)
                    merges_in_progress.insert(seqno);
            }
        }

        if (!picked)
        {
            merge_requests.acquire();
            continue;
        }

        auto unlock_files = [&]()
        {
            chassert(!mutex.try_lock());
            for (uint32_t seqno : seqnos_to_lock)
            {
                size_t erased = merges_in_progress.erase(seqno);
                chassert(erased);
            }
        };

        Stopwatch stopwatch;

        /// Do the merge.
        try
        {
            /// Make a private copy of all involved StorageRun-s. We'll be mutating them in place,
            /// then shallowCopy-ing them into StorageState when publishing partial results.
            /// All files in these runs are immutable (SortedRunWriter only adds finished files to
            /// SortedRun).
            for (auto & r : input_runs)
                r = r->shallowCopy();
            if (output_run)
                output_run = output_run->shallowCopy();
            else
                output_run = std::make_shared<SortedRun>(input_runs.front()->min_file_seqno, input_runs.back()->max_file_seqno);

            /// node_count_delta bookkeeping: the output run takes ownership of the merged nodes'
            /// count, the (eventually trimmed) inputs contribute 0. Using += (not =) keeps this
            /// correct for a resumed merge, whose output already carries the sum and whose inputs
            /// were already zeroed by the original merge.
            for (const SortedRunPtr & r : input_runs)
            {
                output_run->node_count_delta += r->node_count_delta;
                r->node_count_delta = 0;
            }

            SortedRunWriter writer(output_run, storage);

            std::vector<SortedRunNodeStream> input_streams;
            input_streams.reserve(input_runs.size());
            for (const SortedRunPtr & r : input_runs)
                input_streams.emplace_back(r, storage->block_cache.get());

            MergingNodeStream merging_stream;
            for (SortedRunNodeStream & s : input_streams)
                merging_stream.addSubstream(&s);

            auto publish_results = [&](bool is_final)
            {
                std::vector<SortedRunPtr> to_publish;
                std::vector<SortedFilePtr> removed_files;

                /// Paths <= min_path_cutoff are now visible in the output SortedRun,
                /// paths > min_path_cutoff stay visible in the input SortedRun-s. Trim (and free)
                /// the input files that became fully redundant.
                ///
                /// (Be careful about cancelled-out paths; the following scenario would be a
                ///  problem, if it were possible:
                ///   1. The MergingNodeStream reported node with path A, we wrote it to the output.
                ///   2. The MergingNodeStream looked at path B, saw a Create + Remove, and didn't
                ///      report any output node. Nothing was appended to output_run.
                ///   3. We decided to publish results. We got min_path_cutoff = A from output_run,
                ///      but inputs' dropConsumedFiles dropped files with max_path <= B. The Remove
                ///      for path B happened to be dropped, but the Create was kept.
                ///   4. Readers now see a Create but no Remove for path B. We have accidentally
                ///      performed necromancy.
                ///  Currently this is not possible because we only call publish_results after
                ///  appending non-cancelled-out node and after finishing the merge.)
                if (is_final)
                {
                    /// (Assert that all input was consumed.)
                    for (size_t i = 0; i < input_runs.size(); ++i)
                    {
                        input_streams[i].dropConsumedFiles(removed_files);
                        chassert(input_runs[i]->files.empty());
                    }
                }
                else
                {
                    chassert(!output_run->files.empty());
                    NodePath min_path_cutoff = output_run->files.back()->blocks.back().max_path;
                    for (size_t i = 0; i < input_runs.size(); ++i)
                    {
                        input_runs[i]->setMinPathCutoff(min_path_cutoff);
                        input_streams[i].dropConsumedFiles(removed_files);
                        if (!input_runs[i]->files.empty())
                            to_publish.push_back(input_runs[i]);
                    }
                    chassert(!to_publish.empty());
                }

                if (!output_run->files.empty()) // may be empty if all input nodes cancelled out
                    to_publish.push_back(output_run);

                if (!is_final)
                {
                    /// We keep mutating output_run and the input runs, so publish snapshots.
                    for (SortedRunPtr & r : to_publish)
                        r = r->shallowCopy();
                }

                /// Publish the updated input and output files.
                {
                    std::lock_guard lock(mutex);
                    std::lock_guard storage_lock(*storage->storage_mutex);
                    auto & runs = storage->sorted_runs;

                    /// Find the range of runs relevant to this merge.
                    size_t start_idx = 0;
                    while (start_idx < runs.size() && runs[start_idx]->min_file_seqno < output_run->min_file_seqno)
                        ++start_idx;
                    chassert(start_idx < runs.size());
                    size_t end_idx = start_idx;
                    while (end_idx < runs.size() && runs[end_idx]->max_file_seqno <= output_run->max_file_seqno)
                    {
                        chassert(merges_in_progress.contains(runs[end_idx]->min_file_seqno));
                        ++end_idx;
                    }
                    chassert(end_idx > start_idx);

                    /// Replace the whole range with to_publish.
                    runs.erase(runs.begin() + start_idx, runs.begin() + end_idx);
                    runs.insert(runs.begin() + start_idx, to_publish.begin(), to_publish.end());

                    storage->recalculateWriteThrottling();
                }

                /// Evict newly obsolete files from block cache and mark them for deletion from disk.
                for (SortedFilePtr & f : removed_files)
                {
                    f->removeFromBlockCache(storage->block_cache.get());

                    if (!storage->memory_only)
                    {
                        /// TODO: Arrange for the file to be deleted when its SortedFilePtr refcount
                        ///       reaches 0.
                    }

                    f.reset();
                }
            };

            while (true)
            {
                if (shutting_down)
                    throw Exception(ErrorCodes::ABORTED, "Cancelling merge because of shutdown");

                merging_stream.next();
                if (merging_stream.at_end)
                    break;

                writer.appendNode(merging_stream.node);

                if (writer.finishFileIfBigEnough())
                    publish_results(/*is_final=*/ false);
            }

            output_run = writer.finish();

            {
                int64_t sum_of_file_deltas = 0;
                for (const SortedFilePtr & file : output_run->files)
                    sum_of_file_deltas += file->node_count_delta;
                chassert(output_run->node_count_delta == sum_of_file_deltas);
                output_run->node_count_delta = sum_of_file_deltas;
            }

            const double duration = stopwatch.elapsedSeconds();
            LOG_DEBUG(storage->log,
                "Merged {} sorted runs into a sorted run of {} files ({} bytes) in {:.3} s",
                input_runs.size(), output_run->files.size(), output_run->total_block_size,
                duration);

            publish_results(/*is_final=*/ true);
            {
                std::lock_guard lock(mutex);
                unlock_files();
            }
            maybeStartMerge();
        }
        catch (...)
        {
            DB::tryLogCurrentException(storage->log, "Merge failed");

            /// Don't retry immediately.
            std::this_thread::sleep_for(std::chrono::seconds(1));

            std::lock_guard lock(mutex);
            unlock_files();
            continue;
        }
    }
}

bool BackgroundWork::pickRunsToMerge(std::vector<SortedRunPtr> & out_inputs, SortedRunPtr & out_partial_output, std::vector<uint32_t> & out_seqnos_to_lock)
{
    chassert(!mutex.try_lock());
    chassert(!storage->storage_mutex->try_lock());

    const auto & runs = storage->sorted_runs;

    /// Look for interrupted merge to resume.
    for (size_t output_idx = 1; output_idx < runs.size(); ++output_idx)
    {
        /// If run's file_seqno range overlaps previous run, it must be a partial merge output.
        if (runs[output_idx]->min_file_seqno <= runs[output_idx - 1]->min_file_seqno &&
            !merges_in_progress.contains(runs[output_idx]->min_file_seqno))
        {
            /// Find the partial merge inputs [start_idx, output_idx). Some inputs may have been
            /// already removed as exhausted.
            size_t start_idx = output_idx - 1;
            while (start_idx > 0 && runs[output_idx]->min_file_seqno <= runs[start_idx - 1]->min_file_seqno)
                --start_idx;

            for (size_t i = start_idx; i < output_idx; ++i)
            {
                chassert(runs[i]->min_path_cutoff.has_value());
                chassert(!merges_in_progress.contains(runs[i]->min_file_seqno));
            }

            out_inputs.assign(runs.begin() + start_idx, runs.begin() + output_idx);
            out_partial_output = runs[output_idx];

            for (const auto & r : out_inputs)
                out_seqnos_to_lock.push_back(r->min_file_seqno);
            if (out_partial_output->min_file_seqno != out_seqnos_to_lock[0])
                /// (This happens if the lowest-numbered input run was exhausted and removed.)
                out_seqnos_to_lock.push_back(out_partial_output->min_file_seqno);

            return true;
        }
    }

    /// Merge policy aims for being simple to implement and to think about, and keeping the number
    /// of sorted runs small. Write amplification is a lower priority. Keeper server is usually
    /// bottlenecked on single-thread performance, leaving plenty of cpu cores available to run merges.
    ///
    /// Current merge policy:
    ///  * A range of sorted runs is eligible to be merged if all of:
    ///     - The runs are not involved in other merges.
    ///     - The number of runs is not too small and not too big.
    ///     - The lowest-numbered of the runs is not much bigger than all other runs put together.
    ///       This limits write amplification.
    ///       We use lowest-numbered instead of biggest to avoid ending up with sawtooth-shaped
    ///       sequence of sizes.
    ///  * Among eligible ranges we prefer ones with lowest-numbered first run, to keep the run
    ///    sizes mostly decreasing. (If we preferred the smallest runs, we'd be merging small recent
    ///    runs before older runs and end up with recent sawtooth-shaped sequence of sizes again.
    ///    Maybe; this is all theoretical.)
    ///    Then we prefer longer ranges.
    ///
    /// Having multiple merge threads is important to avoid accumulating small runs while a big
    /// merge is running; that is true for any merge policy, big merges do need to happen sometimes.
    ///
    /// We pick a merge only when there's a thread available to run it right away, so that we see
    /// the latest state and can e.g. pick up all the recently flushed runs.

    const DB::CoordinationSettings & settings = storage->keeper_context->getCoordinationSettings();
    const size_t min_files_to_merge = settings[DB::CoordinationSetting::min_files_to_merge];
    const size_t max_files_to_merge = settings[DB::CoordinationSetting::max_files_to_merge];
    const float max_size_ratio = settings[DB::CoordinationSetting::max_size_ratio];

    /// Pick an eligible range of runs with lowest-numbered start idx.
    for (size_t start_idx = 0; start_idx < runs.size(); ++start_idx)
    {
        size_t end_idx = start_idx;
        size_t total_bytes = 0;
        while (end_idx < runs.size() && !merges_in_progress.contains(runs[end_idx]->min_file_seqno) && end_idx - start_idx < max_files_to_merge)
        {
            total_bytes += runs[end_idx]->total_block_size;
            ++end_idx;
        }
        if (end_idx - start_idx < min_files_to_merge)
            continue;
        if (double(runs[start_idx]->total_block_size) / double(total_bytes) > double(max_size_ratio))
            continue;

        out_inputs.assign(storage->sorted_runs.begin() + start_idx, storage->sorted_runs.begin() + end_idx);
        for (const auto & r : out_inputs)
            out_seqnos_to_lock.push_back(r->min_file_seqno);
        return true;
    }

    return false;
}

}
