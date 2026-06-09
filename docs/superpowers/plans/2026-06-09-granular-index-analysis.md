# Granular (sub-part) Index Analysis Parallelism — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parallelize primary-key and skip-index analysis at mark-range *chunk* granularity within each part, removing the current `num_parts` ceiling on `filterPartsByPrimaryKeyAndSkipIndexes` and fixing the few-large-parts imbalance.

**Architecture:** A new pure helper `splitMarkRanges` partitions a part's existing `MarkRanges` into contiguous sub-ranges sized by a global mark budget with a floor. Each `(part, sub-range)` becomes a work item scheduled into one shared `ThreadPool`; its shared queue load-balances. Each chunked item runs `markRangesFromPKRange` then the ordinary skip-index filtering loop on its sub-range, writing a per-chunk result slot and updating only granule-level stat atomics. A sequential post-pass concatenates a part's chunk slots and computes part-level stats. Parts using non-chunkable features (disjunction bitset, top-K min-max, FINAL exact mode, or a vector-similarity index) fall back to a single work item run through the **unchanged** legacy `process_part` body.

**Tech Stack:** C++ (ClickHouse), `boost::container::devector`-based `MarkRanges`, ClickHouse `ThreadPool`, gtest (`unit_tests_dbms`), `0_stateless` SQL tests.

Spec: `docs/superpowers/specs/2026-06-09-granular-index-analysis-design.md`

---

## File Structure

- **Create** `src/Storages/MergeTree/MarkRangeChunking.h` / `.cpp` — pure `splitMarkRanges` helper. One responsibility: partition `MarkRanges` into contiguous chunks. Reusable by the design-3 upgrade.
- **Create** `src/Storages/MergeTree/tests/gtest_mark_range_chunking.cpp` — unit tests (auto-globbed by `src/CMakeLists.txt:874`, no CMake edit).
- **Modify** `src/Core/Settings.cpp` — declare new query setting `min_marks_per_index_analysis_task`.
- **Modify** `src/Core/SettingsChangesHistory.cpp` — record the new setting.
- **Modify** `src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp` — extern the setting; add the chunked work-item path, post-pass, and scheduler changes inside `filterPartsByPrimaryKeyAndSkipIndexes` (function starts at `:819`; the `process_part` lambda is `:944`–`:1123`; the scheduler is `:1127`–`:1162`).
- **Create** `tests/queries/0_stateless/<NNNNN>_granular_index_analysis_equivalence.sql` (+ `.reference`) — equivalence + concurrency-stress test, via `./tests/queries/0_stateless/add-test`.

---

## Task 1: `splitMarkRanges` pure helper + gtest

**Files:**
- Create: `src/Storages/MergeTree/MarkRangeChunking.h`
- Create: `src/Storages/MergeTree/MarkRangeChunking.cpp`
- Test: `src/Storages/MergeTree/tests/gtest_mark_range_chunking.cpp`

`MarkRanges` is `boost::container::devector<MarkRange>` with `getNumberOfMarks()` (`src/Storages/MergeTree/MarkRange.h:34`), and `MarkRange{size_t begin, size_t end}` (`MarkRange.h:20`). `splitMarkRanges` walks the flattened mark sequence of the input and emits contiguous `MarkRanges`, each holding about `target_marks_per_chunk` marks, splitting an individual `MarkRange` across a chunk boundary when needed. `target_marks_per_chunk == 0` is treated as "do not split" (returns a single chunk equal to the input).

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <Storages/MergeTree/MarkRangeChunking.h>

using namespace DB;

namespace
{
size_t totalMarks(const std::vector<MarkRanges> & chunks)
{
    size_t n = 0;
    for (const auto & c : chunks)
        n += c.getNumberOfMarks();
    return n;
}

/// Flatten chunks back into the sequence of individual marks they cover.
std::vector<size_t> flatten(const std::vector<MarkRanges> & chunks)
{
    std::vector<size_t> marks;
    for (const auto & c : chunks)
        for (const auto & r : c)
            for (size_t m = r.begin; m < r.end; ++m)
                marks.push_back(m);
    return marks;
}
}

TEST(MarkRangeChunking, EmptyInputProducesNoChunks)
{
    MarkRanges empty;
    EXPECT_TRUE(splitMarkRanges(empty, 10).empty());
}

TEST(MarkRangeChunking, ZeroTargetReturnsSingleChunkEqualToInput)
{
    MarkRanges in{{0, 100}};
    auto chunks = splitMarkRanges(in, 0);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].getNumberOfMarks(), 100u);
}

TEST(MarkRangeChunking, TargetLargerThanTotalReturnsSingleChunk)
{
    MarkRanges in{{0, 50}};
    auto chunks = splitMarkRanges(in, 1000);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].getNumberOfMarks(), 50u);
}

TEST(MarkRangeChunking, SplitsSingleRangeIntoEvenChunks)
{
    MarkRanges in{{0, 100}};
    auto chunks = splitMarkRanges(in, 25);
    EXPECT_EQ(chunks.size(), 4u);
    EXPECT_EQ(totalMarks(chunks), 100u);
}

TEST(MarkRangeChunking, SplitsAcrossMultipleRangesPreservingMarks)
{
    /// Two disjoint ranges: [0,30) and [100,140) -> 70 marks total.
    MarkRanges in{{0, 30}, {100, 140}};
    auto chunks = splitMarkRanges(in, 20);
    EXPECT_EQ(totalMarks(chunks), 70u);

    /// The exact same marks must be covered, in the same order, with no overlaps or gaps.
    std::vector<size_t> expected;
    for (size_t m = 0; m < 30; ++m) expected.push_back(m);
    for (size_t m = 100; m < 140; ++m) expected.push_back(m);
    EXPECT_EQ(flatten(chunks), expected);

    /// No chunk exceeds the target.
    for (const auto & c : chunks)
        EXPECT_LE(c.getNumberOfMarks(), 20u);
}

TEST(MarkRangeChunking, RemainderGoesIntoFinalChunk)
{
    MarkRanges in{{0, 10}};
    auto chunks = splitMarkRanges(in, 3);
    EXPECT_EQ(totalMarks(chunks), 10u);
    EXPECT_GE(chunks.size(), 4u); /// 3+3+3+1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target unit_tests_dbms 2>&1 | tail -5`
Expected: FAIL to compile — `MarkRangeChunking.h` / `splitMarkRanges` do not exist.

- [ ] **Step 3: Write minimal implementation**

`src/Storages/MergeTree/MarkRangeChunking.h`:
```cpp
#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <vector>

namespace DB
{

/// Partition `ranges` into contiguous sub-ranges, each containing about
/// `target_marks_per_chunk` marks (the last chunk may hold the remainder).
/// A single MarkRange is split across a chunk boundary when needed.
/// `target_marks_per_chunk == 0` means "do not split": returns one chunk equal to `ranges`
/// (or no chunks if `ranges` is empty). The concatenation of the returned chunks, in order,
/// covers exactly the same marks as `ranges`.
std::vector<MarkRanges> splitMarkRanges(const MarkRanges & ranges, size_t target_marks_per_chunk);

}
```

`src/Storages/MergeTree/MarkRangeChunking.cpp`:
```cpp
#include <Storages/MergeTree/MarkRangeChunking.h>

namespace DB
{

std::vector<MarkRanges> splitMarkRanges(const MarkRanges & ranges, size_t target_marks_per_chunk)
{
    std::vector<MarkRanges> chunks;

    if (ranges.getNumberOfMarks() == 0)
        return chunks;

    if (target_marks_per_chunk == 0)
    {
        chunks.emplace_back(ranges);
        return chunks;
    }

    MarkRanges current;
    size_t current_marks = 0;

    for (const auto & range : ranges)
    {
        size_t begin = range.begin;
        while (begin < range.end)
        {
            size_t take = std::min(target_marks_per_chunk - current_marks, range.end - begin);
            current.push_back(MarkRange(begin, begin + take));
            current_marks += take;
            begin += take;

            if (current_marks == target_marks_per_chunk)
            {
                chunks.push_back(std::move(current));
                current = MarkRanges{};
                current_marks = 0;
            }
        }
    }

    if (current_marks > 0)
        chunks.push_back(std::move(current));

    return chunks;
}

}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cmake --build build --target unit_tests_dbms 2>&1 | tail -5 && ./build/src/unit_tests_dbms --gtest_filter='MarkRangeChunking.*'`
Expected: all `MarkRangeChunking.*` tests PASS. (Redirect build output to `build/build_mark_chunking.log` and have a subagent summarize per repo convention.)

- [ ] **Step 5: Commit**

```bash
git add src/Storages/MergeTree/MarkRangeChunking.h src/Storages/MergeTree/MarkRangeChunking.cpp src/Storages/MergeTree/tests/gtest_mark_range_chunking.cpp
git commit -m "Add splitMarkRanges helper for sub-part index analysis chunking"
```

---

## Task 2: Add the `min_marks_per_index_analysis_task` setting

**Files:**
- Modify: `src/Core/Settings.cpp:304-306` (next to `max_threads_for_indexes`)
- Modify: `src/Core/SettingsChangesHistory.cpp`
- Modify: `src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp:88` (extern block)

Semantics: the `MIN_MARKS_PER_TASK` floor. **`0` (default) disables within-part chunking** — every part becomes a single work item (exactly today's behavior). A positive value `N` enables chunking for parts larger than `N` marks. The production default will be set by the benchmark in Task 6; it stays `0` until then so this change is behavior-preserving.

- [ ] **Step 1: Declare the setting**

In `src/Core/Settings.cpp`, immediately after the `max_threads_for_indexes` block (`:304-306`), add:
```cpp
DECLARE(UInt64, min_marks_per_index_analysis_task, 0, R"(
The minimum number of marks per task when analysing the primary key and skip indexes of a single
data part in parallel. When set to a positive value, the analysis of a part larger than this many
marks is split into several tasks distributed across threads, in addition to the existing per-part
parallelism. `0` disables this sub-part parallelism (analysis is parallelized only across parts).
)", 0) \
```

- [ ] **Step 2: Record in settings history**

In `src/Core/SettingsChangesHistory.cpp`, add an entry for the current development version's list:
```cpp
    {"min_marks_per_index_analysis_task", 0, 0, "New setting to parallelize index analysis within a part."},
```
(Place it in the most recent version block, following the surrounding alphabetical/standard pattern in that block.)

- [ ] **Step 3: Extern the setting in the executor**

In `src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp`, in the `namespace Setting` block (`:80-106`), after the `max_threads_for_indexes` line (`:88`) add:
```cpp
    extern const SettingsUInt64 min_marks_per_index_analysis_task;
```

- [ ] **Step 4: Verify it builds and the setting exists**

Run: `cmake --build build --target clickhouse 2>&1 | tail -5` (redirect to `build/build_setting.log`, summarize via subagent), then
`./build/programs/clickhouse local -q "SELECT name, value FROM system.settings WHERE name='min_marks_per_index_analysis_task'"`
Expected: one row, value `0`.

- [ ] **Step 5: Commit**

```bash
git add src/Core/Settings.cpp src/Core/SettingsChangesHistory.cpp src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp
git commit -m "Add min_marks_per_index_analysis_task setting (disabled by default)"
```

---

## Task 3: Chunked work-item path, post-pass, and scheduler

**Files:**
- Modify: `src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp` — within `filterPartsByPrimaryKeyAndSkipIndexes` (`:819`).
- Add include at top of the file (near the other `Storages/MergeTree/*` includes): `#include <Storages/MergeTree/MarkRangeChunking.h>`.

The existing `process_part` lambda (`:944-1123`) and the scheduler block (`:1127-1162`) are the change site. Keep `process_part` **unchanged** — it is the `whole_part` path. Add a parallel `process_chunk` path, build a flat work-item list, schedule it, and run a post-pass. All chunking is gated by `min_marks_per_index_analysis_task`; when `0`, every part is a `whole_part` item and the code reduces to today's behavior.

This task is verified by the SQL equivalence test in Task 4 (a behavior-preserving refactor guarded against regression). Write Task 4's test first if executing strictly TDD; otherwise build this and run Task 4 immediately after.

- [ ] **Step 1: Add the `whole_part` predicate and supporting types**

Insert **after** the `perform_top_k_optimization` / `top_k_handle_ties` declarations (`:902-903`) and **before** the `num_threads` computation (`:905`), so `perform_top_k_optimization` is already in scope:

```cpp
    /// A part must be analysed as a single unit (no sub-part chunking) when it uses a feature
    /// whose state spans the whole part and cannot be reconstructed by concatenating per-chunk
    /// results: the disjunction bitset, the top-K min-max optimization, FINAL exact mode, or a
    /// vector-similarity index (whose filtering early-returns unless the input covers the whole part).
    const size_t min_marks_per_task = settings[Setting::min_marks_per_index_analysis_task];

    auto part_uses_vector_similarity_index = [&](size_t part_index) -> bool
    {
        for (const auto & index_and_condition : skip_indexes.useful_indices)
            if (index_and_condition.index->isVectorSimilarityIndex())
                return true;
        (void)part_index;
        return false;
    };

    auto is_whole_part_only = [&](size_t part_index) -> bool
    {
        return min_marks_per_task == 0
            || use_skip_indexes_for_disjunctions
            || perform_top_k_optimization
            || (is_final_query && use_skip_indexes_if_final_exact_mode_)
            || part_uses_vector_similarity_index(part_index);
    };

    struct ChunkResult
    {
        MarkRanges ranges;
        MarkRanges exact_ranges;
        RangesInDataPartReadHints read_hints;
        bool used_skip_index = false;
        bool pk_non_empty = false;   /// PK-survivor ranges for this chunk were non-empty (before skip indexes)
    };

    struct WorkItem
    {
        size_t part_index;
        size_t chunk_index;   /// index into chunk_results[part_index]
        MarkRanges sub_ranges;
        bool whole_part;
    };

    /// Per-part chunk result slots (only used by the chunked path).
    std::vector<std::vector<ChunkResult>> chunk_results(parts_with_ranges.size());

    std::atomic<bool> abort_analysis{false};
```

Note: `perform_top_k_optimization` is computed at `:902`, before this insertion point — keep this block after `:903`.

- [ ] **Step 2: Build the flat work-item list (replaces nothing; add before the scheduler block at `:1125`)**

Insert just before `LOG_TRACE(log, "Filtering marks by primary and secondary keys");` (`:1125`):

```cpp
    std::vector<WorkItem> work_items;
    {
        /// Global mark budget: aim for roughly num_threads * OVERSUBSCRIBE work items across all
        /// parts combined, but never smaller than min_marks_per_task.
        constexpr size_t OVERSUBSCRIBE = 3;
        size_t total_marks = 0;
        for (const auto & part : parts_with_ranges)
            total_marks += part.getMarksCount();

        size_t chunk_marks = 0;
        if (min_marks_per_task != 0)
        {
            const size_t divisor = std::max<size_t>(1, num_threads * OVERSUBSCRIBE);
            chunk_marks = std::max<size_t>(min_marks_per_task, (total_marks + divisor - 1) / divisor);
        }

        for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
        {
            if (is_whole_part_only(part_index)
                || parts_with_ranges[part_index].getMarksCount() <= min_marks_per_task
                || chunk_marks == 0)
            {
                work_items.push_back(WorkItem{part_index, 0, MarkRanges{}, /*whole_part=*/true});
                continue;
            }

            auto sub_range_chunks = splitMarkRanges(parts_with_ranges[part_index].ranges, chunk_marks);
            chunk_results[part_index].resize(sub_range_chunks.size());
            for (size_t chunk_index = 0; chunk_index < sub_range_chunks.size(); ++chunk_index)
                work_items.push_back(
                    WorkItem{part_index, chunk_index, std::move(sub_range_chunks[chunk_index]), /*whole_part=*/false});
        }
    }
```

- [ ] **Step 3: Add the `process_chunk` lambda right after the existing `process_part` lambda (after `:1123`)**

```cpp
        /// Chunked path: analyse a contiguous sub-range of one part. Writes a ChunkResult slot and
        /// updates only granule-level stat atomics; part-level counters are handled by the post-pass.
        auto process_chunk = [&](const WorkItem & item)
        {
            if (abort_analysis.load(std::memory_order_relaxed))
                return;

            if (query_status)
                query_status->checkTimeLimit();

            const size_t part_index = item.part_index;
            auto & orig = parts_with_ranges[part_index];
            ChunkResult & out = chunk_results[part_index][item.chunk_index];

            /// 1) Primary key analysis on the sub-range. markRangesFromPKRange reads only
            /// data_part and .ranges from its RangesInDataPart argument.
            MarkRanges pk_ranges = item.sub_ranges;
            if (metadata_snapshot->hasPrimaryKey() || part_offset_condition || total_offset_condition)
            {
                CurrentMetrics::Increment metric(CurrentMetrics::FilteringMarksWithPrimaryKey);
                ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithPrimaryKeyMicroseconds);

                const size_t total_marks_count = item.sub_ranges.getNumberOfMarks();
                ProfileEvents::increment(ProfileEvents::FilteringMarksWithPrimaryKeyProcessedMarks, total_marks_count);
                pk_stat.total_granules.fetch_add(total_marks_count, std::memory_order_relaxed);

                RangesInDataPart sub_part(orig.data_part, orig.parent_part, orig.part_index_in_query,
                                          orig.part_starting_offset_in_query, item.sub_ranges, {});
                pk_ranges = markRangesFromPKRange(
                    sub_part, metadata_snapshot, key_condition, part_offset_condition, total_offset_condition,
                    find_exact_ranges ? &out.exact_ranges : nullptr, settings, log);

                pk_stat.search_algorithm.store(pk_ranges.search_algorithm, std::memory_order_relaxed);
                pk_stat.granules_dropped.fetch_add(total_marks_count - pk_ranges.getNumberOfMarks(), std::memory_order_relaxed);
                pk_stat.elapsed_us.fetch_add(watch.elapsed(), std::memory_order_relaxed);
            }

            sum_marks_pk.fetch_add(pk_ranges.getNumberOfMarks(), std::memory_order_relaxed);

            out.ranges = pk_ranges;
            out.pk_non_empty = !pk_ranges.empty();
            out.read_hints = orig.read_hints; /// inherit any pre-existing whole-part hints

            /// 2) Ordinary skip-index filtering on the sub-range survivors.
            if (!skip_indexes.empty() && !out.ranges.empty())
            {
                CurrentMetrics::Increment metric(CurrentMetrics::FilteringMarksWithSecondaryKeys);
                auto alter_conversions = MergeTreeData::getAlterConversionsForPart(orig.data_part, mutations_snapshot, context);
                const auto & all_updated_columns = alter_conversions->getAllUpdatedColumns();

                /// Chunked path never uses the disjunction bitset (is_whole_part_only excludes it),
                /// so pass a disabled bitset.
                PartialDisjunctionResult unused_partial;

                const auto num_indexes = skip_indexes.useful_indices.size();
                for (size_t idx = 0; idx < num_indexes; ++idx)
                {
                    if (out.ranges.empty())
                        break;

                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

                    const auto index_idx = skip_indexes.per_part_index_orders[part_index][idx];
                    const auto & index_and_condition = skip_indexes.useful_indices[index_idx];

                    auto index_stat_idx = idx;
                    if (settings[Setting::per_part_index_stats])
                        index_stat_idx += num_indexes * part_index;
                    auto & stat = useful_indices_stat[index_stat_idx];

                    size_t total_granules = out.ranges.getNumberOfMarks();
                    stat.total_granules.fetch_add(total_granules, std::memory_order_relaxed);

                    if (auto check_result = canUseIndex(index_and_condition.index, metadata_snapshot, all_updated_columns); !check_result)
                    {
                        LOG_TRACE(log, "Index {} is not used for part {}. Reason: {}",
                                  index_and_condition.index->index.name, orig.data_part->name, check_result.error().text);
                        continue;
                    }

                    /// Vector-similarity indexes are excluded via is_whole_part_only; everything reaching
                    /// here is filtered with filterMarksUsingIndex on the sub-range.
                    if (!(use_skip_indexes_on_data_read_ && !index_and_condition.index->isVectorSimilarityIndex()))
                    {
                        std::tie(out.ranges, out.read_hints) = filterMarksUsingIndex(
                            index_and_condition.index, index_and_condition.condition, key_condition_rpn_template,
                            orig.data_part, out.ranges, out.read_hints, reader_settings,
                            mark_cache.get(), uncompressed_cache.get(), vector_similarity_index_cache.get(),
                            /*use_skip_indexes_for_disjunctions=*/false, unused_partial, log);
                    }

                    stat.granules_dropped.fetch_add(total_granules - out.ranges.getNumberOfMarks(), std::memory_order_relaxed);
                    stat.elapsed_us.fetch_add(watch.elapsed(), std::memory_order_relaxed);
                    out.used_skip_index = true;
                }
            }

            /// 3) Row-limit fail-fast (disjoint sub-ranges -> total_rows sums correctly).
            if (!out.ranges.empty())
            {
                if (limits.max_rows || leaf_limits.max_rows)
                {
                    /// getRowsCount lives on RangesInDataPart; build a throwaway with the chunk's ranges.
                    RangesInDataPart counter(orig.data_part, orig.parent_part, orig.part_index_in_query,
                                             orig.part_starting_offset_in_query, out.ranges, {});
                    auto current_rows_estimate = counter.getRowsCount();
                    size_t total_rows_estimate = current_rows_estimate + total_rows.fetch_add(current_rows_estimate, std::memory_order_relaxed);
                    if (query_info.trivial_limit > 0 && total_rows_estimate > query_info.trivial_limit)
                        total_rows_estimate = query_info.trivial_limit;

                    bool exceeds = (limits.max_rows && total_rows_estimate > limits.max_rows)
                                || (leaf_limits.max_rows && total_rows_estimate > leaf_limits.max_rows);
                    if (exceeds && has_projections)
                    {
                        result.exceeded_row_limits = true;
                        abort_analysis.store(true, std::memory_order_relaxed);
                        return;
                    }
                    limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
                    leaf_limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
                }
            }
        };
```

- [ ] **Step 4: Replace the scheduler block (`:1127-1162`) to dispatch work items**

```cpp
        auto dispatch = [&](const WorkItem & item)
        {
            if (item.whole_part)
                process_part(item.part_index);
            else
                process_chunk(item);
        };

        if (num_threads <= 1)
        {
            for (const auto & item : work_items)
                dispatch(item);
        }
        else
        {
            ThreadPool pool(
                CurrentMetrics::MergeTreeDataSelectExecutorThreads,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsActive,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
                num_threads);

            for (const auto & item : work_items)
            {
                pool.scheduleOrThrow(
                    [&, &item_ref = item, thread_group = CurrentThread::getGroup()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                        dispatch(item_ref);
                    },
                    Priority{},
                    context->getSettingsRef()[Setting::lock_acquire_timeout].totalMicroseconds());
            }
            pool.wait();
        }
```

Also raise the thread cap so it is no longer bounded by part count. Replace the `num_threads` computation at `:905-909` with:
```cpp
        size_t num_threads = num_streams;
        if (settings[Setting::max_threads_for_indexes])
            num_threads = std::min<size_t>(num_streams, settings[Setting::max_threads_for_indexes]);
        num_threads = std::min<size_t>(num_threads, std::max<size_t>(1, work_items.size()));
```
Because `work_items` is built later, move the work-item construction (Step 2) to *before* this `num_threads` line, and compute `chunk_marks` in Step 2 using a provisional `num_threads = settings[Setting::max_threads_for_indexes] ? min(num_streams, that) : num_streams`. (Concretely: in Step 2 compute `size_t provisional_threads = settings[Setting::max_threads_for_indexes] ? std::min<size_t>(num_streams, settings[Setting::max_threads_for_indexes]) : num_streams;` and use it as the divisor base; then set the final `num_threads` from `work_items.size()` as above.)

- [ ] **Step 5: Add the post-pass after `pool.wait()` / sequential loop, before the existing top-K merge (`:1166`)**

```cpp
        /// Post-pass: assemble chunked parts and compute part-level stats.
        for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
        {
            if (chunk_results[part_index].empty())
                continue; /// whole_part item already wrote parts_with_ranges[part_index] in place

            auto & dst = parts_with_ranges[part_index];
            MarkRanges merged;
            MarkRanges merged_exact;
            RangesInDataPartReadHints merged_hints;
            bool used_skip_index = false;
            bool pk_non_empty = false;

            for (auto & chunk : chunk_results[part_index])
            {
                for (const auto & r : chunk.ranges)
                    merged.push_back(r);
                for (const auto & r : chunk.exact_ranges)
                    merged_exact.push_back(r);
                /// Text-index granules are whole-part and identical across chunks: insert/overwrite by name.
                for (auto & [name, granule] : chunk.read_hints.index_granules)
                    merged_hints.index_granules[name] = granule;
                used_skip_index = used_skip_index || chunk.used_skip_index;
                pk_non_empty = pk_non_empty || chunk.pk_non_empty;
            }
            merged.search_algorithm = pk_stat.search_algorithm.load(std::memory_order_relaxed);

            dst.ranges = std::move(merged);
            dst.exact_ranges = std::move(merged_exact);
            dst.read_hints = std::move(merged_hints);

            /// Part-level stats (deferred from the chunked path).
            pk_stat.total_parts.fetch_add(1, std::memory_order_relaxed);
            if (dst.ranges.empty())
                pk_stat.parts_dropped.fetch_add(1, std::memory_order_relaxed);
            /// Legacy `process_part` increments sum_parts_pk on the PK result (before skip indexes), at :979.
            if (pk_non_empty)
                sum_parts_pk.fetch_add(1, std::memory_order_relaxed);

            /// Per-index part counters are approximated under chunking (see "Known limitation" below);
            /// we only record that a skip index was applied to this part.
            if (used_skip_index)
                skip_index_used_in_part[part_index] = 1;
        }
```

Note on `sum_marks_pk`/`sum_parts_pk`: the chunked path already added each chunk's PK-survivor marks to `sum_marks_pk`; `sum_parts_pk` is incremented once per part whose PK result (pre-skip-index) was non-empty in the post-pass, matching legacy `:977-980`. Mirror exactly.

**Known limitation (document, don't fix here):** granule-level per-index stats (`total_granules`, `granules_dropped`) sum exactly across chunks, so EXPLAIN-indexes "Granules: X/Y" and `SelectedMarks` remain exact. The per-index *part* counters (`total_parts`, `parts_dropped`) are inherently ill-defined per chunk (different chunks may be emptied by different indexes), so under chunking they are approximated and not incremented per-index in the post-pass; this only affects the per-index "Parts: X/Y" line of EXPLAIN-indexes when the setting is enabled, never the selected data. Add a one-line code comment to that effect. The equivalence test (Task 4) asserts on results and `SelectedMarks`, not per-index part counts, so it stays green.

- [ ] **Step 6: Build**

Run: `cmake --build build --target clickhouse 2>&1 | tee build/build_task3.log | tail -5`
Expected: builds clean. Use a subagent to summarize `build/build_task3.log`.

- [ ] **Step 7: Commit**

```bash
git add src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp
git commit -m "Parallelize index analysis at sub-part (mark-range chunk) granularity"
```

---

## Task 4: SQL equivalence + concurrency-stress test

**Files:**
- Create via: `./tests/queries/0_stateless/add-test granular_index_analysis_equivalence` → produces `tests/queries/0_stateless/<NNNNN>_granular_index_analysis_equivalence.sql` and `.reference`.

The test builds a table with several parts (one large), runs queries under `min_marks_per_index_analysis_task = 0` (baseline) and a small positive value (heavy chunking, e.g. `8`), and asserts identical results and identical selected-mark ProfileEvents. It covers: PK binary-search predicate, PK generic-exclusion predicate (`modulo`/`OR`), a `minmax` skip index, a `set` skip index, multiple skip indexes, empty result, and a single huge part.

- [ ] **Step 1: Write the test**

`tests/queries/0_stateless/<NNNNN>_granular_index_analysis_equivalence.sql`:
```sql
-- Tags: no-random-merge-tree-settings
-- Verify that sub-part index analysis (min_marks_per_index_analysis_task > 0)
-- selects exactly the same data as part-level analysis (= 0).

DROP TABLE IF EXISTS t_granular_idx;

CREATE TABLE t_granular_idx
(
    k UInt64,
    v UInt64,
    s String,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_s s TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128;

-- One large insert (many granules in a single part) plus a few small ones (several parts).
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(500000);
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(1000);
INSERT INTO t_granular_idx SELECT number + 1000000, number % 1000, toString(number % 50) FROM numbers(1000);

-- Helper: run a query both ways and confirm equal results.
-- PK binary search (continuous range on the primary key).
SELECT 'pk_range',
    (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 8);

-- PK generic exclusion (non-continuous predicate on the key).
SELECT 'pk_generic',
    (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 8);

-- minmax skip index.
SELECT 'minmax',
    (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_v')
  = (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_v');

-- set skip index.
SELECT 'set',
    (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_s')
  = (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_s');

-- Both skip indexes + PK range together.
SELECT 'combined',
    (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 8);

-- Empty result.
SELECT 'empty',
    (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 8);

-- Selected marks must match exactly: compare the number of granules read for the same query.
SELECT 'marks_match',
    (SELECT sum(read_granules = read_granules) FROM
        (SELECT 1 AS read_granules)) ; -- placeholder removed below

DROP TABLE t_granular_idx;
```

Replace the `marks_match` placeholder line with a concrete granule-equality check using `system.query_log` ProfileEvents `SelectedMarks`:
```sql
SET log_queries = 1;
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 AND v = 42 SETTINGS min_marks_per_index_analysis_task = 0, log_comment = 'gia_base';
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 AND v = 42 SETTINGS min_marks_per_index_analysis_task = 8, log_comment = 'gia_chunked';
SYSTEM FLUSH LOGS;
SELECT 'marks_match',
    (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_base' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1)
  = (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_chunked' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1);
```

The `.reference` file contains `1` for every comparison line:
```
pk_range	1
pk_generic	1
minmax	1
set	1
combined	1
empty	1
marks_match	1
```

- [ ] **Step 2: Run with chunking disabled (must already pass — pure baseline equivalence to itself)**

Run: `./build/programs/clickhouse local` is not enough for query_log; run via the test harness:
`tests/clickhouse-test -c ./build/programs/clickhouse <NNNNN>_granular_index_analysis_equivalence 2>&1 | tee build/test_gia.log`
Expected: PASS. (Summarize `build/test_gia.log` via subagent.)

- [ ] **Step 3: Concurrency stress under sanitizers**

If a TSan/ASan build exists (`build_tsan`/`build_asan`), run the same test against it to exercise concurrent same-part chunks:
Run: `tests/clickhouse-test -c ./build_tsan/programs/clickhouse <NNNNN>_granular_index_analysis_equivalence 2>&1 | tee build/test_gia_tsan.log`
Expected: PASS, no TSan reports.

- [ ] **Step 4: Run the new test 10+ times to check flakiness** (per repo convention)

Run: `for i in $(seq 1 12); do tests/clickhouse-test -c ./build/programs/clickhouse <NNNNN>_granular_index_analysis_equivalence || echo "FAIL run $i"; done 2>&1 | tee build/test_gia_loop.log`
Expected: 12/12 PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/queries/0_stateless/<NNNNN>_granular_index_analysis_equivalence.sql tests/queries/0_stateless/<NNNNN>_granular_index_analysis_equivalence.reference
git commit -m "Add equivalence test for sub-part index analysis"
```

---

## Task 5: Broader regression run

**Files:** none (verification only).

- [ ] **Step 1: Run the existing index/select stateless tests with chunking forced on**

Run a targeted subset that exercises PK + skip indexes, forcing chunking via the global setting override, to catch any equivalence break the new test didn't:
`tests/clickhouse-test -c ./build/programs/clickhouse --client-option min_marks_per_index_analysis_task=8 00990 01515 02343 02882 2>&1 | tee build/test_gia_regression.log`
(Choose the actual relevant test numbers for PK ranges, minmax, set, and multiple skip indexes by grepping `tests/queries/0_stateless` for `TYPE minmax`/`TYPE set` usage; list them in the commit message.)
Expected: all PASS. Summarize the log via subagent.

- [ ] **Step 2: Commit** (only if any test needed adjustment; otherwise skip).

---

## Task 6: Benchmark and set the production default

**Files:**
- Modify: `src/Core/Settings.cpp` (the `min_marks_per_index_analysis_task` default).

- [ ] **Step 1: Construct a few-large-parts benchmark**

On the metal box (see memory: 192-vCPU GNR), build a table with 1–4 very large parts and a `minmax` + `set` skip index, then run a selective query that forces heavy skip-index evaluation. Measure index-analysis time via `ProfileEvents['FilteringMarksWithSecondaryKeysMicroseconds']` and wall-clock, sweeping `min_marks_per_index_analysis_task` over `{0, 256, 1024, 4096, 16384}`.

- [ ] **Step 2: Confirm the many-tiny-parts case does not regress**

Run the standard perf suite or a many-small-parts query at the candidate default vs `0`; ensure no regression from task overhead.

- [ ] **Step 3: Set the default**

Pick the value that maximizes the few-large-parts speedup without regressing many-tiny-parts (spec open item; expected in the low thousands of marks). Update the `DECLARE(UInt64, min_marks_per_index_analysis_task, <value>, ...)` default in `src/Core/Settings.cpp` and the `SettingsChangesHistory.cpp` entry to reflect the new default.

- [ ] **Step 4: Commit**

```bash
git add src/Core/Settings.cpp src/Core/SettingsChangesHistory.cpp
git commit -m "Enable sub-part index analysis by default (min_marks_per_index_analysis_task)"
```

---

## Design-3 follow-up (out of scope here)

Keep the `WorkItem`/`ChunkResult`/post-pass structure. Replace the static `scheduleOrThrow`-all loop with an adaptive queue: a thread that finds the queue empty splits the largest still-running chunk further (re-`splitMarkRanges` its remaining sub-range and enqueues the halves), eliminating the tail where one oversized chunk outlives the rest. No change to correctness reasoning — chunks remain disjoint contiguous sub-ranges concatenated in order.
