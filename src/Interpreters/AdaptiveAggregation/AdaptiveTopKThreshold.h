#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

/// The threshold of a top-K that the merge threads of an adaptive aggregation share: the worst of the `limit`
/// best ranks that the converted merge units have output, a floor for a descending order and a ceiling for an
/// ascending one. The ranks are those of `Aggregator::Params::bucket_top_k`: a `count`, `uniqExact` or
/// `uniqExactIf`. Every group lives in one unit, and a converted unit outputs its groups with exact ranks,
/// because it holds every record and source cell of its keys. So a group ranked worse than the threshold has
/// `limit` output groups ahead of it and cannot reach the top: the conversions and the count-first units drop
/// it, and the count bins skip the bins bounded below a floor (see `AdaptiveTopKPruning`). Before the first
/// selection, the threshold is the worst rank of the order, which drops nothing.
///
/// Settling: when no group can rank better than `best_possible_rank`, a threshold that reaches it means that
/// every group the merge has not converted can at best tie with `limit` output groups. `LIMIT` may return any
/// of the tied groups, so the merge skips the units left without draining them. For example,
/// `GROUP BY URL ORDER BY count() ASC LIMIT 10` over 18M groups, 11M of them with one row, settles with its
/// first unit, because no count is below 1.
///
/// Every merge thread offers the output of each unit under one mutex, so the work under it must stay constant
/// per rank. A heap of the best ranks would cost a logarithmic sift with cache misses per rank, which
/// measured tens of milliseconds under the mutex at `LIMIT 50000` over 64 threads. Instead, the ranks better
/// than the threshold are appended, and a selection keeps the `limit` best of them and publishes the worst.
/// The first selection runs at `limit` kept ranks, and every later one once the kept ranks exceed `limit` by
/// an eighth of it: with `limit` 50000, at 50000 kept ranks and then after every 6250 more. A selection reads
/// every kept rank once, so each kept rank costs a few dozen sequential comparisons. The frequent selections
/// keep the threshold close to the exact one even when the units output far fewer groups than `limit`.
///
/// A published threshold is exact: a rank no better than it can join the `limit` best only as a tie with
/// their worst, which leaves the threshold the same. Between selections the threshold lags the offers, which
/// only drops fewer groups. For example, with `limit` 3 and a descending order, the offers 5, 9 and 2 publish
/// the threshold 2. The offers 7 and 8 that follow publish 5 and then 7, because with a `limit` this small
/// every kept rank past it selects again. Later offers of 1 and 6 are dropped, as they are no better than 7.
///
/// `TopKThresholdCollector` publishes the same kind of value for the sorting of `ORDER BY ... LIMIT`, but it
/// takes sorted `IColumn` prefixes of at most 1000 rows, while a conversion outputs unsorted `UInt64` ranks
/// and `limit` reaches `Aggregator::Params::max_bucket_top_k`.
class AdaptiveTopKThreshold
{
public:
    /// The caller guarantees that `limit_` is positive and, when `best_possible_rank_` is set, that no group
    /// ranks better than it.
    AdaptiveTopKThreshold(size_t limit_, bool ascending_, std::optional<UInt64> best_possible_rank_);

    /// The threshold of the last selection, or the worst rank of the order before the first one. The read
    /// takes no lock, so it can lag the offers of other threads, which only drops fewer groups.
    ALWAYS_INLINE UInt64 current() const { return threshold.load(std::memory_order_relaxed); }

    /// Whether the threshold reached `best_possible_rank`, so that the merge can skip every unit left.
    bool settled() const { return best_possible_rank && current() == *best_possible_rank; }

    /// Offers the ranks of the groups that a converted unit output. The caller guarantees that each rank is
    /// the final rank of its group, merged from every source, and that every group is offered at most once
    /// and reaches the output.
    void offer(std::span<const UInt64> ranks);

private:
    template <typename Better>
    void offerImpl(std::span<const UInt64> ranks);

    /// Keeps the `limit` best of the kept ranks, publishes the worst of them and returns it.
    template <typename Better>
    UInt64 selectBest() TSA_REQUIRES(mutex);

    const size_t limit;
    const bool ascending;
    const std::optional<UInt64> best_possible_rank;
    /// How many ranks past `limit` the kept ranks reach before the next selection: an eighth of `limit`, at
    /// least one.
    const size_t selection_slack;

    std::mutex mutex;
    std::atomic<UInt64> threshold;
    /// The ranks better than the threshold when they were offered: the `limit` best of the last selection,
    /// then the ranks offered after it.
    std::vector<UInt64> kept TSA_GUARDED_BY(mutex);
    /// How many kept ranks trigger the next selection: `limit` for the first one, then
    /// `limit + selection_slack`.
    size_t select_at TSA_GUARDED_BY(mutex);
};

}
