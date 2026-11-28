#pragma once

#include <algorithm>
#include <optional>

#include <base/types.h>

namespace DB
{

class AdaptiveTopKThreshold;

/// The NULL key of a top-K (see `Aggregator::Params::bucket_top_k`) over a method that keeps the NULL key in
/// a slot of its own (see `keeps_null_key_slot`). The NULL key is a group outside the cells of the table, and
/// it has no hash, so no count bin bounds it (see `AdaptiveTopKPruning`). Its ranks in the producers' tables
/// bound its final rank instead: the final rank of a `count` is the sum of these ranks once every producer
/// finished, and a `uniqExact` merges to at least the largest and at most the sum of them, because a union
/// has no more elements than its parts together. By these bounds, the merge takes the NULL key into the top-K
/// like a group of the bins: the threshold takes a rank of it before any unit is converted
/// (`offerBeforeMerge`), the unit that holds it merges it only while it can reach the top (`canReachTop`),
/// and its exact rank replaces the offered one once its unit is converted (`replaceOffer`). The rules of the
/// three share one invariant: the threshold counts the offered rank only while the unit would still merge the
/// NULL key.
class AdaptiveTopKNullKey
{
public:
    /// Adds the rank of the NULL key in a producer's table. The caller holds the session's
    /// `producer_buffers_mutex`.
    void addProducerRank(UInt64 rank)
    {
        held = true;
        rank_sum += rank;
        rank_max = std::max(rank_max, rank);
    }

    /// A rank that the NULL key does not exceed once every producer finished: the sum of its ranks in the
    /// producers' tables, 0 when no producer held it.
    [[nodiscard]] UInt64 upperBound() const { return rank_sum; }

    /// Offers `threshold` a rank of the NULL key before the merge, so the threshold rises before any unit is
    /// converted, and a large NULL key drops more of the units that the merge tasks merge first. A descending
    /// order takes a bound from below: the sum of the counts, which is the exact count once every producer
    /// finished, or the largest distinct count in one table. An ascending order takes only the exact count,
    /// the sum of the counts once every producer finished, because `canReachTop` judges an ascending NULL key
    /// by its offered rank. A producer without the adaptive context, another aggregator of a mixed projection
    /// pipeline, does not add its NULL key. The caller calls it once, after the finish barrier and before the
    /// merge starts.
    void offerBeforeMerge(AdaptiveTopKThreshold & threshold, bool ascending_, bool exact_ranks, bool every_producer_finished);

    /// Whether the unit that holds the NULL key merges it, at the threshold of the moment. With the count
    /// bins of a descending order (`pruned`), it can reach the top while its upper bound reaches the
    /// threshold. The threshold counts the exact rank that an ascending order offered, so the NULL key must
    /// reach the output while that rank is not worse than the threshold, even once the threshold settled.
    /// Otherwise it is merged unless a settled threshold skips every unit left.
    [[nodiscard]] bool canReachTop(const AdaptiveTopKThreshold & threshold, bool pruned) const;

    /// Replaces the rank offered before the merge by `rank`, the exact rank of the NULL key in the output of
    /// its unit (see `AdaptiveTopKThreshold::replaceOffer`). Returns whether the NULL key had an offered
    /// rank: the caller then leaves the NULL key out of the unit's offer, because an offer counts a group
    /// once.
    bool replaceOffer(AdaptiveTopKThreshold & threshold, UInt64 rank) const;

private:
    bool held = false;
    bool ascending = false;
    UInt64 rank_sum = 0;
    UInt64 rank_max = 0;
    /// The rank that `offerBeforeMerge` offered.
    std::optional<UInt64> offered;
};

}
