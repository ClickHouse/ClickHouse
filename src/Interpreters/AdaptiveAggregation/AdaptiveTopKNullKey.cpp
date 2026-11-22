#include <Interpreters/AdaptiveAggregation/AdaptiveTopKNullKey.h>

#include <Interpreters/AdaptiveAggregation/AdaptiveTopKThreshold.h>

namespace DB
{

void AdaptiveTopKNullKey::offerBeforeMerge(
    AdaptiveTopKThreshold & threshold, bool ascending_, bool exact_ranks, bool every_producer_finished)
{
    ascending = ascending_;
    if (!held)
        return;
    if (!ascending)
        offered = exact_ranks ? rank_sum : rank_max;
    else if (exact_ranks && every_producer_finished)
        offered = rank_sum;
    if (offered)
        threshold.offer({&*offered, 1});
}

bool AdaptiveTopKNullKey::canReachTop(const AdaptiveTopKThreshold & threshold, bool pruned) const
{
    if (pruned)
        return rank_sum >= threshold.current();
    if (ascending && offered)
        return *offered <= threshold.current();
    return !threshold.settled();
}

bool AdaptiveTopKNullKey::replaceOffer(AdaptiveTopKThreshold & threshold, UInt64 rank) const
{
    if (!offered)
        return false;
    if (rank != *offered)
        threshold.replaceOffer(*offered, rank);
    return true;
}

}
