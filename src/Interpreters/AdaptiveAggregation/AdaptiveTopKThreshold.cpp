#include <Interpreters/AdaptiveAggregation/AdaptiveTopKThreshold.h>

#include <algorithm>
#include <functional>
#include <limits>

#include <base/sort.h>

namespace DB
{

AdaptiveTopKThreshold::AdaptiveTopKThreshold(size_t limit_, bool ascending_, std::optional<UInt64> best_possible_rank_)
    : limit(limit_)
    , ascending(ascending_)
    , best_possible_rank(best_possible_rank_)
    , selection_slack(std::max<size_t>(limit_ / 8, 1))
    , threshold(ascending_ ? std::numeric_limits<UInt64>::max() : 0)
    , select_at(limit_)
{
}

void AdaptiveTopKThreshold::offer(std::span<const UInt64> ranks)
{
    if (ascending)
        offerImpl<std::less<>>(ranks);
    else
        offerImpl<std::greater<>>(ranks);
}

template <typename Better>
void AdaptiveTopKThreshold::offerImpl(std::span<const UInt64> ranks)
{
    std::lock_guard lock(mutex);
    UInt64 worst = threshold.load(std::memory_order_relaxed);
    for (const UInt64 rank : ranks)
    {
        if (!Better{}(rank, worst))
            continue;
        kept.push_back(rank);
        if (kept.size() == select_at)
            worst = selectBest<Better>();
    }
}

template <typename Better>
UInt64 AdaptiveTopKThreshold::selectBest()
{
    ::nth_element(kept.begin(), kept.begin() + (limit - 1), kept.end(), Better{});
    kept.resize(limit);
    const UInt64 worst = kept.back();
    threshold.store(worst, std::memory_order_relaxed);
    select_at = limit + selection_slack;
    return worst;
}

}
