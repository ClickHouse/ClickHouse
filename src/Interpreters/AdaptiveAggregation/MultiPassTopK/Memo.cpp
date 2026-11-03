#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Memo.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <base/unit.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool adaptive_aggregator_top_k_cache_candidates;
}

namespace MultiPassTopKImpl
{

namespace
{

/// The most queries whose memos are kept (see `TopKMemo`), as many as the hash table statistics keep by default, and
/// the most bytes the memos take.
constexpr size_t max_kept_memos = 10'000;
constexpr size_t max_kept_memo_bytes = 64_MiB;

struct TopKMemoWeight
{
    size_t operator()(const TopKMemo & memo) const { return sizeof(TopKMemo) + memo.candidate_keys.allocatedBytes(); }
};

using TopKMemoCache = CacheBase<UInt64, TopKMemo, std::hash<UInt64>, TopKMemoWeight>;

TopKMemoCache & topKMemoCache()
{
    static TopKMemoCache cache(CurrentMetrics::end(), CurrentMetrics::end(), max_kept_memo_bytes, max_kept_memos);
    return cache;
}

/// The key of the memo of a query: the key of the hash table statistics of its aggregation, which hashes
/// the input of the aggregation and its keys, mixed with the top the query takes and its rank. Zero when the
/// statistics are disabled.
UInt64 cacheKeyOf(const MultiPassTopKTemplate & templ)
{
    const UInt64 statistics_key = templ.aggregating->getParams().stats_collecting_params.key;
    if (!statistics_key)
        return 0;
    SipHash hash;
    hash.update(statistics_key);
    hash.update(templ.top);
    hash.update(templ.rank_index);
    return hash.get64();
}

bool settingCachesCandidates(const MultiPassTopKTemplate & templ)
{
    return templ.input->context->getSettingsRef()[Setting::adaptive_aggregator_top_k_cache_candidates];
}

}

TopKMemoPtr memoForRun(const MultiPassTopKTemplate & templ)
{
    const UInt64 cache_key = cacheKeyOf(templ);
    if (!cache_key)
        return nullptr;
    auto memo = topKMemoCache().get(cache_key);
    if (!memo)
        return nullptr;
    switch (memo->mode)
    {
        case TopKMemo::Mode::Original:
            return memo->runs_without_pilot.fetch_sub(1) > 0 ? memo : nullptr;
        case TopKMemo::Mode::Candidates:
        {
            if (!settingCachesCandidates(templ))
                return nullptr;
            const auto & input = *templ.aggregating->getInputHeaders().front();
            Block keys;
            for (const auto & name : templ.aggregating->getParams().keys)
                keys.insert(input.getByName(name).cloneEmpty());
            return isCompatibleHeader(keys, memo->candidate_keys) ? memo : nullptr;
        }
        case TopKMemo::Mode::Bounding:
            return templ.input->bounding_key ? memo : nullptr;
    }
    UNREACHABLE();
}

TopKRunMemo::TopKRunMemo(const MultiPassTopKTemplate & templ, TopKMemoPtr start_)
    : cache_key(cacheKeyOf(templ))
    , caches_candidates(settingCachesCandidates(templ))
    , start(std::move(start_))
{
}

void TopKRunMemo::keep(TopKMemo::Mode mode, Block keys, bool learned)
{
    if (cache_key)
        topKMemoCache().set(cache_key, std::make_shared<TopKMemo>(mode, std::move(keys), learned));
}

}

}
