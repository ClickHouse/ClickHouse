#include <Common/CacheBase.h>

#include <cstddef>

namespace DB
{

struct RuntimeDataflowStatistics
{
    size_t input_bytes = 0;
    size_t output_bytes = 0;
};

class RuntimeDataflowStatisticsCache
{
public:
    using Entry = RuntimeDataflowStatistics;
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;

    std::optional<Entry> getStats([[maybe_unused]] size_t key) const
    {
        return RuntimeDataflowStatistics{.input_bytes = 0, .output_bytes = 0};
    }

    void update([[maybe_unused]] const Entry & new_entry, [[maybe_unused]] size_t key) { }

private:
    // CachePtr getHashTableStatsCache(size_t key) const;

    mutable std::mutex mutex;
    CachePtr stats_cache TSA_GUARDED_BY(mutex);
};

inline RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}

}
