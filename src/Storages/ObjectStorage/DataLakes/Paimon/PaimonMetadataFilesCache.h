#pragma once
#include <config.h>

#if USE_AVRO

#include <memory>
#include <variant>
#include <vector>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>

namespace ProfileEvents
{
extern const Event PaimonMetadataFilesCacheHits;
extern const Event PaimonMetadataFilesCacheMisses;
extern const Event PaimonMetadataFilesCacheWeightLost;
}

namespace CurrentMetrics
{
extern const Metric PaimonMetadataFilesCacheBytes;
extern const Metric PaimonMetadataFilesCacheFiles;
}

namespace DB
{

using ManifestListConstPtr = std::shared_ptr<const std::vector<PaimonManifestFileMeta>>;
using ManifestConstPtr = std::shared_ptr<const PaimonManifest>;

/// Cache cell that can hold different types of Paimon metadata.
struct PaimonMetadataFilesCacheCell
{
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200; /// We always underestimate dynamic allocations.

    std::variant<
        std::vector<PaimonManifestFileMeta>, /// manifest list parsed result
        PaimonManifest                       /// manifest parsed result
    > cached_element;
    size_t memory_bytes;

    explicit PaimonMetadataFilesCacheCell(std::vector<PaimonManifestFileMeta> && manifest_list, size_t file_bytes_size)
        : cached_element(std::move(manifest_list))
        , memory_bytes(3 * file_bytes_size + SIZE_IN_MEMORY_OVERHEAD)
    {
    }

    explicit PaimonMetadataFilesCacheCell(PaimonManifest && manifest)
        : cached_element(std::move(manifest))
        , memory_bytes(
              3 * std::get<PaimonManifest>(cached_element).file_bytes_size
              + SIZE_IN_MEMORY_OVERHEAD)
    {
    }
};

struct PaimonMetadataFilesCacheWeightFunction
{
    size_t operator()(const PaimonMetadataFilesCacheCell & cell) const { return cell.memory_bytes; }
};

class PaimonMetadataFilesCache
    : public CacheBase<String, PaimonMetadataFilesCacheCell, std::hash<String>, PaimonMetadataFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<String, PaimonMetadataFilesCacheCell, std::hash<String>, PaimonMetadataFilesCacheWeightFunction>;

    PaimonMetadataFilesCache(const String & cache_policy, size_t max_size_bytes, size_t max_count, double size_ratio)
        : Base(
            cache_policy,
            CurrentMetrics::PaimonMetadataFilesCacheBytes,
            CurrentMetrics::PaimonMetadataFilesCacheFiles,
            max_size_bytes,
            max_count,
            size_ratio)
    {
    }

    static String makeKey(const String & table_cache_key_prefix, const String & file_path)
    {
        return table_cache_key_prefix + "/" + file_path;
    }

    template <typename LoadFunc>
    ManifestListConstPtr getOrSetManifestList(const String & key, LoadFunc && load_fn)
    {
        auto load_wrapper = [&]()
        {
            auto [manifest_list, file_bytes_size] = load_fn();
            return std::make_shared<PaimonMetadataFilesCacheCell>(std::move(manifest_list), file_bytes_size);
        };
        auto result = Base::getOrSet(key, load_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PaimonMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PaimonMetadataFilesCacheHits);
        const auto & vec = std::get<std::vector<PaimonManifestFileMeta>>(result.first->cached_element);
        return ManifestListConstPtr(result.first, &vec);
    }

    template <typename LoadFunc>
    ManifestConstPtr getOrSetManifest(const String & key, LoadFunc && load_fn)
    {
        auto load_wrapper = [&]()
        {
            auto manifest = load_fn();
            return std::make_shared<PaimonMetadataFilesCacheCell>(std::move(manifest));
        };
        auto result = Base::getOrSet(key, load_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PaimonMetadataFilesCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PaimonMetadataFilesCacheHits);
        const auto & manifest = std::get<PaimonManifest>(result.first->cached_element);
        return ManifestConstPtr(result.first, &manifest);
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::PaimonMetadataFilesCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using PaimonMetadataFilesCachePtr = std::shared_ptr<PaimonMetadataFilesCache>;

}

#endif
