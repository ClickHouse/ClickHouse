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
extern const Event PaimonMetadataCacheHits;
extern const Event PaimonMetadataCacheMisses;
}

namespace CurrentMetrics
{
extern const Metric PaimonMetadataCacheBytes;
extern const Metric PaimonMetadataCacheFiles;
}

namespace DB
{

using namespace Paimon;


/// Cache cell that can hold different types of Paimon metadata
struct PaimonMetadataCacheCell
{
    std::variant<
        std::vector<PaimonManifestFileMeta>,  /// manifest list parsed result
        PaimonManifest                         /// manifest parsed result
    > cached_element;

    size_t sizeInBytes() const
    {
        return std::visit(
            [](const auto & elem) -> size_t
            {
                using T = std::decay_t<decltype(elem)>;
                if constexpr (std::is_same_v<T, std::vector<PaimonManifestFileMeta>>)
                {
                    size_t size = sizeof(std::vector<PaimonManifestFileMeta>);
                    for (const auto & meta : elem)
                        size += sizeof(meta) + meta.file_name.size();
                    return size;
                }
                else if constexpr (std::is_same_v<T, PaimonManifest>)
                {
                    size_t size = sizeof(PaimonManifest);
                    for (const auto & entry : elem.entries)
                        size += sizeof(entry) + entry.partition.size() + entry.file.file_name.size() + entry.file.bucket_path.size();
                    return size;
                }
                return 0;
            },
            cached_element);
    }
};

struct PaimonMetadataCacheWeightFunction
{
    size_t operator()(const PaimonMetadataCacheCell & cell) const { return cell.sizeInBytes(); }
};

/// Caches parsed manifest lists and manifest files.
///
/// Note: This cache is optional and can be disabled.
/// When disabled (nullptr in PaimonPersistentComponents),
/// metadata files will be loaded directly from object storage on each access.
/// This is similar to Iceberg's use_iceberg_metadata_files_cache setting.
///
/// In the future, this can be controlled by a setting like:
///   use_paimon_metadata_files_cache (default: false)
class PaimonMetadataCache
    : public CacheBase<String, PaimonMetadataCacheCell, std::hash<String>, PaimonMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<String, PaimonMetadataCacheCell, std::hash<String>, PaimonMetadataCacheWeightFunction>;

    PaimonMetadataCache(const String & cache_policy, size_t max_size_bytes, size_t max_count, double size_ratio)
        : Base(
              cache_policy,
              CurrentMetrics::PaimonMetadataCacheBytes,
              CurrentMetrics::PaimonMetadataCacheFiles,
              max_size_bytes,
              max_count,
              size_ratio)
    {
    }

    static String makeKey(const String & table_path, const String & file_path) { return table_path + "/" + file_path; }

    /// Get or load manifest list.
    template <typename LoadFunc>
    std::vector<PaimonManifestFileMeta> getOrSetManifestList(const String & key, LoadFunc && load_fn)
    {
        auto load_wrapper = [&]()
        { return std::make_shared<PaimonMetadataCacheCell>(PaimonMetadataCacheCell{load_fn()}); };
        auto result = Base::getOrSet(key, load_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PaimonMetadataCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PaimonMetadataCacheHits);
        return std::get<std::vector<PaimonManifestFileMeta>>(result.first->cached_element);
    }

    template <typename LoadFunc>
    PaimonManifest getOrSetManifest(const String & key, LoadFunc && load_fn)
    {
        auto load_wrapper = [&]() { return std::make_shared<PaimonMetadataCacheCell>(PaimonMetadataCacheCell{load_fn()}); };
        auto result = Base::getOrSet(key, load_wrapper);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PaimonMetadataCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PaimonMetadataCacheHits);
        return std::get<PaimonManifest>(result.first->cached_element);
    }

    bool contains(const String & key) const { return Base::contains(key); }

    void remove(const String & key) { Base::remove(key); }

    void clear() { Base::clear(); }
};

using PaimonMetadataCachePtr = std::shared_ptr<PaimonMetadataCache>;

}

#endif

