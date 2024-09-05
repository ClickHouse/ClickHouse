#pragma once

#include <functional>
#include <string>
#include <Core/Defines.h>
#include <Interpreters/Cache/FileCache_fwd.h>

namespace Poco { namespace Util { class AbstractConfiguration; } } // NOLINT(cppcoreguidelines-virtual-class-destructor)

namespace DB
{
class NamedCollection;

struct FileCacheSettings
{
    std::string base_path;

    size_t max_size = 0;
    size_t max_elements = FILECACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;

    bool cache_on_write_operations = false;

    size_t cache_hits_threshold = FILECACHE_DEFAULT_HITS_THRESHOLD;
    bool enable_filesystem_query_cache_limit = false;

    bool enable_bypass_cache_with_threshold = false;
    size_t bypass_cache_threshold = FILECACHE_BYPASS_THRESHOLD;

    size_t boundary_alignment = FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT;
    size_t background_download_threads = FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS;
    size_t background_download_queue_size_limit = FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT;

    size_t load_metadata_threads = FILECACHE_DEFAULT_LOAD_METADATA_THREADS;
    bool load_metadata_asynchronously = false;

    bool write_cache_per_user_id_directory = false;

    std::string cache_policy = "LRU";
    double slru_size_ratio = 0.5;

    double keep_free_space_size_ratio = FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO;
    double keep_free_space_elements_ratio = FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO;
    size_t keep_free_space_remove_batch = FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
    void loadFromCollection(const NamedCollection & collection);

    std::string toString() const;
    std::vector<std::string> getSettingsDiff(const FileCacheSettings & other) const;

    bool operator ==(const FileCacheSettings &) const = default;

private:
    using FuncHas = std::function<bool(std::string_view)>;
    using FuncGetUInt = std::function<size_t(std::string_view)>;
    using FuncGetString = std::function<std::string(std::string_view)>;
    using FuncGetDouble = std::function<double(std::string_view)>;
    void loadImpl(FuncHas has, FuncGetUInt get_uint, FuncGetString get_string, FuncGetDouble get_double);
};

}
