#pragma once

#include <Common/FileCache_fwd.h>

namespace Poco { namespace Util { class AbstractConfiguration; } } // NOLINT(cppcoreguidelines-virtual-class-destructor)

namespace DB
{

struct FileCacheSettings
{
    size_t max_size = 0;
    size_t max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;

    bool cache_on_write_operations = false;

    size_t enable_cache_hits_threshold = REMOTE_FS_OBJECTS_CACHE_ENABLE_HITS_THRESHOLD;
    bool enable_filesystem_query_cache_limit = false;

    bool do_not_evict_index_and_mark_files = true;
    bool allow_to_remove_persistent_segments_from_cache_by_default = true;

    /// In bytes.
    size_t background_download_max_memory_usage = REMOTE_FS_OBJECTS_CACHE_BACKGROUND_DOWNLOAD_MEMORY_USAGE;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

}
