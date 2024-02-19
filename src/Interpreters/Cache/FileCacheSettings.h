#pragma once

#include <Core/Defines.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <string>

namespace Poco { namespace Util { class AbstractConfiguration; } } // NOLINT(cppcoreguidelines-virtual-class-destructor)

namespace DB
{

struct FileCacheSettings
{
    std::string base_path;

    size_t max_size = 0;
    size_t max_elements = FILECACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;

    bool cache_on_write_operations = false;

    size_t cache_hits_threshold = FILECACHE_DEFAULT_HITS_THRESHOLD;
    bool enable_filesystem_query_cache_limit = false;

    bool enable_bypass_cache_with_threashold = false;
    size_t bypass_cache_threashold = FILECACHE_BYPASS_THRESHOLD;

    size_t boundary_alignment = FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT;
    size_t background_download_threads = FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS;

    size_t load_metadata_threads = FILECACHE_DEFAULT_LOAD_METADATA_THREADS;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

}
