#include "FileCacheSettings.h"

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    max_size = config.getUInt64(config_prefix + ".data_cache_max_size", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_CACHE_SIZE);
    max_elements = config.getUInt64(config_prefix + ".data_cache_max_elements", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS);
    max_file_segment_size = config.getUInt64(config_prefix + ".max_file_segment_size", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE);
    cache_on_write_operations = config.getUInt64(config_prefix + ".cache_on_write_operations", false);
    enable_filesystem_query_cache_limit = config.getUInt64(config_prefix + ".enable_filesystem_query_cache_limit", false);
    enable_cache_hits_threshold = config.getUInt64(config_prefix + ".enable_cache_hits_threshold", REMOTE_FS_OBJECTS_CACHE_ENABLE_HITS_THRESHOLD);
    do_not_evict_index_and_mark_files = config.getUInt64(config_prefix + ".do_not_evict_index_and_mark_files", true);
    allow_to_remove_persistent_segments_from_cache_by_default = config.getUInt64(config_prefix + ".allow_to_remove_persistent_segments_from_cache_by_default", true);
    background_downlaod_max_memory_usage = config.getUInt64(config_prefix + ".background_downlaod_max_memory_usage", REMOTE_FS_OBJECTS_CACHE_BACKGROUND_DOWNLOAD_MEMORY_USAGE);
}

}
