#pragma once

#include <Common/FileCache_fwd.h>
#include <string>

namespace Poco { namespace Util { class AbstractConfiguration; } }

namespace DB
{

struct FileCacheSettings
{
    size_t max_size = 0;
    size_t max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    bool cache_on_write_operations = false;
    bool do_not_evict_index_and_mark_files = true;
    bool allow_remove_persistent_cache_by_default = true;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

}
