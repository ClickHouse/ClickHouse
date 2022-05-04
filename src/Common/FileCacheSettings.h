#pragma once

#include <string>
#include <Common/FileCache_fwd.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

struct FileCacheSettings
{
    std::string cache_method = "ARC";
    double size_ratio = 0.2; /// just for ARC
    int move_threshold = 4; /// just for ARC
    size_t max_size = 0;
    size_t max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    bool cache_on_write_operations = false;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

}
