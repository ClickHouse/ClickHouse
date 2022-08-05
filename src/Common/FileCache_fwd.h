#pragma once
#include <memory>

namespace DB
{

static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 100 * 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS = 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_ENABLE_HITS_THRESHOLD = 0;

class IFileCache;
using FileCachePtr = std::shared_ptr<IFileCache>;

struct FileCacheSettings;
struct CreateFileSegmentSettings;

}
