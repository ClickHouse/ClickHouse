#pragma once
#include <memory>

namespace DB
{

static constexpr int FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 100 * 1024 * 1024;
static constexpr int FILECACHE_DEFAULT_MAX_ELEMENTS = 1024 * 1024;
static constexpr int FILECACHE_DEFAULT_HITS_THRESHOLD = 0;
static constexpr size_t FILECACHE_BYPASS_THRESHOLD = 256 * 1024 * 1024;
static constexpr size_t FILECACHE_DELAYED_CLEANUP_INTERVAL_MS = 1000 * 60; /// 1 min

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheSettings;
struct CreateFileSegmentSettings;

}
