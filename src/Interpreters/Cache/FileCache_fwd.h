#pragma once
#include <memory>
#include <Core/SettingsEnums.h>

namespace DB
{

static constexpr int FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 32 * 1024 * 1024; /// 32Mi
static constexpr int FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT = 4 * 1024 * 1024; /// 4Mi
static constexpr int FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD = 4 * 1024 * 1024; /// 4Mi
static constexpr int FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS = 5;
static constexpr int FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT = 5000;
static constexpr int FILECACHE_DEFAULT_LOAD_METADATA_THREADS = 16;
static constexpr int FILECACHE_DEFAULT_MAX_ELEMENTS = 10000000;
static constexpr size_t FILECACHE_BYPASS_THRESHOLD = 256 * 1024 * 1024;
static constexpr double FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO = 0; /// Disabled.
static constexpr double FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO = 0; /// Disabled.
static constexpr int FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH = 100;
static constexpr auto FILECACHE_DEFAULT_CONFIG_PATH = "filesystem_caches";

static constexpr auto FILECACHE_DEFAULT_CACHE_POLICY = FileCachePolicy::SLRU;

/// SLRU ratio of 0.6 means:
/// 60% of cache for protected elements.
/// 40% of cache for probationary elements.
static constexpr double FILECACHE_DEFAULT_SLRU_RATIO = 0.6;

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheSettings;
struct FileCacheKey;

}
