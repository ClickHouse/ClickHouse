#pragma once
#include <memory>

namespace DB
{

static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 100 * 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS = 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_HITS_THRESHOLD = 0;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_BYPASS_THRESHOLD = 256 * 1024 * 1024;

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheSettings;
struct CreateFileSegmentSettings;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;

struct FileSegmentsHolder;
using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

}
