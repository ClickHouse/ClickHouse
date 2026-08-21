#pragma once
#include <list>

namespace DB
{

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

class IFileCachePriority;
using FileCachePriorityPtr = std::shared_ptr<IFileCachePriority>;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;

struct FileSegmentMetadata;
using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;

struct LockedKey;
using LockedKeyPtr = std::shared_ptr<LockedKey>;

struct KeyMetadata;
using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

}
