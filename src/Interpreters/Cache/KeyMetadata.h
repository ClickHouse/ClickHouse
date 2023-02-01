#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/LockedFileCachePriority.h>

namespace DB
{
class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;

struct FileSegmentMetadata : private boost::noncopyable
{
    FileSegmentPtr file_segment;

    /// Iterator is put here on first reservation attempt, if successful.
    IFileCachePriority::Iterator queue_iterator;

    /// Pointer to file segment is always hold by the cache itself.
    /// Apart from pointer in cache, it can be hold by cache users, when they call
    /// getorSet(), but cache users always hold it via FileSegmentsHolder.
    bool releasable() const { return file_segment.unique(); }

    size_t size() const { return file_segment->reserved_size; }

    FileSegmentMetadata(
        FileSegmentPtr file_segment_,
        KeyTransaction & key_transaction,
        LockedCachePriority * locked_queue);

    FileSegmentMetadata(FileSegmentMetadata && other) noexcept
        : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator)) {}
};


struct KeyMetadata : public std::map<size_t, FileSegmentMetadata>
{
    const FileSegmentMetadata * getByOffset(size_t offset) const;
    FileSegmentMetadata * getByOffset(size_t offset);

    const FileSegmentMetadata * tryGetByOffset(size_t offset) const;
    FileSegmentMetadata * tryGetByOffset(size_t offset);

    std::string toString() const;

    KeyGuardPtr guard = std::make_shared<KeyGuard>();
    bool created_base_directory = false;

    bool removed = false;
};
using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

}
