#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/LockedKey.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileSegmentMetadata::FileSegmentMetadata(
    FileSegmentPtr file_segment_,
    LockedKey & locked_key,
    LockedCachePriority * locked_queue)
    : file_segment(file_segment_)
{
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_segment->state())
    {
        case FileSegment::State::DOWNLOADED:
        {
            if (!locked_queue)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Adding file segment with state DOWNLOADED requires locked queue lock");
            }
            queue_iterator = locked_queue->add(
                file_segment->key(), file_segment->offset(), file_segment->range().size(), locked_key.getKeyMetadata());

            break;
        }
        case FileSegment::State::EMPTY:
        case FileSegment::State::DOWNLOADING:
        {
            break;
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state, got: {}",
                FileSegment::stateToString(file_segment->state()));
    }
}

size_t FileSegmentMetadata::size() const
{
    return file_segment->getReservedSize();
}

const FileSegmentMetadata * KeyMetadata::getByOffset(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

FileSegmentMetadata * KeyMetadata::getByOffset(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

const FileSegmentMetadata * KeyMetadata::tryGetByOffset(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

FileSegmentMetadata * KeyMetadata::tryGetByOffset(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

std::string KeyMetadata::toString() const
{
    std::string result;
    for (auto it = begin(); it != end(); ++it)
    {
        if (it != begin())
            result += ", ";
        result += std::to_string(it->first);
    }
    return result;
}

}
