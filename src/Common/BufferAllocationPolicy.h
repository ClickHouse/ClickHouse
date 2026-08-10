#pragma once


#include <base/types.h>

#include <limits>
#include <memory>

namespace DB
{

class BufferAllocationPolicy;
using BufferAllocationPolicyPtr = std::unique_ptr<BufferAllocationPolicy>;

///  Buffer number starts with 0
class BufferAllocationPolicy
{
public:

    struct Settings
    {
        size_t strict_size = 0;
        size_t min_size = 16 * 1024 * 1024;
        size_t max_size = 5ULL * 1024 * 1024 * 1024;
        size_t multiply_factor = 2;
        size_t multiply_parts_count_threshold = 500;
        size_t max_single_size = 32 * 1024 * 1024; /// Max size for a single buffer/block
    };

    virtual size_t getBufferNumber() const = 0;
    virtual size_t getBufferSize() const = 0;
    virtual void nextBuffer() = 0;
    virtual ~BufferAllocationPolicy() = 0;

    static BufferAllocationPolicyPtr create(Settings settings_);

};

/// How much memory a multipart-upload writer (WriteBufferFromS3, WriteBufferFromAzureBlobStorage) can hold
/// in its upload buffers at once, derived from the very settings the writer builds its
/// BufferAllocationPolicy and its TaskTracker from. Callers that must know a writer's footprint before the
/// writer exists - the up-front merge memory reservation - use this instead of reimplementing the policy.
/// This is a CEILING, not an allocation the writer makes up front: even its first buffer starts at the size
/// the caller passed to the writer's constructor and is only grown toward the policy's first-part size once
/// it fills (WriteBufferFromS3::reallocateFirstBuffer), so every byte above that initial buffer is data that
/// has already been written.
struct MultipartUploadMemory
{
    /// The number of in-flight parts is unbounded, so no finite ceiling exists.
    static constexpr UInt64 UNLIMITED = std::numeric_limits<UInt64>::max();

    /// Every buffer that can be alive at once - the one being filled plus max_inflight_parts_for_one_file
    /// detached ones - or UNLIMITED. A detached buffer really does coexist with the buffer being filled:
    /// WriteBufferFromS3::nextImpl detaches the full buffer, keeps it in detached_part_data (it defers the
    /// first upload until a second part exists) and allocates the next buffer, and TaskTracker::add erases a
    /// finished future as soon as the task notifies, while the task - and the PartData it holds - is
    /// destroyed only afterwards.
    UInt64 ceiling = 0;
};

/// max_inflight_parts_for_one_file == 0 means unlimited (see TaskTracker::waitTilInflightShrink), which
/// yields MultipartUploadMemory::UNLIMITED as the ceiling.
MultipartUploadMemory getMultipartUploadMemory(const BufferAllocationPolicy::Settings & settings, UInt64 max_inflight_parts_for_one_file);

/// The in-flight part limit a multipart writer effectively runs with. `S3ObjectStorage::writeObject` /
/// `AzureObjectStorage::writeObject` pass a thread-pool scheduler to the writer only when
/// `s3_allow_parallel_part_upload` / `azure_allow_parallel_part_upload` is set; without one the writer's
/// `TaskTracker` runs every upload inline on `add` (`TaskTracker::syncRunner`), so an upload is finished -
/// and its buffer freed - before the writer returns to filling the next one. The peak is then the same as
/// with an in-flight limit of one (one detached buffer coexisting with the buffer being filled, see
/// MultipartUploadMemory::ceiling), however large - or unlimited, that is zero - the configured limit is.
/// Without this, a background profile that disables parallel part upload would be priced at the configured
/// limit, or at UNLIMITED, for memory the writer can never allocate.
UInt64 getEffectiveMaxInflightParts(UInt64 max_inflight_parts_for_one_file, bool parallel_part_upload_allowed);

}
