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
struct MultipartUploadMemory
{
    /// The number of in-flight parts is unbounded, so no finite ceiling exists.
    static constexpr UInt64 UNLIMITED = std::numeric_limits<UInt64>::max();

    /// The first buffer: a writer allocates it however little data flows through it.
    UInt64 guaranteed = 0;
    /// Every buffer that can be alive at once - the one being filled plus the ones whose uploads are in
    /// flight - or UNLIMITED.
    UInt64 ceiling = 0;
};

/// max_inflight_parts_for_one_file == 0 means unlimited (see TaskTracker::waitTilInflightShrink), which
/// yields MultipartUploadMemory::UNLIMITED as the ceiling.
MultipartUploadMemory getMultipartUploadMemory(const BufferAllocationPolicy::Settings & settings, UInt64 max_inflight_parts_for_one_file);

}
