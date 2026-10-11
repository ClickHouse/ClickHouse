#include <Common/BufferAllocationPolicy.h>
#include <Core/Defines.h>
#include <base/defines.h>

#include <algorithm>
#include <deque>
#include <memory>

namespace DB
{

class FixedSizeBufferAllocationPolicy : public BufferAllocationPolicy
{
    const size_t buffer_size = 0;
    size_t buffer_number = 0;

public:
    explicit FixedSizeBufferAllocationPolicy(const BufferAllocationPolicy::Settings & settings_)
        : buffer_size(settings_.strict_size)
    {
        chassert(buffer_size > 0);
    }

    size_t getBufferNumber() const override { return buffer_number; }

    size_t getBufferSize() const override
    {
        chassert(buffer_number > 0);
        return buffer_size;
    }

    void nextBuffer() override
    {
        ++buffer_number;
    }
};


class ExpBufferAllocationPolicy : public DB::BufferAllocationPolicy
{
    const size_t first_size = 0;
    const size_t second_size = 0;

    const size_t multiply_factor = 0;
    const size_t multiply_threshold = 0;
    const size_t max_size = 0;

    size_t current_size = 0;
    size_t buffer_number = 0;

public:
    explicit ExpBufferAllocationPolicy(const BufferAllocationPolicy::Settings & settings_)
        /// For min_size <= max_size this is exactly std::clamp(max_single_size, min_size, max_size).
        /// It is spelled out because min_size > max_size is a pair the Azure settings accept unchanged
        /// (there is no validateUploadSettings for them), and std::clamp with lo > hi is undefined
        /// behavior: the first buffer is then min_size - consistent with the second buffer, which is
        /// handed out at min_size unclamped, and with getMultipartUploadMemory below, which prices every
        /// buffer at max(min_size, max_size).
        : first_size(std::min(std::max(settings_.max_single_size, settings_.min_size), std::max(settings_.min_size, settings_.max_size)))
        , second_size(settings_.min_size)
        , multiply_factor(settings_.multiply_factor)
        , multiply_threshold(settings_.multiply_parts_count_threshold)
        , max_size(settings_.max_size)
    {
        chassert(first_size > 0);
        chassert(second_size > 0);
        chassert(multiply_factor >= 1);
        chassert(multiply_threshold > 0);
        chassert(max_size > 0);
    }

    size_t getBufferNumber() const override { return buffer_number; }

    size_t getBufferSize() const override
    {
        chassert(buffer_number > 0);
        return current_size;
    }

    void nextBuffer() override
    {
        ++buffer_number;

        if (1 == buffer_number)
        {
            current_size = first_size;
            return;
        }

        if (2 == buffer_number)
            current_size = second_size;

        if (0 == ((buffer_number - 1) % multiply_threshold))
        {
            current_size *= multiply_factor;
            current_size = std::min(current_size, max_size);
        }
    }
};


BufferAllocationPolicy::~BufferAllocationPolicy() = default;

BufferAllocationPolicyPtr BufferAllocationPolicy::create(BufferAllocationPolicy::Settings settings_)
{
    if (settings_.strict_size > 0)
        return std::make_unique<FixedSizeBufferAllocationPolicy>(settings_);
    return std::make_unique<ExpBufferAllocationPolicy>(settings_);
}

UInt64 getEffectiveMaxInflightParts(UInt64 max_inflight_parts_for_one_file, bool parallel_part_upload_allowed)
{
    if (parallel_part_upload_allowed)
        return max_inflight_parts_for_one_file;
    return 1;
}

MultipartUploadMemory getMultipartUploadMemory(const BufferAllocationPolicy::Settings & settings, UInt64 max_inflight_parts_for_one_file)
{
    MultipartUploadMemory result;
    result.allocation_settings = settings;
    result.max_inflight_parts = max_inflight_parts_for_one_file;

    /// Mirror the two policies of create above. FixedSizeBufferAllocationPolicy hands out buffers of
    /// strict_size, the first one included. ExpBufferAllocationPolicy starts at
    /// min(max(max_single_size, min_size), max(min_size, max_size)) - clamp(max_single_size, min_size,
    /// max_size), defined for min_size > max_size too - hands out min_size (unclamped) as the second
    /// buffer, and grows later buffers up to max_size - so the largest buffer it can ever hand out is
    /// max(min_size, max_size), whatever max_single_size is. Including max_single_size here would overstate
    /// the ceiling when max_single_part_upload_size exceeds max_upload_part_size (a configuration
    /// validateUploadSettings accepts), rejecting wide remote merges for memory the writer can never
    /// allocate.
    UInt64 largest_buffer_size = settings.strict_size;
    if (settings.strict_size == 0)
        largest_buffer_size = std::max<UInt64>(settings.min_size, settings.max_size);

    if (max_inflight_parts_for_one_file == 0)
    {
        result.ceiling = MultipartUploadMemory::UNLIMITED;
        return result;
    }

    /// The buffer being filled plus the detached ones, all of them priced at the largest size the policy can
    /// hand out: by the time a writer holds max_inflight_parts_for_one_file parts in flight, the buffer it is
    /// filling has long grown past the first one. The extra buffer over the in-flight limit is not slack:
    /// WriteBufferFromS3::nextImpl detaches a full buffer and allocates the next one before submitting the
    /// first upload (it holds the part back until a second one exists), so two buffers are alive with nothing
    /// in flight at all, and TaskTracker::add erases a finished future as soon as the task notifies, while
    /// the task still owns its PartData for a moment longer. Saturate rather than wrap around - a
    /// configuration that allows an enormous number of in-flight parts is indistinguishable from an unlimited
    /// one here.
    UInt64 live_buffers = 0;
    if (__builtin_add_overflow(max_inflight_parts_for_one_file, static_cast<UInt64>(1), &live_buffers)
        || __builtin_mul_overflow(live_buffers, largest_buffer_size, &result.ceiling))
        result.ceiling = MultipartUploadMemory::UNLIMITED;

    return result;
}

namespace
{

/// Neither `WriteBufferFromS3` nor `WriteBufferFromAzureBlobStorage` allocates the allocation policy's first
/// buffer up front: both start from the buffer the caller asks for, capped at `DBMS_DEFAULT_BUFFER_SIZE` in
/// the constructor, and `reallocateFirstBuffer` doubles that allocation - capped by the policy's first buffer
/// size - every time it fills up. Replay that growth, so that output which does not even fill the first
/// buffer is not priced at the whole first part size. Returns false when the doubling overflows.
bool replayFirstBufferGrowth(UInt64 first_policy_buffer_size, UInt64 bytes_written, UInt64 & first_buffer_size)
{
    first_buffer_size = std::min<UInt64>(first_policy_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);
    while (bytes_written > first_buffer_size && first_buffer_size < first_policy_buffer_size)
    {
        if (__builtin_mul_overflow(first_buffer_size, static_cast<UInt64>(2), &first_buffer_size))
            return false;
        first_buffer_size = std::min(first_buffer_size, first_policy_buffer_size);
    }
    return true;
}

}

UInt64 getMultipartUploadMemoryCeilingForWrittenBytes(const MultipartUploadMemory & memory, UInt64 bytes_written)
{
    if (memory.ceiling == 0 || bytes_written == 0)
        return memory.ceiling;

    /// An unlimited in-flight-part setting has no finite bound, regardless of which buffer tiers this
    /// amount of output can reach. In particular, do not turn it into one reachable buffer by calculating
    /// max_inflight_parts + 1 below: TaskTracker::waitTilInflightShrink does not constrain the number of
    /// detached buffers when this setting is zero.
    if (memory.ceiling == MultipartUploadMemory::UNLIMITED)
        return MultipartUploadMemory::UNLIMITED;

    const auto & settings = memory.allocation_settings;
    UInt64 largest_reachable_buffer = settings.strict_size;
    if (settings.strict_size != 0)
    {
        /// The writer does not allocate a strict-size first buffer up front. It starts with at most
        /// DBMS_DEFAULT_BUFFER_SIZE and doubles that allocation in nextImpl until it reaches strict_size.
        /// After that, each complete strict-size part can leave one detached or in-flight buffer behind
        /// while the next buffer is being filled. Do not charge more such buffers than the written data
        /// can reach, otherwise a small merge with a large strict upload part size is rejected for memory
        /// the writer cannot allocate.
        UInt64 first_buffer_size = 0;
        if (!replayFirstBufferGrowth(settings.strict_size, bytes_written, first_buffer_size))
            return MultipartUploadMemory::UNLIMITED;

        if (bytes_written <= first_buffer_size)
            return first_buffer_size;

        UInt64 reachable_buffers = bytes_written / settings.strict_size;
        if (bytes_written % settings.strict_size)
            ++reachable_buffers;
        const UInt64 live_buffers = std::min(memory.max_inflight_parts + 1, reachable_buffers);
        if (__builtin_mul_overflow(live_buffers, settings.strict_size, &largest_reachable_buffer))
            return MultipartUploadMemory::UNLIMITED;
        return std::min(memory.ceiling, largest_reachable_buffer);
    }
    else
    {
        auto policy = BufferAllocationPolicy::create(settings);
        policy->nextBuffer();
        const UInt64 first_policy_buffer_size = policy->getBufferSize();

        /// The first buffer is grown into, not allocated up front, exactly like in the strict-size branch
        /// above. While the output fits into it, that grown buffer is all the writer holds: nothing has been
        /// detached, so no policy-sized buffer exists yet. Charging the policy's first part here would
        /// over-reserve a small remote merge by the whole first tier - 32 MiB per stream with the default S3
        /// sizing - and close `merges_mutations_memory_usage_soft_limit` on memory the writer never allocates.
        UInt64 first_buffer_size = 0;
        if (!replayFirstBufferGrowth(first_policy_buffer_size, bytes_written, first_buffer_size))
            return MultipartUploadMemory::UNLIMITED;

        if (bytes_written <= first_buffer_size)
            return std::min(memory.ceiling, first_buffer_size);

        UInt64 written = 0;
        UInt64 live_memory = 0;
        UInt64 peak_live_memory = 0;
        UInt64 live_buffers = 0;
        if (__builtin_add_overflow(memory.max_inflight_parts, static_cast<UInt64>(1), &live_buffers))
            return MultipartUploadMemory::UNLIMITED;

        /// A multipart writer keeps the current buffer together with the most recent detached or in-flight
        /// buffers. The exponential policy may have several older, smaller buffers alive while the current
        /// buffer has reached a larger tier, so multiplying every live slot by the largest reached tier
        /// over-reserves small merges. Replay the policy and retain exactly the live allocation window.
        std::deque<UInt64> allocated_buffers;
        UInt64 buffer_size = first_policy_buffer_size;
        while (true)
        {
            allocated_buffers.push_back(buffer_size);
            if (__builtin_add_overflow(live_memory, buffer_size, &live_memory))
                return MultipartUploadMemory::UNLIMITED;

            if (allocated_buffers.size() > live_buffers)
            {
                live_memory -= allocated_buffers.front();
                allocated_buffers.pop_front();
            }

            peak_live_memory = std::max(peak_live_memory, live_memory);
            if (written >= bytes_written)
                break;
            written += std::min(buffer_size, bytes_written - written);
            if (written == bytes_written)
                break;

            policy->nextBuffer();
            buffer_size = policy->getBufferSize();
        }

        return std::min(memory.ceiling, peak_live_memory);
    }
}

}
