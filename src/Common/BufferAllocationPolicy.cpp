#include <Common/BufferAllocationPolicy.h>
#include <base/defines.h>

#include <algorithm>
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
        : first_size(std::clamp(settings_.max_single_size, settings_.min_size, settings_.max_size))
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

    /// Mirror the two policies of create above. FixedSizeBufferAllocationPolicy hands out buffers of
    /// strict_size, the first one included; ExpBufferAllocationPolicy starts at max_single_size (raised to
    /// min_size when that is larger) and grows later buffers up to max_size.
    UInt64 largest_buffer_size = settings.strict_size;
    if (settings.strict_size == 0)
        largest_buffer_size = std::max<UInt64>({settings.max_single_size, settings.min_size, settings.max_size});

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

}

