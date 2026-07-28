#include <gtest/gtest.h>

#include <Common/BufferAllocationPolicy.h>

#include <base/defines.h>

#include <algorithm>
#include <deque>
#include <limits>

using namespace DB;

namespace
{

/// The largest total size of live upload buffers a writer using this policy reaches with the given in-flight
/// limit, by replaying WriteBufferFromS3::nextImpl over the real BufferAllocationPolicy: it detaches the full
/// buffer, holds the first part back until a second one exists, submits the detached parts one by one - each
/// submission blocked by TaskTracker::add while the in-flight count is at the limit - and only then allocates
/// the next buffer. So it is a check of getMultipartUploadMemory against the code it models rather than
/// against a restated formula.
UInt64 liveBuffersUpperBound(const BufferAllocationPolicy::Settings & settings, size_t max_inflight, size_t buffers)
{
    chassert(max_inflight > 0);

    auto policy = BufferAllocationPolicy::create(settings);

    std::deque<size_t> detached;
    std::deque<size_t> inflight;
    bool multipart_started = false;

    UInt64 worst = 0;
    auto observe = [&](size_t current)
    {
        UInt64 live = current;
        for (auto size : detached)
            live += size;
        for (auto size : inflight)
            live += size;
        worst = std::max(worst, live);
    };

    policy->nextBuffer();
    size_t current_buffer = policy->getBufferSize();
    observe(current_buffer);

    for (size_t i = 1; i < buffers; ++i)
    {
        /// detachBuffer: the full buffer moves to detached_part_data, no buffer is being filled.
        detached.push_back(current_buffer);
        current_buffer = 0;
        observe(current_buffer);

        if (multipart_started || detached.size() > 1)
        {
            multipart_started = true;
            /// writeMultipartUpload: writePart moves the front part into the upload task, and the ones behind
            /// it stay in detached_part_data meanwhile.
            while (!detached.empty())
            {
                inflight.push_back(detached.front());
                detached.pop_front();
                observe(current_buffer);

                /// TaskTracker::add returns only once the in-flight count is below the limit.
                while (inflight.size() >= max_inflight)
                    inflight.pop_front();
            }
        }

        policy->nextBuffer();
        current_buffer = policy->getBufferSize();
        observe(current_buffer);
    }
    return worst;
}

}

TEST(MultipartUploadMemory, ExponentialPolicyBoundsTheLiveBuffers)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 4);

    /// The first buffer, which a writer allocates however little data flows through it.
    EXPECT_EQ(memory.guaranteed, 32 * 1024 * 1024);
    /// Five live buffers (the one being filled plus four in flight), each of which the policy can have
    /// grown to max_size.
    EXPECT_EQ(memory.ceiling, 5 * 5ULL * 1024 * 1024 * 1024);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 4, 2000));
}

TEST(MultipartUploadMemory, ExponentialPolicyFirstBufferFollowsMinUploadPartSize)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 8 * 1024 * 1024;
    settings.min_size = 64 * 1024 * 1024;
    settings.max_size = 1024ULL * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 1);

    /// ExpBufferAllocationPolicy raises the first buffer to min_size when that is the larger of the two.
    EXPECT_EQ(memory.guaranteed, 64 * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 2 * 1024ULL * 1024 * 1024);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 1, 2000));
}

TEST(MultipartUploadMemory, StrictUploadPartSizeUsesTheFixedSizePolicy)
{
    /// A non-zero strict_size switches BufferAllocationPolicy::create to FixedSizeBufferAllocationPolicy, so
    /// every buffer - the first one included - is strict_size, and neither max_single_size nor max_size says
    /// anything about the writer's footprint.
    BufferAllocationPolicy::Settings settings;
    settings.strict_size = 512 * 1024 * 1024;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 3);

    EXPECT_EQ(memory.guaranteed, 512ULL * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 4 * 512ULL * 1024 * 1024);
    /// Every buffer is strict_size, so max_size does not enter the ceiling at all.
    EXPECT_LT(memory.ceiling, settings.max_size);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 3, 2000));

    /// The exponential formula would have under-reported both, which is what makes an up-front reservation
    /// admit too many concurrent merges.
    EXPECT_GT(memory.guaranteed, std::max(settings.max_single_size, settings.min_size));
}

TEST(MultipartUploadMemory, ADetachedBufferCoexistsWithTheBufferBeingFilled)
{
    /// Uniform buffers, so the buffer count is what the numbers below show. With a single in-flight part the
    /// writer still holds two buffers at once: WriteBufferFromS3::nextImpl detaches the first full buffer,
    /// keeps it in detached_part_data - the upload is not even submitted yet, the part is held back until a
    /// second one exists - and allocates the next buffer. Pricing only max_inflight_parts_for_one_file buffers
    /// would therefore under-report the writer by half here, and an admission gate that under-reserves admits
    /// too many concurrent merges.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 32 * 1024 * 1024;
    settings.max_size = 32ULL * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 1);

    EXPECT_EQ(memory.guaranteed, 32ULL * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 2 * 32ULL * 1024 * 1024);

    const UInt64 live = liveBuffersUpperBound(settings, 1, 2000);
    EXPECT_EQ(live, 2 * 32ULL * 1024 * 1024);
    EXPECT_GE(memory.ceiling, live);
    EXPECT_GT(live, 1 * 32ULL * 1024 * 1024);
}

TEST(MultipartUploadMemory, UnlimitedInflightPartsHaveNoCeiling)
{
    /// max_inflight_parts_for_one_file == 0 means unlimited (TaskTracker::waitTilInflightShrink returns
    /// immediately), so no finite ceiling exists - multiplying by zero would collapse it to a single buffer.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 0);

    EXPECT_EQ(memory.guaranteed, 32 * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, MultipartUploadMemory::UNLIMITED);

    BufferAllocationPolicy::Settings strict_settings;
    strict_settings.strict_size = 64 * 1024 * 1024;
    const auto strict_memory = getMultipartUploadMemory(strict_settings, 0);
    EXPECT_EQ(strict_memory.guaranteed, 64 * 1024 * 1024);
    EXPECT_EQ(strict_memory.ceiling, MultipartUploadMemory::UNLIMITED);
}

TEST(MultipartUploadMemory, AbsurdInflightLimitSaturatesInsteadOfWrappingAround)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, std::numeric_limits<UInt64>::max() / 1024);

    EXPECT_EQ(memory.ceiling, MultipartUploadMemory::UNLIMITED);
}
