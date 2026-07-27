#include <gtest/gtest.h>

#include <Common/BufferAllocationPolicy.h>

#include <algorithm>
#include <limits>
#include <vector>

using namespace DB;

namespace
{

/// The largest total size of live upload buffers a writer using this policy can reach with the given
/// in-flight limit: the buffer being filled plus the largest `max_inflight` buffers handed out before it.
/// Walks the real BufferAllocationPolicy, so it is a check of getMultipartUploadMemory against the code it
/// models rather than against a restated formula.
UInt64 liveBuffersUpperBound(const BufferAllocationPolicy::Settings & settings, size_t max_inflight, size_t buffers)
{
    auto policy = BufferAllocationPolicy::create(settings);

    std::vector<size_t> sizes;
    for (size_t i = 0; i < buffers; ++i)
    {
        policy->nextBuffer();
        sizes.push_back(policy->getBufferSize());
    }

    UInt64 worst = 0;
    for (size_t last = 0; last < sizes.size(); ++last)
    {
        /// Buffer `last` is being filled; up to max_inflight of the preceding ones are still uploading.
        std::vector<size_t> preceding(sizes.begin(), sizes.begin() + last);
        std::sort(preceding.begin(), preceding.end(), std::greater<>());

        UInt64 live = sizes[last];
        for (size_t i = 0; i < std::min(max_inflight, preceding.size()); ++i)
            live += preceding[i];

        worst = std::max(worst, live);
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
