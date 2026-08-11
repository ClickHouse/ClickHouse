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

UInt64 firstPolicyBufferSize(const BufferAllocationPolicy::Settings & settings)
{
    auto policy = BufferAllocationPolicy::create(settings);
    policy->nextBuffer();
    return policy->getBufferSize();
}

/// The memory a writer holds while its FIRST buffer is being filled, after `written` bytes have gone through
/// it. WriteBufferFromS3 does NOT allocate the allocation policy's first part up front: BufferWithOwnMemory is
/// constructed with the size the caller passes (S3ObjectStorage::writeObject hands it the stream's own
/// buf_size), allocateBuffer only shrinks it when it exceeds the policy size, and reallocateFirstBuffer
/// doubles it - capped by the policy size - on the nextImpl path, that is only once it has actually filled up.
UInt64 firstBufferMemoryAfterWriting(const BufferAllocationPolicy::Settings & settings, UInt64 initial_buffer_size, UInt64 written)
{
    const UInt64 max_first_buffer = firstPolicyBufferSize(settings);
    UInt64 memory = std::min(initial_buffer_size, max_first_buffer);

    UInt64 filled = 0;
    while (filled < written)
    {
        filled += std::min(memory - filled, written - filled);
        if (filled == written)
            break;
        /// The buffer is full and there is more to write: nextImpl grows the first buffer, or - once it is at
        /// the policy size - detaches it and moves on to the multipart buffers priced by the ceiling.
        if (memory == max_first_buffer)
            break;
        memory = std::min(memory * 2, max_first_buffer);
    }
    return memory;
}

}

TEST(MultipartUploadMemory, ExponentialPolicyBoundsTheLiveBuffers)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 4);

    /// The first buffer the policy hands out - which the writer grows into as data fills it, rather than
    /// allocating it up front (see TheFirstBufferIsNotPinnedUpFront below).
    EXPECT_EQ(firstPolicyBufferSize(settings), 32 * 1024 * 1024);
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
    EXPECT_EQ(firstPolicyBufferSize(settings), 64 * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 2 * 1024ULL * 1024 * 1024);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 1, 2000));
}

TEST(MultipartUploadMemory, MaxSinglePartUploadSizeDoesNotInflateTheCeiling)
{
    /// max_single_part_upload_size above max_upload_part_size is a supported configuration
    /// (S3RequestSettings::validateUploadSettings never constrains it), but the policy clamps the first
    /// buffer down to max_size and never hands out anything larger, so the ceiling must not price the
    /// buffers at max_single_size - that would reject wide remote merges for memory the writer can never
    /// allocate.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 256 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 64ULL * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 20);

    EXPECT_EQ(firstPolicyBufferSize(settings), 64ULL * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 21 * 64ULL * 1024 * 1024);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 20, 2000));
}

TEST(MultipartUploadMemory, MinUploadPartSizeAboveMaxStillPricesTheMinSizeBuffers)
{
    /// min_size above max_size is accepted for Azure (there is no validateUploadSettings there at all), and
    /// ExpBufferAllocationPolicy hands out min_size unclamped as the second buffer, so the ceiling must keep
    /// pricing the buffers at min_size - dropping to max_size alone would under-report the writer 4x here,
    /// and an admission gate that under-reserves admits merges that then exceed their reservation. The policy
    /// itself is not replayed here: its first-buffer std::clamp has the lo > hi precondition violated by this
    /// configuration, which is exactly why the ceiling must stay conservative about it.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 256 * 1024 * 1024;
    settings.max_size = 64ULL * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 1);

    EXPECT_EQ(memory.ceiling, 2 * 256ULL * 1024 * 1024);
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

    EXPECT_EQ(firstPolicyBufferSize(settings), 512ULL * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 4 * 512ULL * 1024 * 1024);
    /// Every buffer is strict_size, so max_size does not enter the ceiling at all.
    EXPECT_LT(memory.ceiling, settings.max_size);
    EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, 3, 2000));

    /// The exponential formula would have under-reported both, which is what makes an up-front reservation
    /// admit too many concurrent merges.
    EXPECT_GT(firstPolicyBufferSize(settings), std::max(settings.max_single_size, settings.min_size));
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

    EXPECT_EQ(firstPolicyBufferSize(settings), 32ULL * 1024 * 1024);
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

    EXPECT_EQ(memory.ceiling, MultipartUploadMemory::UNLIMITED);

    BufferAllocationPolicy::Settings strict_settings;
    strict_settings.strict_size = 64 * 1024 * 1024;
    const auto strict_memory = getMultipartUploadMemory(strict_settings, 0);
    EXPECT_EQ(strict_memory.ceiling, MultipartUploadMemory::UNLIMITED);
}

TEST(MultipartUploadMemory, WithoutParallelPartUploadTheCeilingIsTwoBuffers)
{
    /// `S3ObjectStorage::writeObject` / `AzureObjectStorage::writeObject` give the writer a thread-pool
    /// scheduler only when `s3_allow_parallel_part_upload` / `azure_allow_parallel_part_upload` is set.
    /// Without one the writer's TaskTracker runs every upload inline (TaskTracker::syncRunner), so an upload
    /// is done - and its buffer released - before the writer resumes filling the next one, and the peak is
    /// the same as with an in-flight limit of one: one detached buffer plus the buffer being filled. The
    /// configured limit, however large or unlimited, cannot be reached, so pricing it would reserve memory
    /// the writer can never allocate.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    for (const UInt64 configured_inflight : {0UL, 1UL, 4UL, 100UL})
    {
        const UInt64 effective = getEffectiveMaxInflightParts(configured_inflight, /*parallel_part_upload_allowed=*/false);
        EXPECT_EQ(effective, 1u);

        const auto memory = getMultipartUploadMemory(settings, effective);
        EXPECT_EQ(memory.ceiling, 2 * 5ULL * 1024 * 1024 * 1024);
        EXPECT_GE(memory.ceiling, liveBuffersUpperBound(settings, effective, 2000));

        /// With parallel upload allowed the configured limit is used as it stands - unlimited included.
        EXPECT_EQ(getEffectiveMaxInflightParts(configured_inflight, /*parallel_part_upload_allowed=*/true), configured_inflight);
    }

    EXPECT_EQ(getMultipartUploadMemory(settings, getEffectiveMaxInflightParts(0, true)).ceiling, MultipartUploadMemory::UNLIMITED);
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

TEST(MultipartUploadMemory, TheFirstBufferIsNotPinnedUpFront)
{
    /// Default S3 sizing: the allocation policy's first part is 32 MiB, but a merge output stream hands the
    /// writer a 1 MiB buffer (2 * max_compress_block_size, or adaptive_write_buffer_initial_size for an
    /// adaptive stream), and that is all the writer holds until data actually fills it. An up-front merge
    /// reservation that charged the policy's first part for every stream whose data volume it cannot derive
    /// from the source parts - a rebuilt projection, a variable-size DEFAULT-filled column, a delayed
    /// vertical stream - would over-reserve such a stream 32x and starve merge admission, which is why
    /// CompactionStatistics::estimateNeededMemoryForMerge prices them at the writer's initial buffers.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const UInt64 initial = 1024 * 1024;
    EXPECT_EQ(firstPolicyBufferSize(settings), 32ULL * 1024 * 1024);
    EXPECT_EQ(firstBufferMemoryAfterWriting(settings, initial, 0), initial);
    EXPECT_EQ(firstBufferMemoryAfterWriting(settings, initial, 1024), initial);

    /// It grows only with the data written into it - which the reactive background_memory_tracker sees as it
    /// materializes - and never past the policy's first part.
    EXPECT_LE(firstBufferMemoryAfterWriting(settings, initial, 20ULL * 1024 * 1024), 2 * 20ULL * 1024 * 1024);
    EXPECT_EQ(firstBufferMemoryAfterWriting(settings, initial, 100ULL * 1024 * 1024), 32ULL * 1024 * 1024);

    /// A caller that passes MORE than the policy's first part gets it shrunk down (allocateBuffer).
    EXPECT_EQ(firstBufferMemoryAfterWriting(settings, 64ULL * 1024 * 1024, 0), 32ULL * 1024 * 1024);
}
