#include <gtest/gtest.h>

#include <Common/BufferAllocationPolicy.h>
#include <Core/Defines.h>

#include <base/defines.h>

#include <algorithm>
#include <deque>
#include <limits>
#include <vector>

using namespace DB;

namespace
{

/// The largest total size of live upload buffers a writer using this policy reaches with the given in-flight
/// limit, by replaying WriteBufferFromS3::nextImpl over the real BufferAllocationPolicy: it detaches the full
/// buffer, holds the first part back until a second one exists, submits the detached parts one by one - each
/// submission blocked by TaskTracker::add while the in-flight count is at the limit, with uploads completing
/// in the order that keeps the most memory alive - and only then allocates the next buffer. So it is a check of getMultipartUploadMemory against the code it models rather than
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

                /// TaskTracker::add returns only once the in-flight count is below the limit. Uploads complete
                /// in any order, so let the smallest one finish first: the worst case for the live memory.
                while (inflight.size() >= max_inflight)
                    inflight.erase(std::min_element(inflight.begin(), inflight.end()));
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
/// The writer is flushed after the last byte as well (a merge writer stream finalizes its hashing wrappers,
/// which call `next` of the file buffer), so a buffer filled exactly by the last byte is grown too.
UInt64 firstBufferMemoryAfterWriting(const BufferAllocationPolicy::Settings & settings, UInt64 initial_buffer_size, UInt64 written)
{
    const UInt64 max_first_buffer = firstPolicyBufferSize(settings);
    UInt64 memory = std::min(initial_buffer_size, max_first_buffer);

    UInt64 filled = 0;
    while (true)
    {
        filled += std::min(memory - filled, written - filled);
        if (filled < memory)
            break;
        /// The buffer is full, and nextImpl runs on it either for the next byte or for the final flush: it
        /// grows the first buffer, or - once it is at the policy size - detaches it and moves on to the
        /// multipart buffers priced by the ceiling.
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

TEST(MultipartUploadMemory, ExponentialPolicyCapsTheCeilingAtTheReachableTier)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 20);

    /// With 500 parts at each tier, 200 GiB reaches 256 MiB parts but cannot yet reach 512 MiB.
    /// Reserve the 21 concurrently live 256 MiB buffers, not the eventual 5 GiB tier.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 200ULL * 1024 * 1024 * 1024),
        21 * 256ULL * 1024 * 1024);
}

TEST(MultipartUploadMemory, ExponentialPolicyAccountsForTheLiveBufferTiers)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 20);

    /// One GiB reaches the initial 32 MiB buffer and then 16 MiB multipart buffers, but it does not reach
    /// the next tier. The live window is the 21 most recent buffers, and the 32 MiB first buffer is still
    /// one of them by the time 21 buffers coexist: it leaves the window only once a 22nd buffer is
    /// allocated, which needs more than 352 MiB of output. So the peak is that first buffer plus 20 buffers
    /// of min_upload_part_size - not 21 buffers of the largest reached tier, which is what pricing every
    /// live slot at the biggest tier would give.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024 * 1024),
        32ULL * 1024 * 1024 + 20 * 16ULL * 1024 * 1024);
}

TEST(MultipartUploadMemory, ExponentialPolicyAllowsForUploadsCompletingOutOfOrder)
{
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 64 * 1024 * 1024;
    settings.min_size = 8 * 1024 * 1024;
    settings.max_size = 64 * 1024 * 1024;
    settings.multiply_factor = 2;
    settings.multiply_parts_count_threshold = 1;

    const auto memory = getMultipartUploadMemory(settings, 2);

    /// The policy hands out 64, 16, 32, 64 MiB buffers. At 113 MiB of output the fourth buffer is being
    /// filled. If the first upload is still pending while the second one has finished, the first 64 MiB
    /// buffer coexists with the fourth one, and the third may still be held too - so the live set is the
    /// largest earlier buffers, not the most recent ones (16 + 32 + 64 MiB).
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 113ULL * 1024 * 1024),
        (64ULL + 32 + 64) * 1024 * 1024);
    EXPECT_GE(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 113ULL * 1024 * 1024), liveBuffersUpperBound(settings, 2, 4));
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
    /// ExpBufferAllocationPolicy is well-defined for it: the first-buffer computation is the spelled-out
    /// clamp that tolerates lo > hi (see the constructor), so the first AND the second buffer are both
    /// min_size, and later buffers cap at max_size. The ceiling must therefore keep pricing the buffers at
    /// min_size - dropping to max_size alone would under-report the writer 4x here, and an admission gate
    /// that under-reserves admits merges that then exceed their reservation.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 256 * 1024 * 1024;
    settings.max_size = 64ULL * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 1);

    EXPECT_EQ(firstPolicyBufferSize(settings), 256ULL * 1024 * 1024);
    EXPECT_EQ(memory.ceiling, 2 * 256ULL * 1024 * 1024);
    /// The policy is replayed here like in every other case: the writer's live buffers really reach
    /// two min_size buffers (the detached first part plus the second buffer being filled).
    const UInt64 live = liveBuffersUpperBound(settings, 1, 2000);
    EXPECT_EQ(live, 2 * 256ULL * 1024 * 1024);
    EXPECT_GE(memory.ceiling, live);
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

TEST(MultipartUploadMemory, StrictUploadPartSizeCapsTheCeilingAtReachableBuffers)
{
    BufferAllocationPolicy::Settings settings;
    settings.strict_size = 512 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 20);

    /// Before the first strict-size part is full, the writer doubles its initial 1-MiB buffer in place.
    /// It has not detached a part yet, so reserve only the reached 256-MiB tier rather than two 512-MiB
    /// buffers.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 200ULL * 1024 * 1024),
        256ULL * 1024 * 1024);

    /// A strict-size writer starts with the caller's at-most-1-MiB buffer and grows it only after data
    /// fills it. With 1 GiB of output it can reach at most two completed 512-MiB buffers and the next
    /// buffer, which the final flush allocates after the second one is detached, not all 21 configured
    /// in-flight buffers.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024 * 1024),
        3 * 512ULL * 1024 * 1024);
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024 * 1024 - 1),
        2 * 512ULL * 1024 * 1024);

    /// The first full strict-size buffer can be detached while the second is only partially filled.
    /// The partial second buffer is already included by the ceiling division, so do not add another
    /// unreachable strict-size buffer.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 600ULL * 1024 * 1024),
        2 * 512ULL * 1024 * 1024);
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
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 200ULL * 1024 * 1024 * 1024),
        MultipartUploadMemory::UNLIMITED);

    BufferAllocationPolicy::Settings strict_settings;
    strict_settings.strict_size = 64 * 1024 * 1024;
    const auto strict_memory = getMultipartUploadMemory(strict_settings, 0);
    EXPECT_EQ(strict_memory.ceiling, MultipartUploadMemory::UNLIMITED);
    EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(strict_memory, 1), MultipartUploadMemory::UNLIMITED);
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

TEST(MultipartUploadMemory, ExponentialPolicyDoesNotChargeTheFirstPartBeforeItIsGrownInto)
{
    /// Default S3 sizing: the policy's first part is 32 MiB, but the writer starts with the caller's
    /// buffer - at most DBMS_DEFAULT_BUFFER_SIZE, see the constructor of WriteBufferFromS3 - and
    /// reallocateFirstBuffer only doubles it once it fills up. Output that does not fill the first buffer
    /// must therefore be priced at the buffer the writer actually holds, not at the whole first part:
    /// CompactionStatistics uses this value as the per-stream multipart term, so charging 32 MiB per stream
    /// for a small remote merge closes merges_mutations_memory_usage_soft_limit on memory that is never
    /// allocated.
    BufferAllocationPolicy::Settings settings;
    settings.max_single_size = 32 * 1024 * 1024;
    settings.min_size = 16 * 1024 * 1024;
    settings.max_size = 5ULL * 1024 * 1024 * 1024;

    const auto memory = getMultipartUploadMemory(settings, 4);
    EXPECT_EQ(firstPolicyBufferSize(settings), 32ULL * 1024 * 1024);

    /// The sub-first-buffer window: every one of these amounts is held by the growing first buffer alone,
    /// with nothing detached and no policy-sized buffer allocated at all.
    for (const UInt64 bytes_written :
         {1024ULL,
          1024ULL * 1024,
          1024ULL * 1024 + 1,
          3ULL * 1024 * 1024,
          20ULL * 1024 * 1024,
          32ULL * 1024 * 1024 - 1})
    {
        EXPECT_EQ(
            getMultipartUploadMemoryCeilingForWrittenBytes(memory, bytes_written),
            firstBufferMemoryAfterWriting(settings, DBMS_DEFAULT_BUFFER_SIZE, bytes_written))
            << "bytes_written = " << bytes_written;
    }

    EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024 - 1), 1024ULL * 1024);
    /// The final flush grows a buffer that the last byte filled exactly.
    EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024), 2ULL * 1024 * 1024);
    EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1024ULL * 1024 + 1), 2ULL * 1024 * 1024);
    EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 20ULL * 1024 * 1024), 32ULL * 1024 * 1024);

    /// Filling the first part is the first moment a second buffer exists: the full first buffer is detached
    /// (by the next byte or by the final flush) while the policy's second buffer - min_upload_part_size - is
    /// allocated.
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 32ULL * 1024 * 1024),
        32ULL * 1024 * 1024 + 16ULL * 1024 * 1024);
    EXPECT_EQ(
        getMultipartUploadMemoryCeilingForWrittenBytes(memory, 32ULL * 1024 * 1024 + 1),
        32ULL * 1024 * 1024 + 16ULL * 1024 * 1024);
}

TEST(MultipartUploadMemory, ExponentialPolicySubFirstBufferWindowNeverExceedsTheReplayedWriter)
{
    /// The same property over a spread of policies and output sizes, checked against the replay of the real
    /// writer: the value must never be below what the writer holds (that would under-reserve an admitted
    /// merge) and, inside the first-buffer window, never above it either.
    const std::vector<BufferAllocationPolicy::Settings> policies = []
    {
        std::vector<BufferAllocationPolicy::Settings> result;
        for (const UInt64 max_single_size : {8ULL * 1024 * 1024, 32ULL * 1024 * 1024, 256ULL * 1024 * 1024})
        {
            for (const UInt64 min_size : {512ULL * 1024, 16ULL * 1024 * 1024, 64ULL * 1024 * 1024})
            {
                BufferAllocationPolicy::Settings settings;
                settings.max_single_size = max_single_size;
                settings.min_size = min_size;
                settings.max_size = 5ULL * 1024 * 1024 * 1024;
                result.push_back(settings);
            }
        }
        return result;
    }();

    for (const auto & settings : policies)
    {
        const auto memory = getMultipartUploadMemory(settings, 4);
        const UInt64 first_part = firstPolicyBufferSize(settings);

        for (const UInt64 bytes_written : {1ULL, 100ULL * 1024, 1024ULL * 1024, 5ULL * 1024 * 1024, 40ULL * 1024 * 1024})
        {
            const UInt64 priced = getMultipartUploadMemoryCeilingForWrittenBytes(memory, bytes_written);
            const UInt64 held = firstBufferMemoryAfterWriting(settings, DBMS_DEFAULT_BUFFER_SIZE, bytes_written);
            EXPECT_GE(priced, held) << "first part " << first_part << ", bytes_written " << bytes_written;
            if (bytes_written < first_part)
                EXPECT_EQ(priced, held) << "first part " << first_part << ", bytes_written " << bytes_written;
        }
    }
}

TEST(MultipartUploadMemory, AdaptiveInitialBufferIsReplayedFromItsRealStart)
{
    /// With use_adaptive_write_buffer, S3ObjectStorage::writeObject and AzureObjectStorage::writeObject start
    /// the writer from adaptive_write_buffer_initial_size (16 KiB by default), not from
    /// DBMS_DEFAULT_BUFFER_SIZE. A stream that has written 100 KiB holds a 128 KiB buffer, not 1 MiB.
    const UInt64 adaptive_initial = 16 * 1024;
    EXPECT_EQ(getMultipartUploadInitialBufferSize(true, adaptive_initial), adaptive_initial);
    EXPECT_EQ(getMultipartUploadInitialBufferSize(true, 64ULL * 1024 * 1024), DBMS_DEFAULT_BUFFER_SIZE);
    EXPECT_EQ(getMultipartUploadInitialBufferSize(false, adaptive_initial), DBMS_DEFAULT_BUFFER_SIZE);

    BufferAllocationPolicy::Settings exponential;
    exponential.max_single_size = 32 * 1024 * 1024;
    exponential.min_size = 16 * 1024 * 1024;
    exponential.max_size = 5ULL * 1024 * 1024 * 1024;

    BufferAllocationPolicy::Settings strict;
    strict.strict_size = 16 * 1024 * 1024;

    for (const auto & settings : {exponential, strict})
    {
        auto memory = getMultipartUploadMemory(settings, 4);
        memory.initial_buffer_size = adaptive_initial;
        const UInt64 first_part = firstPolicyBufferSize(settings);

        for (const UInt64 bytes_written : {1ULL, 16ULL * 1024, 16ULL * 1024 + 1, 100ULL * 1024, 1024ULL * 1024, 3ULL * 1024 * 1024})
        {
            EXPECT_EQ(
                getMultipartUploadMemoryCeilingForWrittenBytes(memory, bytes_written),
                firstBufferMemoryAfterWriting(settings, adaptive_initial, bytes_written))
                << "first part " << first_part << ", bytes_written " << bytes_written;
        }

        EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 100ULL * 1024), 128ULL * 1024);

        /// Once the first part is full the first-buffer start no longer matters.
        auto default_start = getMultipartUploadMemory(settings, 4);
        EXPECT_EQ(
            getMultipartUploadMemoryCeilingForWrittenBytes(memory, first_part + 1),
            getMultipartUploadMemoryCeilingForWrittenBytes(default_start, first_part + 1));
    }
}

TEST(MultipartUploadMemory, NonAdaptiveStreamStartsFromCallerBufferSize)
{
    /// Without use_adaptive_write_buffer the writer starts from the buf_size its caller passes to writeFile,
    /// capped at DBMS_DEFAULT_BUFFER_SIZE: a marks file opened at 4 KiB that received 10 KiB of marks holds
    /// a 16 KiB buffer, not 1 MiB.
    const UInt64 marks_buffer_size = 4096;

    BufferAllocationPolicy::Settings exponential;
    exponential.max_single_size = 32 * 1024 * 1024;
    exponential.min_size = 16 * 1024 * 1024;
    exponential.max_size = 5ULL * 1024 * 1024 * 1024;

    BufferAllocationPolicy::Settings strict;
    strict.strict_size = 16 * 1024 * 1024;

    for (const auto & settings : {exponential, strict})
    {
        auto memory = getMultipartUploadMemory(settings, 4);
        memory.initial_buffer_size = getMultipartUploadInitialBufferSize(false, 16 * 1024);

        /// A caller that does not tell whether the writer follows its buffer size keeps the cap.
        EXPECT_EQ(getMultipartUploadMemoryForBufferSize(memory, marks_buffer_size).initial_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);

        memory.initial_buffer_size_follows_caller = true;
        const auto marks_memory = getMultipartUploadMemoryForBufferSize(memory, marks_buffer_size);
        EXPECT_EQ(marks_memory.initial_buffer_size, marks_buffer_size);
        /// A caller buffer above the cap is capped, like in the writer's constructor.
        EXPECT_EQ(getMultipartUploadMemoryForBufferSize(memory, 64ULL * 1024 * 1024).initial_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);

        for (const UInt64 bytes_written : {1ULL, 4096ULL, 4097ULL, 10ULL * 1024, 1024ULL * 1024, 3ULL * 1024 * 1024})
        {
            EXPECT_EQ(
                getMultipartUploadMemoryCeilingForWrittenBytes(marks_memory, bytes_written),
                firstBufferMemoryAfterWriting(settings, marks_buffer_size, bytes_written))
                << "bytes_written " << bytes_written;
        }

        EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(marks_memory, 10ULL * 1024), 16ULL * 1024);
    }
}

TEST(MultipartUploadMemory, FirstBufferKeepsTheAllocationOfALargerInitialBuffer)
{
    /// A writer whose initial buffer is larger than the policy's first part shrinks that buffer only logically
    /// (`allocateBuffer` calls `Memory::resize`, which keeps the capacity), and the allocation stays alive with
    /// the first part until its upload releases it. With 64-KiB parts, a single in-flight part and a 1-MiB
    /// initial buffer, the first flush keeps the 1-MiB allocation next to the following 64-KiB buffer.
    constexpr UInt64 part_size = 64 * 1024;

    BufferAllocationPolicy::Settings strict;
    strict.strict_size = part_size;

    BufferAllocationPolicy::Settings exponential;
    exponential.max_single_size = part_size;
    exponential.min_size = part_size;
    exponential.max_size = part_size;

    for (const auto & settings : {strict, exponential})
    {
        auto memory = getMultipartUploadMemory(settings, 1);
        memory.initial_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

        EXPECT_EQ(memory.ceiling, DBMS_DEFAULT_BUFFER_SIZE + part_size);
        /// Before the first part is full, the writer holds the whole initial allocation.
        EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, 1000), DBMS_DEFAULT_BUFFER_SIZE);
        EXPECT_EQ(getMultipartUploadMemoryCeilingForWrittenBytes(memory, DBMS_DEFAULT_BUFFER_SIZE), DBMS_DEFAULT_BUFFER_SIZE + part_size);
    }
}
