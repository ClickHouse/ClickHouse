/// Tests of `Sanitizer` (`san.c`, `safety_check.c`): junk locations, stashed-pointer checks and the safety check
/// message format (with the abort hook), the per-thread guard countdowns, `sanInit`, and the guard size helpers.

#include <allocator/Sanitizer.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <string>

using namespace jemalloc;

namespace
{

std::string last_message;
int abort_calls = 0;

void captureAbort(const char * message)
{
    last_message = message;
    ++abort_calls;
}

}

TEST(Sanitizer, JunkLocations)
{
    alignas(16) unsigned char buf[256];
    void * first;
    void * mid;
    void * last;
    sanJunkPtrLocations(buf, 8, &first, &mid, &last);
    CHECK(first == buf && mid == buf && last == buf);
    sanJunkPtrLocations(buf, 16, &first, &mid, &last);
    CHECK(first == buf && mid == buf + 8 && last == buf + 8);
    sanJunkPtrLocations(buf, 48, &first, &mid, &last);
    CHECK(first == buf && mid == buf + 24 && last == buf + 40);
    sanJunkPtrLocations(buf, 80, &first, &mid, &last);
    CHECK(mid == buf + 40 && last == buf + 72);
    sanJunkPtrLocations(buf, 112, &first, &mid, &last);
    CHECK(mid == buf + 56 && last == buf + 104);

    static_assert(!sanJunkPtrShouldSlow());
    std::memset(buf, 0, sizeof(buf));
    sanJunkPtr(buf, 112);
    uintptr_t w;
    std::memcpy(&w, buf, 8);
    CHECK_EQ(w, uaf_detect_junk);
    std::memcpy(&w, buf + 56, 8);
    CHECK_EQ(w, uaf_detect_junk);
    std::memcpy(&w, buf + 104, 8);
    CHECK_EQ(w, uaf_detect_junk);
    std::memcpy(&w, buf + 8, 8);
    CHECK_EQ(w, uintptr_t(0));
}

TEST(Sanitizer, StashedPtrsAndSafetyCheck)
{
    safetyCheckSetAbort(captureAbort);
    alignas(64) static unsigned char a[64];
    alignas(64) static unsigned char b[64];
    sanJunkPtr(a, 48);
    sanJunkPtr(b, 48);
    void * ptrs[2] = {a, b};
    abort_calls = 0;
    sanCheckStashedPtrs(ptrs, 2, 48);
    CHECK_EQ(abort_calls, 0);

    /// A write after free into the middle word of the second pointer.
    b[24] = 1;
    sanCheckStashedPtrs(ptrs, 2, 48);
    CHECK_EQ(abort_calls, 1);
    char expected[256];
    std::snprintf(expected, sizeof(expected), "<jemalloc>: Write-after-free detected on deallocated pointer %p (size 48).\n", static_cast<void *>(b));
    CHECK_STREQ(last_message.c_str(), expected);

    /// Writes outside the three words are not detected.
    sanJunkPtr(b, 48);
    b[16] = 1;
    sanCheckStashedPtrs(ptrs, 2, 48);
    CHECK_EQ(abort_calls, 1);

    safetyCheckFailSizedDealloc(true, reinterpret_cast<void *>(0x1000), 64, 32);
    CHECK_EQ(abort_calls, 2);
    CHECK_STREQ(
        last_message.c_str(),
        "<jemalloc>: size mismatch detected (true size 64 vs input size 32), likely caused by application sized deallocation "
        "bugs (source address: 0x1000, the current pointer being freed). Suggest building with --enable-debug or address "
        "sanitizer for debugging. Abort.\n");
    safetyCheckFailSizedDealloc(false, reinterpret_cast<void *>(0x2000), 1, 2);
    CHECK(last_message.find("in thread cache, possibly from previous deallocations") != std::string::npos);

    safetyCheckFail("Called realloc(non-null-ptr, 0) with zero_realloc:abort set\n");
    CHECK_STREQ(last_message.c_str(), "Called realloc(non-null-ptr, 0) with zero_realloc:abort set\n");

    /// The message is truncated to the 4096-byte buffer.
    std::string long_arg(5000, 'x');
    safetyCheckFail("%s", long_arg.c_str());
    CHECK_EQ(last_message.size(), size_t(4095));
    safetyCheckSetAbort(nullptr);
}

TEST(Sanitizer, Init)
{
    CHECK_EQ(san_cache_bin_nonfast_mask, uintptr_t(-1));
    sanInit(ssize_t(LG_PAGE));
    CHECK_EQ(san_cache_bin_nonfast_mask, PAGE - 1);
    sanInit(-1);
    CHECK_EQ(san_cache_bin_nonfast_mask, uintptr_t(-1));
    CHECK(!sanUafDetectionEnabled());
}

TEST(Sanitizer, GuardDecisions)
{
    static constinit ThreadState tsd;
    ExtentHooks ehooks;
    ehooks.init(const_cast<extent_hooks_t *>(&ehooks_default_extent_hooks), 0);

    /// Disabled by default.
    CHECK(!sanGuardEnabled());
    CHECK(!sanSlabExtentDecideGuard(&tsd, &ehooks));
    CHECK(!sanLargeExtentDecideGuard(&tsd, &ehooks, PAGE, PAGE));

    opt.san_guard_small = 3;
    opt.san_guard_large = 2;
    tsdSanInit(tsd);
    CHECK(sanGuardEnabled());
    CHECK_EQ(tsd.san_extents_until_guard_small, uint64_t(3));
    CHECK_EQ(tsd.san_extents_until_guard_large, uint64_t(2));

    /// Every 3rd slab.
    bool slab_decisions[7];
    for (bool & d : slab_decisions)
        d = sanSlabExtentDecideGuard(&tsd, &ehooks);
    CHECK(!slab_decisions[0] && !slab_decisions[1] && slab_decisions[2] && !slab_decisions[3] && !slab_decisions[4] && slab_decisions[5]);
    /// No thread state: never guarded, the counter is untouched.
    CHECK(!sanSlabExtentDecideGuard(nullptr, &ehooks));

    /// Every 2nd large extent; a refused one (alignment > PAGE) keeps the counter at 1.
    CHECK(!sanLargeExtentDecideGuard(&tsd, &ehooks, PAGE, PAGE));
    CHECK_EQ(tsd.san_extents_until_guard_large, uint64_t(1));
    CHECK(!sanLargeExtentDecideGuard(&tsd, &ehooks, PAGE, 2 * PAGE));
    CHECK_EQ(tsd.san_extents_until_guard_large, uint64_t(1));
    CHECK(!sanLargeExtentDecideGuard(&tsd, &ehooks, SC_LARGE_MAXCLASS, PAGE));
    CHECK(sanLargeExtentDecideGuard(&tsd, &ehooks, PAGE, PAGE));
    CHECK_EQ(tsd.san_extents_until_guard_large, uint64_t(2));

    opt.san_guard_small = 0;
    opt.san_guard_large = 0;
}

TEST(Sanitizer, Sizes)
{
    CHECK_EQ(sanTwoSideGuardedSize(PAGE), 3 * PAGE);
    CHECK_EQ(sanTwoSideUnguardedSize(3 * PAGE), PAGE);
    CHECK_EQ(sanOneSideGuardedSize(PAGE), 2 * PAGE);
    CHECK_EQ(sanOneSideUnguardedSize(2 * PAGE), PAGE);
    CHECK_EQ(SAN_PAGE_GUARDS_SIZE, 2 * PAGE);
    CHECK(sanBumpEnabled() == opt.retain);
}
