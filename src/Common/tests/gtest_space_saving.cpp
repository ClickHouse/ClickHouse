#include <Common/SpaceSaving.h>

#include <set>
#include <string>

#include <gtest/gtest.h>

#include <base/scope_guard.h>
#include <Common/FailPoint.h>

namespace DB::FailPoints
{
extern const char space_saving_copy_arena_throw[];
}

using namespace DB;

/// Collect the key strings from a topK result into a std::set for comparison.
static std::set<std::string> collectKeys(const std::vector<SpaceSaving<std::string_view, StringViewHash>::Counter> & counters)
{
    std::set<std::string> keys;
    for (const auto & c : counters)
        keys.emplace(c.key);
    return keys;
}

/// Test that SpaceSaving<std::string_view> merge into empty destination
/// correctly deep-copies keys into the destination arena.
TEST(SpaceSaving, StringViewMergeIntoEmptyCorrectness)
{
    SpaceSaving<std::string_view, StringViewHash> src(10);
    src.insert("alpha");
    src.insert("beta");
    src.insert("gamma");
    ASSERT_EQ(src.size(), 3);

    SpaceSaving<std::string_view, StringViewHash> dst(10);
    dst.merge(src); /// triggers operator= since dst is empty

    auto src_top = src.topK(10);
    auto dst_top = dst.topK(10);
    ASSERT_EQ(src_top.size(), 3);
    ASSERT_EQ(dst_top.size(), 3);

    /// Verify the actual key values match between source and destination.
    auto src_keys = collectKeys(src_top);
    auto dst_keys = collectKeys(dst_top);
    ASSERT_EQ(src_keys, dst_keys);
    ASSERT_TRUE(dst_keys.count("alpha"));
    ASSERT_TRUE(dst_keys.count("beta"));
    ASSERT_TRUE(dst_keys.count("gamma"));

    /// dst should be independently usable (keys owned by its own arena).
    dst.insert("delta");
    ASSERT_EQ(dst.topK(10).size(), 4);
}

/// Test that operator= is exception-safe: no double-free on partial copy.
///
/// Without the fix, the destination destructor frees keys that still belong
/// to the source arena, causing a double-free. This reliably crashes under
/// ASan (heap-use-after-free / double-free) or manifests as a SEGFAULT in
/// jemalloc's prof_free path in production builds.
///
/// Uses a ONCE failpoint that throws after the first key is emplaced, so
/// copied=1 and keys [1..n) still reference the source arena. The catch
/// block in operator= truncates counter_list to the copied prefix.
///
/// The fix is index-independent (try/catch + resize), so testing the
/// first-key boundary is sufficient to validate the invariant that
/// counter_list only contains locally-owned keys after an exception.
TEST(SpaceSaving, StringViewCopyAssignmentExceptionSafety)
{
    SpaceSaving<std::string_view, StringViewHash> src(10);
    src.insert("hello");
    src.insert("world");
    src.insert("test_key_long_enough");
    ASSERT_EQ(src.size(), 3);

    {
        SpaceSaving<std::string_view, StringViewHash> dst(10);

        FailPointInjection::enableFailPoint(
            FailPoints::space_saving_copy_arena_throw);
        SCOPE_EXIT({ FailPointInjection::disableFailPoint(
            FailPoints::space_saving_copy_arena_throw); });

        /// merge calls operator= since dst is empty; should throw
        ASSERT_THROW(dst.merge(src), Exception);

        /// dst is destroyed here -- must not double-free / SEGFAULT
    }

    /// src should still be intact after the failed merge
    auto top = src.topK(10);
    ASSERT_EQ(top.size(), 3);
    auto keys = collectKeys(top);
    ASSERT_TRUE(keys.count("hello"));
    ASSERT_TRUE(keys.count("world"));
    ASSERT_TRUE(keys.count("test_key_long_enough"));
}
