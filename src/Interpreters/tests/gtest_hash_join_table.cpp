#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>

#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>
#include <Common/Exception.h>
#include <base/getPageSize.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Runs `fn`, which must throw an `Exception` with `code`; `what` names the expectation in the failure report.
template <typename F>
void expectThrowsCode(int code, std::string_view what, F && fn)
{
    try
    {
        fn();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code) << e.message();
        return;
    }
    ADD_FAILURE() << what;
}

bool allBytesAre(const char * begin, const char * end, char value)
{
    return std::all_of(begin, end, [value](char c) { return c == value; });
}

}

/// `commit` accounts and zeroes exactly its range, the buffer reports what has been committed, and a
/// move hands the reservation over whole.
TEST(RangeCommittedBuffer, CommitAccountsAndZeroes)
{
    const size_t page = ::getPageSize();
    RangeCommittedBuffer buffer(2 * page);
    ASSERT_NE(buffer.data(), nullptr);
    EXPECT_EQ(buffer.size(), 2 * page);
    EXPECT_EQ(buffer.committedBytes(), 0u);

    /// Reused allocator memory is not zero: dirty the second range before the first is committed, and
    /// check that the commit zeroes only its own range.
    std::fill_n(buffer.data() + page, page, static_cast<char>(0xAB));
    buffer.commit(0, page);
    EXPECT_EQ(buffer.committedBytes(), page);
    EXPECT_TRUE(allBytesAre(buffer.data(), buffer.data() + page, 0));
    EXPECT_TRUE(allBytesAre(buffer.data() + page, buffer.data() + 2 * page, static_cast<char>(0xAB)));

    buffer.commit(page, page);
    EXPECT_EQ(buffer.committedBytes(), 2 * page);
    EXPECT_TRUE(allBytesAre(buffer.data(), buffer.data() + 2 * page, 0));

    /// A zero-length commit is a no-op; a range past the end is refused before anything is charged.
    buffer.commit(page, 0);
    EXPECT_EQ(buffer.committedBytes(), 2 * page);
#ifndef DEBUG_OR_SANITIZER_BUILD
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, "a commit past the end must throw", [&] { buffer.commit(page, 2 * page); });
    EXPECT_EQ(buffer.committedBytes(), 2 * page);
#endif

    char * data = buffer.data();
    RangeCommittedBuffer moved(std::move(buffer));
    EXPECT_EQ(moved.data(), data);
    EXPECT_EQ(moved.size(), 2 * page);
    EXPECT_EQ(moved.committedBytes(), 2 * page);

    RangeCommittedBuffer empty(0);
    EXPECT_EQ(empty.data(), nullptr);
    EXPECT_EQ(empty.size(), 0u);
    EXPECT_EQ(empty.committedBytes(), 0u);
}
