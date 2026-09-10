#include <Columns/ColumnsNumber.h>
#include <Interpreters/PartitionedHashJoin/DuplicateSpans.h>
#include <Interpreters/RowRefs.h>
#include <Common/Arena.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <vector>

using namespace DB;

TEST(RowRefList, InsertInitialElementFromEmpty)
{
    Arena pool;

    const UInt64 ref_word = RowRef(/*block_no=*/7, /*row_no=*/42).encode();

    /// Key point: default construction, empty list
    RowRefList list;
    ASSERT_EQ(list.word, 0u);
    ASSERT_FALSE(list.begin().ok());

    /// Execute insert (hits the initialization branch)
    EXPECT_NO_THROW({
        list.insert(ref_word, pool);
    });

    /// The single element must be stored inline, with the inline flag set
    EXPECT_TRUE(list.isInline());
    EXPECT_EQ(list.rows(), 1u);
    EXPECT_EQ(refWordBlockNo(list.word), 7u);
    EXPECT_EQ(refWordRowNo(list.word), 42u);

    /// Verify ForwardIterator behavior
    auto it = list.begin();
    ASSERT_TRUE(it.ok());
    EXPECT_EQ(*it, ref_word);

    ++it;
    EXPECT_FALSE(it.ok());
}

TEST(RowRefList, InsertWithinOneNodeKeepsInsertionOrder)
{
    Arena pool;

    /// Up to MAX_LOCAL (7) rows live in the cell node only (head + slots), and one overflow node
    /// holds the rest; with a single overflow node iteration is exact insertion order.
    RowRefList list(/*block_no=*/3, /*row_no=*/0);
    for (size_t row = 1; row < 10; ++row)
        list.insert(RowRef(3, row).encode(), pool);

    ASSERT_FALSE(list.isInline());
    EXPECT_EQ(list.rows(), 10u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(order, expected);
}

TEST(RowRefList, EvictionBoundaryKeepsInsertionOrder)
{
    Arena pool;

    /// Exactly at the chaining boundary (8 rows: head + 6 local + 1 evicted into the first
    /// overflow node) the order is still pure insertion order, and total_rows is correct.
    RowRefList list(/*block_no=*/0, /*row_no=*/0);
    for (size_t row = 1; row < 8; ++row)
        list.insert(RowRef(0, row).encode(), pool);

    EXPECT_EQ(list.rows(), 8u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
        order.push_back(refWordRowNo(*it));

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(order, expected);
}

TEST(RowRefList, MultiNodeChainOrder)
{
    Arena pool;

    /// With two overflow nodes the order is head, local slots, then overflow nodes newest-first.
    RowRefList list(/*block_no=*/2, /*row_no=*/0);
    for (size_t row = 1; row < 16; ++row)
        list.insert(RowRef(2, row).encode(), pool);

    EXPECT_EQ(list.rows(), 16u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    std::vector<UInt32> order;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        EXPECT_EQ(refWordBlockNo(*it), 2u);
        order.push_back(refWordRowNo(*it));
    }

    const std::vector<UInt32> expected{0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 6, 7, 8, 9, 10, 11};
    EXPECT_EQ(order, expected);
}

TEST(RowRefList, CountSaturationStillIteratesEveryRow)
{
    Arena pool;

    /// Below the count sentinel rows() reads the word; at/after saturation it loads total_rows.
    /// Either way the iterator must yield every inserted row exactly once, in order.
    const size_t n = RowRefList::COUNT_SAT + 5;
    RowRefList list(/*block_no=*/0, /*row_no=*/0);

    EXPECT_EQ(list.rows(), 1u);
    for (size_t row = 1; row < RowRefList::COUNT_SAT - 1; ++row)
        list.insert(RowRef(0, row).encode(), pool);
    /// Exact count straight from the word, no node load.
    EXPECT_EQ(list.rows(), RowRefList::COUNT_SAT - 1);

    for (size_t row = RowRefList::COUNT_SAT - 1; row < n; ++row)
        list.insert(RowRef(0, row).encode(), pool);
    /// Saturated: rows() now reflects total_rows loaded from the node.
    EXPECT_EQ(list.rows(), n);

    size_t count = 0;
    UInt64 seen_xor = 0;
    UInt64 expected_xor = 0;
    for (size_t row = 0; row < n; ++row)
        expected_xor ^= row;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        seen_xor ^= refWordRowNo(*it);
        ++count;
    }
    EXPECT_EQ(count, n);
    EXPECT_EQ(seen_xor, expected_xor);
}

TEST(RowRefList, RangeRepresentation)
{
    Arena pool;

    RowRefList list;
    list.setRange(RowRef(/*block_no=*/1, /*row_no=*/100).encode(), /*rows_=*/5, pool);

    ASSERT_FALSE(list.isInline());
    EXPECT_TRUE(list.asBatch()->is_range);
    EXPECT_EQ(list.rows(), 5u);

    std::vector<UInt32> rows;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        EXPECT_EQ(refWordBlockNo(*it), 1u);
        rows.push_back(refWordRowNo(*it));
    }

    const std::vector<UInt32> expected{100, 101, 102, 103, 104};
    EXPECT_EQ(rows, expected);

    /// A single-row range is stored as the inline ref itself: no node allocation.
    RowRefList single;
    single.setRange(RowRef(/*block_no=*/1, /*row_no=*/7).encode(), /*rows_=*/1, pool);
    EXPECT_TRUE(single.isInline());
    EXPECT_EQ(single.rows(), 1u);
    EXPECT_EQ(refWordRowNo(single.firstWord()), 7u);
}

/// Exact spans `PartitionedHashJoin` writes through `SpanWriter`. Every case checks the layout through
/// the public readers (`rows`, `firstWord`, the iterator) and matches `sim_finish.py` variant `fixed`.
namespace
{

UInt64 ref(size_t row)
{
    return RowRef(/*block_no=*/1, row).encode();
}

std::vector<UInt32> rowsOf(const RowRefList & list)
{
    std::vector<UInt32> rows;
    for (auto it = list.begin(); it.ok(); ++it)
        rows.push_back(refWordRowNo(*it));
    return rows;
}

std::vector<UInt32> iotaFrom(size_t from, size_t n)
{
    std::vector<UInt32> rows(n);
    std::iota(rows.begin(), rows.end(), static_cast<UInt32>(from));
    return rows;
}

std::vector<UInt32> expectedNewestFirst(const std::vector<std::vector<UInt32>> & passes)
{
    std::vector<UInt32> out;
    for (auto pass = passes.rbegin(); pass != passes.rend(); ++pass)
    {
        const size_t max = RowRefList::MAX_RANGE_REFS;
        const size_t chunks = (pass->size() + max - 1) / max;
        for (size_t c = chunks; c-- > 0;)
        {
            const size_t begin = c * max;
            const size_t end = std::min(begin + max, pass->size());
            out.insert(out.end(), pass->begin() + static_cast<std::ptrdiff_t>(begin), pass->begin() + static_cast<std::ptrdiff_t>(end));
        }
    }
    return out;
}

struct SpanFixture
{
    static constexpr UInt32 bucket = 7;

    Arena pool;
    SpanWriter writer{pool};
    PassScratch scratch;
    std::vector<RowRefList> cells{16};
    RowRefList zero;

    RowRefList & cell() { return cells[bucket]; }

    void insert(UInt64 word)
    {
        if (cell().word == 0)
            cell() = RowRefList::fromWord(word);
        else
            appendRow(cell(), word, bucket, scratch);
    }

    void insertZero(UInt64 word)
    {
        if (zero.word == 0)
            zero = RowRefList::fromWord(word);
        else
            appendRowZero(zero, word, scratch);
    }

    void finish()
    {
        writer.finish(scratch, [&](UInt32 b) -> RowRefList & { return cells[b]; }, scratch.zero_items.empty() ? nullptr : &zero);
    }

    /// One pass of `n` rows for the test key, ids `[next, next + n)`. Updates `next`.
    std::vector<UInt32> pass(size_t n, size_t & next)
    {
        std::vector<UInt32> ids;
        ids.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            ids.push_back(static_cast<UInt32>(next));
            insert(ref(next++));
        }
        finish();
        return ids;
    }

    std::vector<UInt32> passZero(size_t n, size_t & next)
    {
        std::vector<UInt32> ids;
        ids.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            ids.push_back(static_cast<UInt32>(next));
            insertZero(ref(next++));
        }
        finish();
        return ids;
    }
};

size_t xorRows(const RowRefList & list)
{
    size_t value = 0;
    size_t count = 0;
    for (auto it = list.begin(); it.ok(); ++it)
    {
        value ^= refWordRowNo(*it);
        ++count;
    }
    EXPECT_EQ(count, list.rows());
    return value;
}

}

TEST(RowRefList, RunDecode)
{
    for (const size_t n : {2uz, 300uz})
    {
        SpanFixture build;
        size_t next = 0;
        build.pass(n, next);
        EXPECT_TRUE(build.cell().isRun());
        EXPECT_EQ(build.cell().rows(), n);
        EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u);
        EXPECT_EQ(rowsOf(build.cell()), iotaFrom(0, n));
        EXPECT_EQ(build.writer.stats().arena_bytes, 8u * n);
        EXPECT_EQ(build.writer.stats().headers, 0u);
        EXPECT_FALSE(build.cell().isCount());
        EXPECT_FALSE(build.cell().isFill());
    }
}

TEST(RowRefList, ChainAcrossPasses)
{
    SpanFixture build;
    size_t next = 0;
    const auto g1 = build.pass(3, next);
    const auto g2 = build.pass(2, next);
    const auto g3 = build.pass(1, next);
    EXPECT_TRUE(build.cell().isChain());
    EXPECT_EQ(build.cell().rows(), 6u);
    EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u) << "the headerless range's first ref is the first-inserted row";
    EXPECT_EQ(rowsOf(build.cell()), (std::vector<UInt32>{5, 3, 4, 0, 1, 2}));
    EXPECT_EQ(rowsOf(build.cell()), expectedNewestFirst({g1, g2, g3}));
    EXPECT_EQ(build.writer.stats().headers, 2u);
    EXPECT_EQ(build.writer.stats().arena_bytes, 6u * 8u + 2u * 16u);

    const auto * newest = build.cell().chainHeader();
    EXPECT_EQ(newest->ownLen(), 1u);
    EXPECT_EQ(newest->total(), 6u);
    EXPECT_TRUE(RowRefList::RangeHeader::prevHasHeader(newest->prev));
    EXPECT_EQ(RowRefList::RangeHeader::prevLen(newest->prev), 2u);
    const auto * middle = reinterpret_cast<const RowRefList::RangeHeader *>(RowRefList::RangeHeader::prevPtr(newest->prev));
    EXPECT_EQ(middle->ownLen(), 2u);
    EXPECT_EQ(middle->total(), 5u);
    EXPECT_FALSE(RowRefList::RangeHeader::prevHasHeader(middle->prev));
    EXPECT_EQ(RowRefList::RangeHeader::prevLen(middle->prev), 3u);
}

TEST(RowRefList, InlineThenDuplicates)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(1, next);
    EXPECT_TRUE(build.cell().isInline());
    build.pass(2, next);
    EXPECT_TRUE(build.cell().isRun());
    EXPECT_EQ(build.cell().rows(), 3u);
    EXPECT_EQ(build.writer.stats().headers, 0u);
    EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u);
    EXPECT_EQ(rowsOf(build.cell()), iotaFrom(0, 3));
}

TEST(RowRefList, OneRowAfterRange)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(2, next);
    build.pass(1, next);
    EXPECT_TRUE(build.cell().isChain());
    EXPECT_EQ(build.cell().rows(), 3u);
    EXPECT_EQ(build.cell().chainHeader()->ownLen(), 1u);
    EXPECT_EQ(build.writer.stats().arena_bytes, 8u * 2u + 8u * 1u + 16u);
    EXPECT_EQ(build.writer.stats().headers, 1u);
    EXPECT_EQ(rowsOf(build.cell()), (std::vector<UInt32>{2, 0, 1}));
}

TEST(RowRefList, SaturatedTotalFromHeader)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(20000, next);
    build.pass(20000, next);
    EXPECT_TRUE(build.cell().isChain());
    EXPECT_EQ(build.cell().countField(), RowRefList::COUNT_SAT);
    EXPECT_EQ(build.cell().rows(), 40000u);
    EXPECT_EQ(build.writer.stats().headers, 1u);
    EXPECT_EQ(build.cell().chainHeader()->total(), 40000u);
    EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u);
}

TEST(RowRefList, RangeCapIsCountSatMinusOne)
{
    {
        SpanFixture build;
        size_t next = 0;
        build.pass(RowRefList::MAX_RANGE_REFS, next);
        EXPECT_TRUE(build.cell().isRun());
        EXPECT_EQ(build.cell().rows(), RowRefList::MAX_RANGE_REFS);
        EXPECT_EQ(build.writer.stats().headers, 0u);
        EXPECT_EQ(build.writer.stats().arena_bytes, 8u * RowRefList::MAX_RANGE_REFS);
    }
    {
        SpanFixture build;
        size_t next = 0;
        build.pass(RowRefList::COUNT_SAT, next);
        EXPECT_TRUE(build.cell().isChain());
        EXPECT_EQ(build.cell().countField(), RowRefList::COUNT_SAT);
        EXPECT_EQ(build.cell().rows(), RowRefList::COUNT_SAT);
        EXPECT_EQ(build.writer.stats().headers, 1u);
        EXPECT_EQ(build.writer.stats().arena_bytes, 8u * RowRefList::COUNT_SAT + 16u);
        EXPECT_EQ(build.writer.stats().ref_words, RowRefList::COUNT_SAT);
    }
}

TEST(RowRefList, SplitAt32766)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(70000, next);
    EXPECT_TRUE(build.cell().isChain());
    EXPECT_EQ(build.cell().rows(), 70000u);
    EXPECT_EQ(build.writer.stats().headers, 2u);
    size_t expected_xor = 0;
    for (size_t row = 0; row < 70000; ++row)
        expected_xor ^= row;
    EXPECT_EQ(xorRows(build.cell()), expected_xor);
    EXPECT_EQ(rowsOf(build.cell()), expectedNewestFirst({iotaFrom(0, 70000)}));
    EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u);
}

TEST(RowRefList, HeaderedSplitLinksHeader)
{
    {
        SpanFixture build;
        size_t next = 0;
        build.pass(3, next);
        const size_t later = 2uz * RowRefList::MAX_RANGE_REFS + 5;
        build.pass(later, next);
        EXPECT_EQ(build.cell().rows(), 3u + later);
        EXPECT_EQ(build.writer.stats().headers, 3u);
        const auto * third = build.cell().chainHeader();
        EXPECT_TRUE(RowRefList::RangeHeader::prevHasHeader(third->prev));
        const auto * second = reinterpret_cast<const RowRefList::RangeHeader *>(RowRefList::RangeHeader::prevPtr(third->prev));
        EXPECT_EQ(reinterpret_cast<const void *>(RowRefList::RangeHeader::prevPtr(third->prev)), second);
        EXPECT_EQ(third->ownLen(), 5u);
        EXPECT_EQ(second->ownLen(), RowRefList::MAX_RANGE_REFS);
        EXPECT_TRUE(RowRefList::RangeHeader::prevHasHeader(second->prev))
            << "the first later-pass chunk is headered because the key already had a range";
        const auto * first_later = reinterpret_cast<const RowRefList::RangeHeader *>(RowRefList::RangeHeader::prevPtr(second->prev));
        EXPECT_EQ(first_later->ownLen(), RowRefList::MAX_RANGE_REFS);
        EXPECT_FALSE(RowRefList::RangeHeader::prevHasHeader(first_later->prev));
        EXPECT_EQ(RowRefList::RangeHeader::prevLen(first_later->prev), 3u);
        EXPECT_EQ(rowsOf(build.cell()), expectedNewestFirst({iotaFrom(0, 3), iotaFrom(3, later)}));
    }
    {
        SpanFixture build;
        size_t next = 0;
        const size_t n = 2uz * RowRefList::MAX_RANGE_REFS + 5;
        build.pass(n, next);
        EXPECT_EQ(build.writer.stats().headers, 2u);
        const auto * third = build.cell().chainHeader();
        EXPECT_TRUE(RowRefList::RangeHeader::prevHasHeader(third->prev));
        const auto * second = reinterpret_cast<const RowRefList::RangeHeader *>(RowRefList::RangeHeader::prevPtr(third->prev));
        EXPECT_EQ(second->ownLen(), RowRefList::MAX_RANGE_REFS);
        EXPECT_FALSE(RowRefList::RangeHeader::prevHasHeader(second->prev));
        EXPECT_EQ(RowRefList::RangeHeader::prevLen(second->prev), RowRefList::MAX_RANGE_REFS);
        EXPECT_EQ(rowsOf(build.cell()), expectedNewestFirst({iotaFrom(0, n)}));
    }
}

TEST(RowRefList, SplitBehindPrevious)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(3, next);
    build.pass(40000, next);
    EXPECT_TRUE(build.cell().isChain());
    EXPECT_EQ(build.cell().rows(), 40003u);
    EXPECT_EQ(build.writer.stats().headers, 2u) << "both new chunks are headered";
    EXPECT_EQ(build.cell().chainHeader()->ownLen(), 40000u - RowRefList::MAX_RANGE_REFS);
    EXPECT_TRUE(RowRefList::RangeHeader::prevHasHeader(build.cell().chainHeader()->prev));
}

TEST(RowRefList, ZeroKeyItems)
{
    SpanFixture build;
    size_t next = 0;
    build.passZero(3, next);
    build.passZero(2, next);
    EXPECT_TRUE(build.zero.isChain());
    EXPECT_EQ(build.zero.rows(), 5u);
    EXPECT_EQ(build.writer.stats().headers, 1u);
    EXPECT_EQ(refWordRowNo(build.zero.firstWord()), 0u);
    EXPECT_EQ(rowsOf(build.zero), (std::vector<UInt32>{3, 4, 0, 1, 2}));
}

TEST(RowRefList, NoBuildWordSurvives)
{
    SpanFixture build;
    size_t next = 0;
    build.pass(5, next);
    EXPECT_FALSE(build.cell().isCount());
    EXPECT_FALSE(build.cell().isFill());
    EXPECT_TRUE(build.cell().isRun() || build.cell().isInline());
}

TEST(RowRefList, G1CutMidPass)
{
    /// Matches sim_finish.py: finish after 5 rows of a 12-row pass, then continue.
    SpanFixture build;
    size_t next = 0;
    for (size_t i = 0; i < 5; ++i)
        build.insert(ref(next++));
    build.finish();
    for (size_t i = 0; i < 7; ++i)
        build.insert(ref(next++));
    build.finish();
    EXPECT_EQ(build.cell().rows(), 12u);
    EXPECT_EQ(refWordRowNo(build.cell().firstWord()), 0u);
    EXPECT_EQ(rowsOf(build.cell()), expectedNewestFirst({iotaFrom(0, 5), iotaFrom(5, 7)}));
}
