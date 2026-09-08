#include <Common/Arena.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/PartitionedHashJoin/DuplicateRuns.h>
#include <Interpreters/RowRefs.h>

#include <gtest/gtest.h>

#include <numeric>

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

/// The pair / run / list layouts `PartitionedHashJoin` writes through `DuplicateRunWriter`. Every case
/// checks the layout through the public readers only (`rows`, `firstWord`, the iterator), so a change
/// in the writer that the readers cannot decode fails here before it reaches a join.
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

std::vector<UInt32> iota(size_t n)
{
    std::vector<UInt32> rows(n);
    std::iota(rows.begin(), rows.end(), 0u);
    return rows;
}

/// Appends rows [from, to) one at a time, as one build pass per row would.
void appendSingles(DuplicateRunWriter & writer, RowRefList & list, size_t from, size_t to)
{
    for (size_t row = from; row < to; ++row)
    {
        const UInt64 word = ref(row);
        writer.append(list, &word, 1);
    }
}

std::vector<UInt64> refs(size_t from, size_t to)
{
    std::vector<UInt64> words;
    for (size_t row = from; row < to; ++row)
        words.push_back(ref(row));
    return words;
}

/// What the standard `Batch` layout retains for `m` rows: nothing inline, one 64-byte node up to 7, then
/// 6 more rows per chained node.
size_t batchBytes(size_t m)
{
    if (m <= 1)
        return 0;
    if (m <= RowRefList::MAX_LOCAL)
        return sizeof(RowRefList::Batch);
    return sizeof(RowRefList::Batch) * (1 + (m - RowRefList::MAX_LOCAL + 1 + RowRefList::Batch::SLOTS - 1) / RowRefList::Batch::SLOTS);
}

}

TEST(RowRefList, PairAndSingleRunDecode)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/false);

    RowRefList list;
    appendSingles(writer, list, 0, 1);
    EXPECT_TRUE(list.isInline());
    EXPECT_EQ(list.rows(), 1u);

    appendSingles(writer, list, 1, 2);
    EXPECT_TRUE(list.isPair());
    EXPECT_EQ(list.rows(), 2u);
    EXPECT_EQ(list.countField(), 2u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);
    EXPECT_EQ(rowsOf(list), iota(2));
    EXPECT_EQ(writer.liveBytes(), 16u);
    EXPECT_EQ(writer.stats().pairs, 1u);

    /// A third row promotes the pair to a headerless exact run; the pair slot goes to the free list.
    appendSingles(writer, list, 2, 3);
    EXPECT_TRUE(list.isRun());
    EXPECT_EQ(list.rows(), 3u);
    EXPECT_EQ(list.countField(), 3u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);
    EXPECT_EQ(rowsOf(list), iota(3));
    EXPECT_EQ(writer.stats().pairs, 0u);
    EXPECT_EQ(writer.liveBytes(), 24u) << "an ungrouped run of 3 is exactly 3 ref words";
    EXPECT_EQ(writer.stats().small_blocks, 0u);
    EXPECT_EQ(writer.stats().slack_slots, 0u);

    /// The next pair reuses the freed slot instead of the arena.
    RowRefList other;
    appendSingles(writer, other, 0, 2);
    EXPECT_TRUE(other.isPair());
    EXPECT_EQ(writer.liveBytes(), 40u);

    /// One pass with 300 rows of one key: one exact run, 8 bytes per row, one hop.
    RowRefList big;
    const auto words = refs(0, 300);
    writer.append(big, words.data(), words.size());
    EXPECT_TRUE(big.isRun());
    EXPECT_EQ(big.rows(), 300u);
    EXPECT_EQ(rowsOf(big), iota(300));
    EXPECT_EQ(writer.liveBytes(), 40u + 2400u);
    EXPECT_EQ(writer.stats().descriptors, 0u);
}

TEST(RowRefList, SingleRunGroupedBlockEightSlots)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/true);

    RowRefList list;
    appendSingles(writer, list, 0, 3);
    EXPECT_TRUE(list.isRun());
    EXPECT_EQ(writer.stats().small_blocks, 1u);
    EXPECT_EQ(writer.stats().slack_slots, 5u);
    EXPECT_EQ(writer.liveBytes(), 64u);

    /// Five more passes of one row each fill the block in place: no allocation, no chain.
    appendSingles(writer, list, 3, 8);
    EXPECT_TRUE(list.isRun());
    EXPECT_EQ(list.rows(), 8u);
    EXPECT_EQ(list.countField(), 8u);
    EXPECT_EQ(rowsOf(list), iota(8));
    EXPECT_EQ(writer.stats().in_place_fills, 5u);
    EXPECT_EQ(writer.stats().slack_slots, 0u);
    EXPECT_EQ(writer.stats().descriptors, 0u);
    EXPECT_EQ(writer.liveBytes(), 64u);
    EXPECT_LE(writer.liveBytes(), batchBytes(8));
}

TEST(RowRefList, FirstChainMovesOneRef)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/true);

    RowRefList list;
    appendSingles(writer, list, 0, 8);
    ASSERT_TRUE(list.isRun());

    /// The 9th row: the full block hands its last ref to a new small node and writes its trailing link
    /// into the freed slot. Order and first row are preserved; exactly one ref moved.
    appendSingles(writer, list, 8, 9);
    ASSERT_TRUE(list.isList());
    EXPECT_EQ(list.rows(), 9u);
    EXPECT_EQ(list.countField(), 9u);
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);
    EXPECT_EQ(rowsOf(list), iota(9));

    const auto * descriptor = list.listDescriptor();
    EXPECT_EQ(descriptor->first_count, 7u);
    EXPECT_EQ(descriptor->total, 9u);
    ASSERT_NE(descriptor->tail_link, nullptr);
    EXPECT_EQ(RowRefList::linkCount(*descriptor->tail_link), 2u) << "the moved 8th ref plus the new 9th";
    EXPECT_TRUE(RowRefList::linkIsSmall(*descriptor->tail_link));
    EXPECT_EQ(RowRefList::linkNext(*descriptor->tail_link), nullptr) << "null-terminated";
    EXPECT_EQ(RowRefList::linkNext(descriptor->first[descriptor->first_count]), descriptor->tail_link)
        << "the first run's trailing link sits right after its refs and names the second node";

    EXPECT_EQ(writer.stats().moved_refs, 1u);
    EXPECT_EQ(writer.stats().descriptors, 1u);
    EXPECT_EQ(writer.stats().appended_nodes, 1u);
    EXPECT_EQ(writer.liveBytes(), 64u + 24u + 64u);
}

TEST(RowRefList, ListNullTerminatedOrder)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/true);

    /// 22 passes of one row: block of 7 + link, then small nodes of 7, 7 and 1.
    RowRefList list;
    appendSingles(writer, list, 0, 22);
    ASSERT_TRUE(list.isList());
    EXPECT_EQ(list.rows(), 22u);
    EXPECT_EQ(rowsOf(list), iota(22));
    EXPECT_EQ(refWordRowNo(list.firstWord()), 0u);

    /// Walk the links by hand: first run, trailing link, then leading links until null.
    const auto * descriptor = list.listDescriptor();
    std::vector<UInt32> lengths{descriptor->first_count};
    const UInt64 * node = RowRefList::linkNext(descriptor->first[descriptor->first_count]);
    const UInt64 * last = nullptr;
    while (node)
    {
        lengths.push_back(RowRefList::linkCount(*node));
        last = node;
        node = RowRefList::linkNext(*node);
    }
    EXPECT_EQ(lengths, (std::vector<UInt32>{7, 7, 7, 1}));
    EXPECT_EQ(last, descriptor->tail_link);
    EXPECT_EQ(writer.stats().appended_nodes, 3u);
    EXPECT_EQ(writer.stats().moved_refs, 1u);
    EXPECT_EQ(writer.liveBytes(), 24u + 4u * 64u);
    EXPECT_EQ(writer.stats().slack_slots, 6u) << "only the tail block has free slots";
}

TEST(RowRefList, InPlaceFillOfSmallTail)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/true);

    RowRefList list;
    appendSingles(writer, list, 0, 9);
    ASSERT_TRUE(list.isList());
    const UInt64 nodes_before = writer.stats().appended_nodes;
    const UInt64 fills_before = writer.stats().in_place_fills;

    /// Three more rows fit the tail's five free slots: no node, order kept.
    const auto three = refs(9, 12);
    writer.append(list, three.data(), three.size());
    EXPECT_EQ(list.rows(), 12u);
    EXPECT_EQ(rowsOf(list), iota(12));
    EXPECT_EQ(writer.stats().appended_nodes, nodes_before);
    EXPECT_EQ(writer.stats().in_place_fills, fills_before + 1);

    /// Five more: two fill the tail, three start a new node; still one contiguous order.
    const auto five = refs(12, 17);
    writer.append(list, five.data(), five.size());
    EXPECT_EQ(list.rows(), 17u);
    EXPECT_EQ(rowsOf(list), iota(17));
    EXPECT_EQ(writer.stats().appended_nodes, nodes_before + 1);
    EXPECT_EQ(RowRefList::linkCount(*list.listDescriptor()->tail_link), 3u);
    EXPECT_EQ(writer.stats().slack_slots, 4u);
}

TEST(RowRefList, ExactNodeChunking)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/true);

    RowRefList list;
    appendSingles(writer, list, 0, 3);
    ASSERT_TRUE(list.isRun());

    /// A 70000-row contribution: the block takes four more refs before its link slot, the rest goes
    /// into exact nodes of at most 65535 refs each. The word saturates, so `rows` reads the descriptor.
    const auto many = refs(3, 70003);
    writer.append(list, many.data(), many.size());
    ASSERT_TRUE(list.isList());
    EXPECT_EQ(list.countField(), RowRefList::COUNT_SAT);
    EXPECT_EQ(list.rows(), 70003u);
    EXPECT_EQ(rowsOf(list), iota(70003));

    const auto * descriptor = list.listDescriptor();
    EXPECT_EQ(descriptor->first_count, 7u);
    const UInt64 * node = RowRefList::linkNext(descriptor->first[descriptor->first_count]);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(RowRefList::linkCount(*node), RowRefList::LINK_MAX_COUNT);
    EXPECT_FALSE(RowRefList::linkIsSmall(*node));
    const UInt64 * second = RowRefList::linkNext(*node);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(RowRefList::linkCount(*second), 70003u - 7u - RowRefList::LINK_MAX_COUNT);
    EXPECT_EQ(RowRefList::linkNext(*second), nullptr);
    EXPECT_EQ(writer.stats().appended_nodes, 2u);
    EXPECT_EQ(writer.stats().moved_refs, 0u) << "a block with room does not move a ref, it fills up to its link slot";
    EXPECT_EQ(writer.stats().slack_slots, 0u);
    EXPECT_EQ(writer.liveBytes(), 64u + 24u + 8u * (1 + RowRefList::LINK_MAX_COUNT) + 8u * (1 + 70003u - 7u - RowRefList::LINK_MAX_COUNT));
}

TEST(RowRefList, SaturatedTotalInDescriptor)
{
    Arena pool;
    DuplicateRunWriter writer(pool, /*grouped_=*/false);

    /// Below the saturation point a run keeps its exact count in the word.
    RowRefList below;
    const auto small = refs(0, RowRefList::COUNT_SAT - 1);
    writer.append(below, small.data(), small.size());
    EXPECT_TRUE(below.isRun());
    EXPECT_EQ(below.rows(), RowRefList::COUNT_SAT - 1);
    EXPECT_EQ(writer.stats().descriptors, 0u);

    /// From 32767 rows the word cannot carry the count: the run gets a descriptor and stays a one-node
    /// list, exact, with no link word anywhere.
    RowRefList at;
    const auto exact = refs(0, RowRefList::COUNT_SAT);
    writer.append(at, exact.data(), exact.size());
    EXPECT_TRUE(at.isList());
    EXPECT_EQ(at.countField(), RowRefList::COUNT_SAT);
    EXPECT_EQ(at.rows(), RowRefList::COUNT_SAT);
    EXPECT_EQ(at.listDescriptor()->tail_link, nullptr);
    EXPECT_EQ(at.listDescriptor()->first_count, RowRefList::COUNT_SAT);
    EXPECT_EQ(rowsOf(at), iota(RowRefList::COUNT_SAT));

    RowRefList big;
    const auto words = refs(0, 40000);
    writer.append(big, words.data(), words.size());
    EXPECT_TRUE(big.isList());
    EXPECT_EQ(big.rows(), 40000u);
    EXPECT_EQ(refWordRowNo(big.firstWord()), 0u);
    EXPECT_EQ(rowsOf(big), iota(40000));
    EXPECT_EQ(writer.stats().descriptors, 2u);
    EXPECT_EQ(writer.stats().appended_nodes, 0u);
}

TEST(RowRefList, FootprintNeverExceedsBatchBelowNine)
{
    /// Rows spread one per pass, the worst case for a chained layout, up to 8 rows: one 64-byte block at
    /// most, never above what `Batch` retains for the same rows. Nine rows add the descriptor and a node.
    for (size_t m = 1; m <= 9; ++m)
    {
        Arena pool;
        DuplicateRunWriter writer(pool, /*grouped_=*/true);
        RowRefList list;
        appendSingles(writer, list, 0, m);
        EXPECT_EQ(list.rows(), m);
        EXPECT_EQ(rowsOf(list), iota(m));
        if (m <= 8)
        {
            EXPECT_LE(writer.liveBytes(), batchBytes(m)) << "m = " << m;
            EXPECT_LE(writer.liveBytes(), 64u) << "m = " << m;
            EXPECT_EQ(writer.stats().descriptors, 0u) << "m = " << m;
        }
        else
        {
            EXPECT_EQ(writer.liveBytes(), 64u + 24u + 64u);
        }
    }

    /// The same rows arriving in one pass cost 8 bytes each and nothing else.
    for (size_t m = 3; m <= 8; ++m)
    {
        Arena pool;
        DuplicateRunWriter writer(pool, /*grouped_=*/false);
        RowRefList list;
        const auto words = refs(0, m);
        writer.append(list, words.data(), words.size());
        EXPECT_EQ(writer.liveBytes(), 8u * m) << "m = " << m;
        EXPECT_EQ(rowsOf(list), iota(m));
    }
}
