#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/PartitionedHashJoin/AmacRing.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
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

/// The table type the `UInt64` keys use, for its geometry helpers and its degree arithmetic.
using Key64Table = typename decltype(HashJoinTableMapsAll::key64)::element_type;

struct CountedOwner
{
    size_t & live;

    explicit CountedOwner(size_t & live_)
        : live(live_)
    {
        ++live;
    }

    ~CountedOwner()
    {
        --live;
    }
};

using OwnerTable = typename decltype(HashJoinTableMapsTemplate<std::unique_ptr<CountedOwner>>::key64)::element_type;

constexpr UInt64 key_step = 2654435761ULL;

UInt64 keyOf(size_t i)
{
    return i * key_step + 1;
}

/// An `amacRun` policy with no memory to wait for. Row `r` completes on its `r % 4`-th visit.
/// Every fifth row is handled in `start` without entering the ring. Completions are counted per row.
struct CountingPolicy
{
    template <size_t ring_size>
    struct Ring
    {
        std::array<UInt32, ring_size> row;
        std::array<UInt8, ring_size> remaining{};

        Ring() { row.fill(amac_inactive_row); }
        bool isActive(size_t s) const { return row[s] != amac_inactive_row; }
        void deactivate(size_t s) { row[s] = amac_inactive_row; }
    };

    std::vector<UInt32> completions;
    size_t synchronous = 0;

    explicit CountingPolicy(size_t rows) : completions(rows, 0) { }

    template <typename R>
    bool start(R & ring, size_t s, size_t row)
    {
        if (row % 5 == 0)
        {
            ++completions[row];
            ++synchronous;
            return false;
        }
        ring.row[s] = static_cast<UInt32>(row);
        ring.remaining[s] = static_cast<UInt8>(row % 4);
        return true;
    }

    template <typename R>
    AmacStepResult step(R & ring, size_t s)
    {
        if (ring.remaining[s] > 0)
        {
            --ring.remaining[s];
            return AmacStepResult::Advance;
        }
        ++completions[ring.row[s]];
        return AmacStepResult::Done;
    }
};

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

TEST(HashJoinTable, FailedRehashDestroysOwners)
{
    for (size_t committed_ranges = 0; committed_ranges <= 2; ++committed_ranges)
    {
        SCOPED_TRACE(committed_ranges);
        size_t live = 0;
        EXPECT_THROW(
            (
                [&]
                {
                    OwnerTable table(8, 1);
                    table.commitAll();
                    table.claimZero(0)->getMapped() = std::make_unique<CountedOwner>(live);
                    for (size_t i = 0; i < 2; ++i)
                        table.claimPersisted(table.cellAt(i), i + 1, table.hash(i + 1))->getMapped() = std::make_unique<CountedOwner>(live);

                    table.beginRehash(9);
                    /// Uncommitted bytes must not look like empty cells if cleanup accidentally reads them.
                    std::memset(reinterpret_cast<char *>(table.newCellAt(0)), 0xAB, table.newCellCount() * sizeof(OwnerTable::cell_type));
                    for (size_t i = 0; i < committed_ranges; ++i)
                    {
                        table.commitNewRange(i);
                        auto * cell = table.claimPersisted(table.newCellAt(i * 256), i + 1, table.hash(i + 1));
                        cell->getMapped() = std::move(table.cellAt(i)->getMapped());
                    }
                    EXPECT_EQ(live, 3u);
                    throw std::bad_alloc{};
                }()),
            std::bad_alloc);
        EXPECT_EQ(live, 0u);
    }
}

TEST(HashJoinTable, AdoptedRehashDestroysOwnersOnce)
{
    size_t live = 0;
    {
        OwnerTable table(8, 1);
        table.commitAll();
        table.claimZero(0)->getMapped() = std::make_unique<CountedOwner>(live);
        for (size_t i = 0; i < 2; ++i)
            table.claimPersisted(table.cellAt(i), i + 1, table.hash(i + 1))->getMapped() = std::make_unique<CountedOwner>(live);

        table.beginRehash(9);
        for (size_t i = 0; i < 2; ++i)
        {
            table.commitNewRange(i);
            auto * cell = table.claimPersisted(table.newCellAt(i * 256), i + 1, table.hash(i + 1));
            cell->getMapped() = std::move(table.cellAt(i)->getMapped());
        }
        table.adoptRehash();
        EXPECT_EQ(live, 3u);
    }
    EXPECT_EQ(live, 0u);
}

/// A table past 2^32 cells is refused before anything is allocated. A reserve above 2^31 keys is what
/// the standard grower's rounding maps there.
TEST(HashJoinTable, DegreeCap)
{
#ifndef DEBUG_OR_SANITIZER_BUILD
    expectThrowsCode(ErrorCodes::LOGICAL_ERROR, "degree 33 must throw before allocating", [] { Key64Table table(33, 0); });
#endif
    const size_t reserve_for_33 = (1uz << 31) + 1;
    EXPECT_GE(Key64Table::degreeFor(reserve_for_33), 33u);
}

/// The route the fill saves for a key names the partition whose range holds the key's home cell.
/// That holds for every plan the 16-bit routes cover. Checked on `key64` and `key_string` with the
/// same hashes the build and the probe use.
TEST(HashJoinTable, RoutesMatchTablePlacement)
{
    constexpr size_t rows = 10007;
    auto uint64_key = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        uint64_key->insertValue(keyOf(i));
    uint64_key->insertValue(0); /// the zero key has a route too

    {
        const ColumnRawPtrs key_columns{uint64_key.get()};
        const Sizes key_sizes{sizeof(UInt64)};
        PaddedPODArray<UInt16> routes(uint64_key->size());
        DenseHyperLogLog hll;
        computeJoinRoutesForFill(HashJoin::Type::key64, key_columns, key_sizes, uint64_key->size(), nullptr, routes.data(), hll);
        EXPECT_NEAR(hll.estimate(), static_cast<double>(uint64_key->size()), 0.05 * static_cast<double>(uint64_key->size()));

        for (const size_t bits : {1uz, 9uz, 15uz})
        {
            const size_t size_degree = std::max<size_t>(bits, 16);
            Key64Table table(size_degree, bits);
            const auto & data = uint64_key->getData();
            for (size_t i = 0; i < data.size(); ++i)
            {
                const size_t hash = table.hash(data[i]);
                const size_t partition = routes[i] >> (16 - bits);
                ASSERT_EQ(table.partitionOf(hash), partition) << "bits " << bits << " row " << i;
                const size_t home = table.place(hash);
                ASSERT_GE(home, table.rangeBegin(partition));
                ASSERT_LT(home, table.rangeEnd(partition));
            }
        }
    }

    {
        auto string_key = ColumnString::create();
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string value = i % 7 == 0 ? "" : fmt::format("key-{}-{}", i, std::string(i % 19, 'x'));
            string_key->insertData(value.data(), value.size());
        }
        using StringTable = typename decltype(HashJoinTableMapsAll::key_string)::element_type;
        const ColumnRawPtrs key_columns{string_key.get()};
        const Sizes key_sizes{0};
        PaddedPODArray<UInt16> routes(rows);
        DenseHyperLogLog hll;
        computeJoinRoutesForFill(HashJoin::Type::key_string, key_columns, key_sizes, rows, nullptr, routes.data(), hll);

        constexpr size_t bits = 7;
        StringTable table(/*size_degree_=*/16, bits);
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string_view value = string_key->getDataAt(i);
            const size_t hash = table.hash(value);
            ASSERT_EQ(table.partitionOf(hash), routes[i] >> (16 - bits)) << "row " << i;
        }
    }
}

/// Every row completes exactly once, whether the run has no rows, stays in the drain (fewer rows than
/// slots), or fills the ring and refills it.
TEST(AmacRing, EveryRowCompletesOnce)
{
    for (const size_t rows : {0uz, 1uz, amac_ring_size - 1, amac_ring_size, amac_ring_size + 1, 10007uz})
    {
        CountingPolicy policy(rows);
        amacRun(policy, rows);
        EXPECT_TRUE(std::ranges::all_of(policy.completions, [](UInt32 count) { return count == 1; })) << "rows " << rows;
        EXPECT_EQ(policy.synchronous, (rows + 4) / 5) << "rows " << rows;
    }
}
