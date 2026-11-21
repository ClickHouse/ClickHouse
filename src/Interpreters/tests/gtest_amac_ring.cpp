#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <vector>

#include <Interpreters/HashJoin/AmacRing.h>

using namespace DB;

namespace
{

/// An `amacRun` policy with no memory to wait for. Row `r` advances `r % 4` times and completes on its next visit.
/// Every fifth row is handled in `start` without entering the ring. Completions are counted per row.
struct CountingPolicy
{
    static constexpr UInt32 inactive_row = std::numeric_limits<UInt32>::max();

    struct Ring
    {
        std::array<UInt32, amac_ring_size> row{};
        std::array<UInt8, amac_ring_size> remaining{};

        Ring() { row.fill(inactive_row); }
        [[nodiscard]] bool isActive(size_t s) const { return row[s] != inactive_row; }
        void deactivate(size_t s) { row[s] = inactive_row; }
    };

    /// `amacRun` works on a copy of the policy, so the counts are outside it.
    std::vector<UInt32> & completions;
    size_t & synchronous;

    bool start(Ring & ring, size_t s, size_t row)
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

    AmacStepResult step(Ring & ring, size_t s)
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

/// An `amacRun` policy that retires its rows in row order through a window of `amac_reorder_window` rows, as the ring of
/// `AmacLookup.h` does. Row `r` advances `walk(r)` times before it finishes. A few rows walk longer than the window, so the
/// window fills and free slots wait until they finish. Every fifth row is answered in `start` and only goes into the window.
struct InOrderPolicy
{
    static constexpr bool in_order = true;
    static constexpr UInt32 inactive_row = std::numeric_limits<UInt32>::max();

    struct Ring
    {
        std::array<UInt32, amac_ring_size> row{};
        std::array<UInt32, amac_ring_size> remaining{};
        std::array<bool, amac_reorder_window> finished{};
        size_t retired = 0;

        Ring() { row.fill(inactive_row); }
        [[nodiscard]] bool isActive(size_t s) const { return row[s] != inactive_row; }
        void deactivate(size_t s) { row[s] = inactive_row; }
    };

    /// `amacRun` works on a copy of the policy, so the retired rows are outside it.
    std::vector<UInt32> & retire_order;

    static UInt32 walk(size_t row) { return row % 97 == 3 ? 3 * amac_reorder_window : row % 4; }

    static void finish(Ring & ring, size_t row) { ring.finished[row % amac_reorder_window] = true; }

    void retireReady(Ring & ring)
    {
        while (ring.finished[ring.retired % amac_reorder_window])
        {
            ring.finished[ring.retired % amac_reorder_window] = false;
            retire_order.push_back(static_cast<UInt32>(ring.retired));
            ++ring.retired;
        }
    }

    bool mayStart(Ring & ring, size_t row)
    {
        if (row - ring.retired < amac_reorder_window)
            return true;
        retireReady(ring);
        return row - ring.retired < amac_reorder_window;
    }

    void finishRun(Ring & ring, size_t rows)
    {
        retireReady(ring);
        EXPECT_EQ(ring.retired, rows);
    }

    bool start(Ring & ring, size_t s, size_t row)
    {
        if (row % 5 == 0)
        {
            finish(ring, row);
            return false;
        }
        ring.row[s] = static_cast<UInt32>(row);
        ring.remaining[s] = walk(row);
        return true;
    }

    AmacStepResult step(Ring & ring, size_t s)
    {
        if (ring.remaining[s] > 0)
        {
            --ring.remaining[s];
            return AmacStepResult::Advance;
        }
        const size_t row = ring.row[s];
        finish(ring, row);
        if (row == ring.retired)
            retireReady(ring);
        return AmacStepResult::Done;
    }
};

}

/// Every row completes exactly once, whether the run has no rows, has fewer rows than slots and so only
/// drains, or fills the ring and refills it.
TEST(AmacRing, EveryRowCompletesOnce)
{
    for (const size_t rows : {0uz, 1uz, amac_ring_size - 1, amac_ring_size, amac_ring_size + 1, 10007uz})
    {
        std::vector<UInt32> completions(rows, 0);
        size_t synchronous = 0;
        CountingPolicy policy{completions, synchronous};
        amacRun(policy, rows);
        EXPECT_TRUE(std::ranges::all_of(completions, [](UInt32 count) { return count == 1; })) << "rows " << rows;
        EXPECT_EQ(synchronous, (rows + 4) / 5) << "rows " << rows;
    }
}

/// The rows of a policy in row order retire once each and in row order, also when long walks fill the window and free
/// slots wait for the oldest row.
TEST(AmacRing, InOrderRetiresEveryRowInOrder)
{
    for (const size_t rows : {0uz, 1uz, amac_ring_size + 1, amac_reorder_window, amac_reorder_window + 1, 10007uz})
    {
        std::vector<UInt32> retire_order;
        InOrderPolicy policy{retire_order};
        amacRun(policy, rows);
        ASSERT_EQ(retire_order.size(), rows);
        for (size_t row = 0; row < rows; ++row)
            ASSERT_EQ(retire_order[row], row) << "rows " << rows;
    }
}
