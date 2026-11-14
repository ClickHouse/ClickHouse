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
        bool isActive(size_t s) const { return row[s] != inactive_row; }
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
