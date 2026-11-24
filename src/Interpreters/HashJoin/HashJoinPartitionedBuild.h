#pragma once

#include <Core/Joins.h>
#include <Core/TypeId.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinBuild.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>

#include <absl/functional/function_ref.h>

#include <atomic>
#include <optional>
#include <span>

namespace DB
{

class HashJoinTable;

/// The per-row work of a build with several build streams: the fill pass of a waiting block, and the rounds of scatter and
/// owners that insert the waiting blocks into one table per partition.
namespace JoinPartitionedBuild
{

/// The fill pass of one block and one `ON` disjunct, for a build with several build streams. Fills `clause.skip` (left empty
/// when no row is skipped), `clause.route`, `clause.key128` (`hashed` and packed `String` keys), `clause.hash64` (`String`
/// and `FixedString` keys) and `clause.skipped_null_key`, and feeds `sketch` with every row that will be inserted. The
/// per-row arrays are indexed by the row of the key columns. A row that `selector`, the stored block's selector, does not
/// list is skipped. Throws `LOGICAL_ERROR` when `is_asof` does not match the table's maps, and for a key type that is not
/// built in partitions (the range maps, the `LowCardinality` maps).
void fillClause(const HashJoinTable & table, bool is_asof, WaitingBlockRecord::Clause & clause,
                const ScatteredBlock::Selector & selector, JoinKeySketch & sketch);

/// For a build with one build stream, whose sketch starts late: hashes the keys of the rows to insert and feeds `sketch`.
/// Changes nothing in `clause`.
void sketchClause(const HashJoinTable & table, bool is_asof, const WaitingBlockRecord::Clause & clause,
                  const ScatteredBlock::Selector & selector, JoinKeySketch & sketch);

/// Upper bound on distinct nonzero keys in the busiest partition at the final spill decision, after all streams stopped.
size_t maxPartitionKeysAtFinish(const HashJoinTable & table, const HashJoinBuildState & state, size_t clause_idx, size_t partitions);

/// A round has one owner per this many rows, at least one and at most `RoundSettings::max_threads`.
constexpr size_t MIN_ROWS_PER_OWNER = 65536;

struct RoundSettings
{
    bool any_take_last_row = false;
    bool drops_blocks = false;
    bool keep_right_blocks = false;
    std::optional<TypeIndex> asof_type;
    ASOFJoinInequality asof_inequality{};
    /// The first round sets the capacity and mode for all partitions.
    const JoinPartitionPlan * plan = nullptr;
    /// At most the number of build streams of the table: owner `i` uses the arena of build stream `i`.
    size_t max_threads = 1;
    bool enable_software_prefetch = true;
};

/// One round: scatter `blocks` (records of disjunct `clause_idx`, ascending `block_no`) and insert every partition's piece
/// with one owner per partition. Owner `i` uses `table.arena(i)`. Of the result, only `all_values_unique` is set. Frees each record's per-row arrays as soon as its run is
/// scattered. Marks referenced blocks with `state.markReferenced` when `drops_blocks`. Calls `add_bytes` with the growth of
/// the tables and arenas, once per partition. Checks cancellation through `check_cancelled`. Rethrows the first exception
/// of a worker after all workers stopped. The first round switches the table to `2^plan->partition_bits` partitions.
template <typename Maps>
BuildResult buildRound(HashJoinTable & table, size_t clause_idx, std::span<WaitingBlockRecord * const> blocks,
                       const RoundSettings & settings, HashJoinBuildState & state,
                       absl::FunctionRef<void(size_t)> add_bytes, absl::FunctionRef<void()> check_cancelled);

extern template BuildResult buildRound<HashJoin::MapsOne>(HashJoinTable &, size_t, std::span<WaitingBlockRecord * const>,
    const RoundSettings &, HashJoinBuildState &, absl::FunctionRef<void(size_t)>, absl::FunctionRef<void()>);
extern template BuildResult buildRound<HashJoin::MapsAll>(HashJoinTable &, size_t, std::span<WaitingBlockRecord * const>,
    const RoundSettings &, HashJoinBuildState &, absl::FunctionRef<void(size_t)>, absl::FunctionRef<void()>);
extern template BuildResult buildRound<HashJoin::MapsAsof>(HashJoinTable &, size_t, std::span<WaitingBlockRecord * const>,
    const RoundSettings &, HashJoinBuildState &, absl::FunctionRef<void(size_t)>, absl::FunctionRef<void()>);
extern template BuildResult buildRound<HashJoin::MapsSet>(HashJoinTable &, size_t, std::span<WaitingBlockRecord * const>,
    const RoundSettings &, HashJoinBuildState &, absl::FunctionRef<void(size_t)>, absl::FunctionRef<void()>);

/// Runs `work(worker)` for the workers [0, num_workers): worker 0 on the calling thread, the others on a local pool. The work
/// is claimed from atomic counters, so any worker can finish it alone. Rethrows the first exception after all workers stopped;
/// `stop` tells the others to stop claiming.
void runWorkers(size_t num_workers, std::atomic<bool> & stop, absl::FunctionRef<void(size_t)> work);
}

}
