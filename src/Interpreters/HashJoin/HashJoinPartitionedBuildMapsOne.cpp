#include <Interpreters/HashJoin/HashJoinPartitionedBuildImpl.h>

namespace DB::JoinPartitionedBuild
{
template BuildResult buildRound<HashJoin::MapsOne>(HashJoinTable &, size_t, std::span<WaitingBlockRecord * const>,
    const RoundSettings &, HashJoinBuildState &, absl::FunctionRef<void(size_t)>, absl::FunctionRef<void()>);
}
