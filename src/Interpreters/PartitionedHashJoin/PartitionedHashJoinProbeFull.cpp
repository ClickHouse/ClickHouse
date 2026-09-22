#include <Interpreters/PartitionedHashJoin/PartitionedHashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr PartitionedHashJoin::probeImpl<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t);
template JoinResultPtr PartitionedHashJoin::probeImpl<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t);

}
