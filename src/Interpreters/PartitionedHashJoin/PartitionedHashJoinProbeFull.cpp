#include <Interpreters/PartitionedHashJoin/PartitionedHashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr PartitionedHashJoin::probeImpl<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr PartitionedHashJoin::probeImpl<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t, const Block *);

}
