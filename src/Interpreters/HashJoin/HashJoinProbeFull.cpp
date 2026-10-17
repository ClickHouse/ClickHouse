#include <Interpreters/HashJoin/HashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr HashJoin::probeImpl<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t, const Block *);

}
