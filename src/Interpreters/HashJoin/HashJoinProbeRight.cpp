#include <Interpreters/HashJoin/HashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr HashJoin::probeImpl<JoinKind::Right, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Right, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Right, JoinStrictness::Any, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Right, JoinStrictness::Semi, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Right, JoinStrictness::Anti, HashJoin::MapsAll>(Block, size_t, const Block *);

}
