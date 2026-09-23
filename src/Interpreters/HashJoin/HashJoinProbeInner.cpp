#include <Interpreters/HashJoin/HashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Inner, JoinStrictness::Asof, HashJoin::MapsAsof>(Block, size_t, const Block *);

}
