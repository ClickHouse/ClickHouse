#include <Interpreters/HashJoin/HashJoinProbeImpl.h>

namespace DB
{

template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::All, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsOne>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsAll>(Block, size_t, const Block *);
template JoinResultPtr HashJoin::probeImpl<JoinKind::Left, JoinStrictness::Asof, HashJoin::MapsAsof>(Block, size_t, const Block *);

}
