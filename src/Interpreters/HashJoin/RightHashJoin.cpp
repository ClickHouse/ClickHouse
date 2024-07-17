#include <Interpreters/HashJoin/HashJoinMethods.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Right, JoinStrictness::RightAny, HashJoin::MapsOne>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Any, HashJoin::MapsAll>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::All, HashJoin::MapsAll>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Semi, HashJoin::MapsAll>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Anti, HashJoin::MapsAll>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Asof, HashJoin::MapsAsof>;
}
