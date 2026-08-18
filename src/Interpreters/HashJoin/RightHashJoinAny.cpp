#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Right, JoinStrictness::RightAny, HashJoin::MapsAll>;
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Any, HashJoin::MapsAll>;
}
