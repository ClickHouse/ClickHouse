#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsOne>;
template class HashJoinMethods<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsAll>;
}
