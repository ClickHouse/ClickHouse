#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsAll>;
}
