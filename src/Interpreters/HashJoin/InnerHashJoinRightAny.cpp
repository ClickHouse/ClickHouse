#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsOne>;
}
