#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>
#include <Interpreters/HashJoin/JoinRightColumnsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsOne>;
}
