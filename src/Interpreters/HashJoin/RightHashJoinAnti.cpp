#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Anti, HashJoin::MapsAll>;
}
