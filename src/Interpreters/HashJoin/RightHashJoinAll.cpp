#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Right, JoinStrictness::All, HashJoin::MapsAll>;
}
