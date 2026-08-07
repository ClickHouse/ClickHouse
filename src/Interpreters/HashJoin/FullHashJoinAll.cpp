#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
template class HashJoinMethods<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>;
}
