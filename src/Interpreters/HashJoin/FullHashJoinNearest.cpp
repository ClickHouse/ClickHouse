
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
/// FULL NEAREST JOIN is rejected before execution; the instantiation only satisfies joinDispatch.
template class HashJoinMethods<JoinKind::Full, JoinStrictness::Nearest, HashJoin::MapsAll>;
}
