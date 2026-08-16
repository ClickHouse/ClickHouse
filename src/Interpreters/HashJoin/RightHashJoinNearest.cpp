
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>

namespace DB
{
/// RIGHT NEAREST JOIN is rejected before execution; the instantiation only satisfies joinDispatch.
template class HashJoinMethods<JoinKind::Right, JoinStrictness::Nearest, HashJoin::MapsAll>;
}
