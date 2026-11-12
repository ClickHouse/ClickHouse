#include <Interpreters/HashJoin/PrepareKeys.h>

namespace DB::JoinProbe
{
template PrepareKeysFn selectPrepareKeys<HashJoin::MapsOne>(HashJoin::Type, bool, bool);
}
