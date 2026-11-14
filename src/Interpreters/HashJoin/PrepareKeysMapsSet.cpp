#include <Interpreters/HashJoin/PrepareKeys.h>

namespace DB::JoinProbe
{
template PrepareKeysFn selectPrepareKeys<HashJoin::MapsSet>(HashJoin::Type, bool, bool);
}
