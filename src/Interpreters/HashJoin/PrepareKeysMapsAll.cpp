#include <Interpreters/HashJoin/PrepareKeys.h>

namespace DB::JoinProbe
{
template PrepareKeysFn selectPrepareKeys<HashJoin::MapsAll>(HashJoin::Type, bool, bool);
}
