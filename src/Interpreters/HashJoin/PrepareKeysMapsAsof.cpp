#include <Interpreters/HashJoin/PrepareKeys.h>

namespace DB::JoinProbe
{
template PrepareKeysFn selectPrepareKeys<HashJoin::MapsAsof>(HashJoin::Type, bool, bool);
}
