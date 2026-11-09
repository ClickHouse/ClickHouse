#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsSet>(HashJoin::Type, FindKeysMode, bool);
}
