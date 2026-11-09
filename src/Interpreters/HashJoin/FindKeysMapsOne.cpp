#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsOne>(HashJoin::Type, FindKeysMode, bool);
}
