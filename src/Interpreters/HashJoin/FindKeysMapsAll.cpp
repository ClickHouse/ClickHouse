#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsAll>(HashJoin::Type, FindKeysMode, bool);
}
