#include <Interpreters/HashJoin/AmacLookup.h>
#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
template FindKeysFn selectFindKeys<HashJoin::MapsAll, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
}
