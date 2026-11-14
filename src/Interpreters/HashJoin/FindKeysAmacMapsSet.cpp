#include <Interpreters/HashJoin/AmacLookup.h>
#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<false>>(HashJoin::Type, FindKeysMode, bool);
template FindKeysFn selectFindKeys<HashJoin::MapsSet, AmacLookup<true>>(HashJoin::Type, FindKeysMode, bool);
}
