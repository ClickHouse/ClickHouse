#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsOne, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
}
