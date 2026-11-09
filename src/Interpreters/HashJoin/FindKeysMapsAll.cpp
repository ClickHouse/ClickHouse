#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsAll, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
}
