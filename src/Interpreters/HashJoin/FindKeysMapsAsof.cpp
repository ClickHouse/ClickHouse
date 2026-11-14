#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsAsof, PrefetchingLookup>(HashJoin::Type, FindKeysMode, bool);
}
