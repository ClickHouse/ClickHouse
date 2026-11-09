#include <Interpreters/HashJoin/FindKeys.h>

namespace DB::JoinProbe
{
template FindKeysFn selectFindKeys<HashJoin::MapsAsof>(HashJoin::Type, FindKeysMode, bool);
}
