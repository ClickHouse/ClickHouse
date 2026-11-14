#include <Interpreters/HashJoin/InsertKeys.h>

namespace DB
{
template BuildResult
HashJoinTable::insert<HashJoin::MapsAsof>(const InsertSettings &, const InsertRows &, size_t, absl::FunctionRef<void(size_t)>);
}
