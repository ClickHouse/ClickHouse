#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

namespace DB
{
template AggregateFunctionPtr createAggregateFunctionWithHashType<20>(bool use_64_bit_hash, const DataTypes & argument_types, const Array & params);
}
