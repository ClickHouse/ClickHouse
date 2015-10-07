#include <DB/AggregateFunctions/AggregateFunctionMerge.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionMerge(AggregateFunctionPtr & nested)
{
	return new AggregateFunctionMerge(nested);
}

}
