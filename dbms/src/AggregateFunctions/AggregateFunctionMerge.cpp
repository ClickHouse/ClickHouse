#include <DB/AggregateFunctions/AggregateFunctionMerge.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionMerge(AggregateFunctionPtr & nested)
{
	return std::make_shared<AggregateFunctionMerge>(nested);
}

}
